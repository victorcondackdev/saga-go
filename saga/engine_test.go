package saga_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/victorcondackdev/saga-go/saga"
	"github.com/victorcondackdev/saga-go/sagatest"
	"github.com/victorcondackdev/saga-go/store"
)

type order struct {
	ID    string   `json:"id"`
	Trail []string `json:"trail,omitempty"`
	A     string   `json:"a,omitempty"`
	B     string   `json:"b,omitempty"`
	Big   bool     `json:"big,omitempty"`
}

func step(name string) saga.StepFunc[order] {
	return func(sc saga.Context, o *order) error { o.Trail = append(o.Trail, name); return nil }
}

func undo(name string) saga.StepFunc[order] {
	return func(sc saga.Context, o *order) error { o.Trail = append(o.Trail, "~"+name); return nil }
}

func failing(n *int, failures int, err error) saga.StepFunc[order] {
	return func(sc saga.Context, o *order) error {
		*n++
		if *n <= failures {
			return err
		}
		o.Trail = append(o.Trail, "ok")
		return nil
	}
}

type env struct {
	t     *testing.T
	st    store.Store
	clock *sagatest.Clock
	e     *saga.Engine
	ctx   context.Context
}

func newEnv(t *testing.T, opts ...saga.Option) *env {
	t.Helper()
	clock := sagatest.NewClock(testStart)
	st := store.NewMemory()
	base := []saga.Option{saga.WithClock(clock), saga.WithLogger(saga.DiscardLogger()), saga.WithOwner("e1"), saga.WithRandSeed(1)}
	return &env{t: t, st: st, clock: clock, e: saga.New(st, append(base, opts...)...), ctx: context.Background()}
}

func (v *env) start(d *saga.Definition[order], in order) string {
	v.t.Helper()
	if err := v.e.Register(d); err != nil && !strings.Contains(err.Error(), "already registered") {
		v.t.Fatal(err)
	}
	id, err := saga.Start(v.ctx, v.e, d, in.ID, in)
	if err != nil {
		v.t.Fatal(err)
	}
	return id
}

// drive ticks the engine, advancing the fake clock to the next due time,
// until the instance is at rest.
func (v *env) drive(id string) *store.Instance {
	v.t.Helper()
	for i := 0; i < 1000; i++ {
		n, err := v.e.Tick(v.ctx)
		if err != nil {
			v.t.Fatal(err)
		}
		in, _ := v.st.Get(v.ctx, id)
		if !in.Status.Runnable() {
			return in
		}
		if n == 0 {
			v.clock.Set(in.NextRunAt)
			v.clock.Advance(time.Millisecond)
		}
	}
	v.t.Fatal("saga did not come to rest")
	return nil
}

func trail(t *testing.T, in *store.Instance) string {
	t.Helper()
	o, err := saga.StateOf[order](in)
	if err != nil {
		t.Fatal(err)
	}
	return strings.Join(o.Trail, ",")
}

func TestHappyPath(t *testing.T) {
	var hooks []string
	var invs []saga.Invocation
	v := newEnv(t, saga.WithHook(func(h saga.HookEvent) { hooks = append(hooks, h.Event.Kind) }),
		saga.WithInvocationObserver(func(i saga.Invocation) { invs = append(invs, i) }))
	d := saga.Define[order]("happy").
		Step("a", step("a"), saga.Compensate(undo("a"))).
		Step("b", step("b")).
		MustBuild()
	id := v.start(d, order{ID: "o1"})
	in := v.drive(id)
	if in.Status != saga.StatusCompleted || trail(t, in) != "a,b" || in.Owner != "" {
		t.Fatalf("got %s %s owner=%q", in.Status, trail(t, in), in.Owner)
	}
	want := "saga_created,saga_started,step_started,step_succeeded,step_started,step_succeeded,saga_completed"
	if strings.Join(hooks, ",") != want {
		t.Fatalf("hooks:\n got %s\nwant %s", strings.Join(hooks, ","), want)
	}
	if len(invs) != 2 || invs[0].Key != id+":a" || invs[1].Attempt != 1 || invs[0].Compensation {
		t.Fatalf("invocations: %+v", invs)
	}
	for i, ev := range in.History {
		if ev.Seq != i+1 {
			t.Fatalf("history seq %d at %d", ev.Seq, i)
		}
	}
}

func TestStepContext(t *testing.T) {
	v := newEnv(t)
	var seen []string
	d := saga.Define[order]("ctx").
		Step("a", func(sc saga.Context, o *order) error {
			seen = append(seen, sc.SagaID(), sc.SagaType(), sc.Step(), fmt.Sprint(sc.Attempt()), fmt.Sprint(sc.Compensating()), sc.IdempotencyKey())
			if sc.Logger() == nil {
				t.Error("nil logger")
			}
			return nil
		}, saga.Compensate(func(sc saga.Context, o *order) error {
			seen = append(seen, sc.IdempotencyKey(), fmt.Sprint(sc.Compensating()))
			return nil
		})).
		Step("b", func(sc saga.Context, o *order) error { return saga.Permanent(errors.New("no")) }).
		MustBuild()
	id := v.start(d, order{ID: "c"})
	v.drive(id)
	want := []string{id, "ctx", "a", "1", "false", id + ":a", id + ":a:compensate", "true"}
	if fmt.Sprint(seen) != fmt.Sprint(want) {
		t.Fatalf("got %v want %v", seen, want)
	}
}

func TestRetryThenSuccess(t *testing.T) {
	v := newEnv(t)
	n := 0
	d := saga.Define[order]("retry").
		Step("flaky", failing(&n, 2, errors.New("503")), saga.Retry(saga.RetryPolicy{MaxAttempts: 3, Initial: time.Second, Jitter: 0.000001})).
		MustBuild()
	id := v.start(d, order{ID: "r"})
	in := v.drive(id)
	if in.Status != saga.StatusCompleted || n != 3 || in.Steps[0].Attempts != 3 {
		t.Fatalf("status %s n=%d attempts=%d", in.Status, n, in.Steps[0].Attempts)
	}
	// Backoff 1s then 2s from the fake clock start.
	if el := in.UpdatedAt.Sub(testStart); el < 3*time.Second || el > 3*time.Second+10*time.Millisecond {
		t.Fatalf("elapsed %s, want ~3s of backoff", el)
	}
}

func TestPermanentFailureCompensatesInReverse(t *testing.T) {
	v := newEnv(t)
	n := 0
	d := saga.Define[order]("comp").
		Step("a", step("a"), saga.Compensate(undo("a"))).
		Step("b", step("b")). // no compensation: nothing to undo
		Step("c", step("c"), saga.Compensate(undo("c"))).
		Step("d", failing(&n, 99, saga.Permanent(errors.New("card declined"))), saga.Compensate(undo("d"))).
		Step("e", step("e"), saga.Compensate(undo("e"))).
		MustBuild()
	in := v.drive(v.start(d, order{ID: "x"}))
	if in.Status != saga.StatusCompensated || trail(t, in) != "a,b,c,~c,~a" || n != 1 {
		t.Fatalf("got %s %q n=%d", in.Status, trail(t, in), n)
	}
	if !strings.Contains(in.Failure, "card declined") || in.Steps[4].Status != store.StepPending {
		t.Fatalf("failure %q, e=%s", in.Failure, in.Steps[4].Status)
	}
}

func TestCompensationFailureNeedsAttentionThenRetry(t *testing.T) {
	v := newEnv(t)
	broken := true
	d := saga.Define[order]("compfail").
		Step("a", step("a"), saga.Compensate(func(sc saga.Context, o *order) error {
			if broken {
				return errors.New("ledger down")
			}
			o.Trail = append(o.Trail, "~a")
			return nil
		}), saga.CompensateRetry(saga.RetryPolicy{MaxAttempts: 3, Initial: time.Millisecond})).
		Step("b", func(sc saga.Context, o *order) error { return saga.Permanent(errors.New("boom")) }).
		MustBuild()
	id := v.start(d, order{ID: "x"})
	in := v.drive(id)
	if in.Status != saga.StatusNeedsAttention || in.Steps[0].Comp != store.CompFailed || in.Steps[0].CompAttempts != 3 {
		t.Fatalf("got %s comp=%s attempts=%d", in.Status, in.Steps[0].Comp, in.Steps[0].CompAttempts)
	}
	stuck, _ := v.e.Stuck(v.ctx, time.Hour)
	if len(stuck) != 1 || stuck[0].ID != id {
		t.Fatalf("stuck: %d", len(stuck))
	}
	if err := v.e.Resolve(v.ctx, id, saga.StatusRunning, ""); !errors.Is(err, saga.ErrInvalidTransition) {
		t.Fatalf("resolve to RUNNING: %v", err)
	}
	broken = false
	if err := v.e.Retry(v.ctx, id, "ledger is back"); err != nil {
		t.Fatal(err)
	}
	in = v.drive(id)
	if in.Status != saga.StatusCompensated || trail(t, in) != "a,~a" {
		t.Fatalf("after manual retry: %s %s", in.Status, trail(t, in))
	}
	if err := v.e.Retry(v.ctx, id, ""); !errors.Is(err, saga.ErrInvalidTransition) {
		t.Fatalf("retry of compensated saga: %v", err)
	}
	if err := v.e.Resolve(v.ctx, id, saga.StatusCompleted, ""); !errors.Is(err, saga.ErrInvalidTransition) {
		t.Fatalf("resolve of compensated saga: %v", err)
	}
}

func TestPivotForwardRecovery(t *testing.T) {
	v := newEnv(t)
	n := 0
	d := saga.Define[order]("pivot").
		Step("reserve", step("reserve"), saga.Compensate(undo("reserve"))).
		Step("capture", step("capture"), saga.Pivot()).
		Step("notify", failing(&n, 4, errors.New("smtp down")), saga.Retry(saga.RetryPolicy{MaxAttempts: 2, Initial: time.Millisecond})).
		MustBuild()
	id := v.start(d, order{ID: "p"})
	in := v.drive(id)
	if in.Status != saga.StatusNeedsAttention || !in.PivotPassed || trail(t, in) != "reserve,capture" {
		t.Fatalf("got %s pivot=%v %s", in.Status, in.PivotPassed, trail(t, in))
	}
	if !strings.Contains(in.Failure, "past the pivot") {
		t.Fatalf("failure %q", in.Failure)
	}
	// Operator retries: two more failures, then attention again; then it works.
	if err := v.e.Retry(v.ctx, id, "smtp restarted"); err != nil {
		t.Fatal(err)
	}
	in = v.drive(id)
	if in.Status != saga.StatusNeedsAttention {
		t.Fatalf("second round: %s", in.Status)
	}
	v.e.Retry(v.ctx, id, "")
	in = v.drive(id)
	if in.Status != saga.StatusCompleted || trail(t, in) != "reserve,capture,ok" {
		t.Fatalf("got %s %s", in.Status, trail(t, in))
	}
}

func TestPivotFailureCompensates(t *testing.T) {
	v := newEnv(t)
	d := saga.Define[order]("pivotfail").
		Step("reserve", step("reserve"), saga.Compensate(undo("reserve"))).
		Step("capture", func(sc saga.Context, o *order) error { return saga.Permanent(errors.New("rejected")) }, saga.Pivot()).
		MustBuild()
	in := v.drive(v.start(d, order{ID: "p"}))
	if in.Status != saga.StatusCompensated || trail(t, in) != "reserve,~reserve" || in.PivotPassed {
		t.Fatalf("got %s %s", in.Status, trail(t, in))
	}
}

func TestResolve(t *testing.T) {
	v := newEnv(t)
	d := saga.Define[order]("resolve").
		Step("capture", step("capture"), saga.Pivot()).
		Step("notify", func(sc saga.Context, o *order) error { return saga.Permanent(errors.New("bad address")) }).
		MustBuild()
	id := v.start(d, order{ID: "p"})
	if in := v.drive(id); in.Status != saga.StatusNeedsAttention {
		t.Fatal(in.Status)
	}
	if err := v.e.Resolve(v.ctx, id, saga.StatusCompleted, "customer notified by phone"); err != nil {
		t.Fatal(err)
	}
	in, _ := v.e.Get(v.ctx, id)
	last := in.History[len(in.History)-1]
	if in.Status != saga.StatusCompleted || last.Kind != store.EvManualResolution || !strings.Contains(last.Detail, "phone") {
		t.Fatalf("got %s %+v", in.Status, last)
	}
	if err := v.e.Resolve(v.ctx, "nope", saga.StatusCompleted, ""); !errors.Is(err, store.ErrNotFound) {
		t.Fatal(err)
	}
	if err := v.e.Retry(v.ctx, "nope", ""); !errors.Is(err, store.ErrNotFound) {
		t.Fatal(err)
	}
}

func TestSagaTimeout(t *testing.T) {
	t.Run("before the pivot it compensates", func(t *testing.T) {
		v := newEnv(t)
		d := saga.Define[order]("timeout").
			Step("a", step("a"), saga.Compensate(undo("a"))).
			Step("b", func(sc saga.Context, o *order) error { return errors.New("slow") },
				saga.Retry(saga.RetryPolicy{MaxAttempts: 1000, Initial: time.Second, Max: time.Second})).
			Timeout(10 * time.Second).
			MustBuild()
		in := v.drive(v.start(d, order{ID: "t"}))
		if in.Status != saga.StatusCompensated || in.Failure != "saga deadline exceeded" || trail(t, in) != "a,~a" {
			t.Fatalf("got %s %q %s", in.Status, in.Failure, trail(t, in))
		}
		if a := in.Steps[1].Attempts; a < 8 || a > 12 {
			t.Fatalf("attempts within 10s of 1s backoff: %d", a)
		}
	})
	t.Run("after the pivot it keeps going", func(t *testing.T) {
		v := newEnv(t)
		n := 0
		d := saga.Define[order]("timeout-pivot").
			Step("a", step("a"), saga.Pivot()).
			Step("b", failing(&n, 20, errors.New("slow")), saga.Retry(saga.RetryPolicy{MaxAttempts: 100, Initial: time.Second, Max: time.Second})).
			Timeout(5 * time.Second).
			MustBuild()
		in := v.drive(v.start(d, order{ID: "t"}))
		if in.Status != saga.StatusCompleted || n != 21 {
			t.Fatalf("got %s n=%d", in.Status, n)
		}
	})
}

func TestAttemptTimeout(t *testing.T) {
	v := newEnv(t)
	calls := 0
	d := saga.Define[order]("attempt-timeout").
		Step("slow", func(sc saga.Context, o *order) error {
			calls++
			if calls == 1 {
				<-sc.Done()
				return sc.Err()
			}
			return nil
		}, saga.Timeout(20*time.Millisecond), saga.Retry(saga.RetryPolicy{MaxAttempts: 2, Initial: time.Millisecond})).
		MustBuild()
	in := v.drive(v.start(d, order{ID: "t"}))
	if in.Status != saga.StatusCompleted || calls != 2 {
		t.Fatalf("got %s calls=%d", in.Status, calls)
	}
	var retry store.Event
	for _, ev := range in.History {
		if ev.Kind == store.EvStepRetry {
			retry = ev
		}
	}
	if !strings.Contains(retry.Error, "timed out after 20ms") {
		t.Fatalf("retry event: %+v", retry)
	}
}

func TestAwaitSignal(t *testing.T) {
	v := newEnv(t, saga.WithWaitRecheck(10*time.Second))
	type settlement struct{ Status string }
	d := saga.Define[order]("await").
		Step("send", step("send"), saga.Compensate(undo("send"))).
		Step("settle", func(sc saga.Context, o *order) error {
			var s settlement
			if err := sc.Await("settled", &s); err != nil {
				return err
			}
			o.Trail = append(o.Trail, s.Status)
			return nil
		}, saga.Deadline(time.Minute)).
		MustBuild()

	id := v.start(d, order{ID: "w"})
	v.e.Tick(v.ctx)
	in, _ := v.e.Get(v.ctx, id)
	if in.Steps[1].Status != store.StepWaiting || in.Steps[1].Waiting != "settled" || in.Steps[1].Attempts != 0 {
		t.Fatalf("not waiting: %+v", in.Steps[1])
	}
	if !in.NextRunAt.Equal(testStart.Add(10 * time.Second)) {
		t.Fatalf("recheck at %s", in.NextRunAt)
	}
	if err := v.e.Signal(v.ctx, id, "settled", settlement{Status: "SETTLED"}); err != nil {
		t.Fatal(err)
	}
	if n, _ := v.e.Tick(v.ctx); n != 1 { // due immediately, no clock change
		t.Fatalf("signal did not wake the saga")
	}
	in, _ = v.e.Get(v.ctx, id)
	if in.Status != saga.StatusCompleted || trail(t, in) != "send,SETTLED" {
		t.Fatalf("got %s %s", in.Status, trail(t, in))
	}
	if err := v.e.Signal(v.ctx, "missing", "x", json.RawMessage(`1`)); err == nil {
		t.Fatal("signal to missing saga")
	}
	if err := v.e.Signal(v.ctx, id, "bad", func() {}); err == nil {
		t.Fatal("unmarshalable payload accepted")
	}
	if err := v.e.Signal(v.ctx, id, "raw", []byte(`{}`)); err != nil {
		t.Fatal(err)
	}

	// Never signalled: the step deadline fails it and compensation runs.
	id2 := v.start(d, order{ID: "w2"})
	in = v.drive(id2)
	if in.Status != saga.StatusCompensated || !strings.Contains(in.Failure, "deadline") || trail(t, in) != "send,~send" {
		t.Fatalf("got %s %q %s", in.Status, in.Failure, trail(t, in))
	}
}

func TestBadSignalPayloadIsPermanent(t *testing.T) {
	v := newEnv(t)
	d := saga.Define[order]("badsig").
		Step("a", func(sc saga.Context, o *order) error {
			var n int
			return sc.Await("n", &n)
		}).MustBuild()
	id := v.start(d, order{ID: "b"})
	v.e.Signal(v.ctx, id, "n", "not a number")
	if in := v.drive(id); in.Status != saga.StatusCompensated || !strings.Contains(in.Failure, "decode signal") {
		t.Fatalf("got %s %q", in.Status, in.Failure)
	}
	d2 := saga.Define[order]("nilsig").Step("a", func(sc saga.Context, o *order) error { return sc.Await("n", nil) }).MustBuild()
	id = v.start(d2, order{ID: "n"})
	v.e.Signal(v.ctx, id, "n", 1)
	if in := v.drive(id); in.Status != saga.StatusCompleted {
		t.Fatal(in.Status)
	}
}

func TestConditionalStep(t *testing.T) {
	v := newEnv(t)
	d := saga.Define[order]("cond").
		Step("a", step("a")).
		Step("review", step("review"), saga.When(func(o *order) bool { return o.Big })).
		Step("b", step("b")).
		MustBuild()
	in := v.drive(v.start(d, order{ID: "small"}))
	if trail(t, in) != "a,b" || in.Steps[1].Status != store.StepSkipped {
		t.Fatalf("small: %s %s", trail(t, in), in.Steps[1].Status)
	}
	in = v.drive(v.start(d, order{ID: "big", Big: true}))
	if trail(t, in) != "a,review,b" {
		t.Fatalf("big: %s", trail(t, in))
	}
}

func TestParallelGroup(t *testing.T) {
	for _, serial := range []bool{true, false} {
		t.Run(fmt.Sprintf("serial=%v", serial), func(t *testing.T) {
			var opts []saga.Option
			if serial {
				opts = append(opts, saga.WithSerialGroups())
			}
			v := newEnv(t, opts...)
			var mu sync.Mutex
			started := 0
			bothIn := make(chan struct{})
			branch := func(field string, n *int) saga.StepFunc[order] {
				return func(sc saga.Context, o *order) error {
					mu.Lock()
					started++
					if started == 2 {
						close(bothIn)
					}
					mu.Unlock()
					if !serial {
						<-bothIn // proves the branches run concurrently
					}
					*n++
					if *n == 1 {
						return errors.New("transient")
					}
					if field == "a" {
						o.A = "A"
					} else {
						o.B = "B"
					}
					return nil
				}
			}
			var na, nb int
			d := saga.Define[order]("par").
				Step("first", step("first")).
				Parallel(
					saga.Par("left", branch("a", &na), saga.Retry(saga.RetryPolicy{Initial: time.Millisecond})),
					saga.Par("right", branch("b", &nb), saga.Retry(saga.RetryPolicy{Initial: 5 * time.Millisecond})),
					saga.Par("skipped", step("never"), saga.When(func(o *order) bool { return false })),
				).
				Step("last", step("last")).
				MustBuild()
			in := v.drive(v.start(d, order{ID: "p"}))
			o, _ := saga.StateOf[order](in)
			if in.Status != saga.StatusCompleted || o.A != "A" || o.B != "B" || strings.Join(o.Trail, ",") != "first,last" {
				t.Fatalf("got %s %+v", in.Status, o)
			}
			for _, s := range in.Steps {
				if s.Output != nil {
					t.Fatalf("branch output kept after merge: %s", s.Name)
				}
			}
		})
	}
}

func TestParallelConflictCompensates(t *testing.T) {
	v := newEnv(t, saga.WithSerialGroups())
	d := saga.Define[order]("conflict").
		Parallel(
			saga.Par("x", func(sc saga.Context, o *order) error { o.A = "x"; return nil }, saga.Compensate(undo("x"))),
			saga.Par("y", func(sc saga.Context, o *order) error { o.A = "y"; return nil }),
		).MustBuild()
	in := v.drive(v.start(d, order{ID: "c"}))
	if in.Status != saga.StatusCompensated || !strings.Contains(in.Failure, `both changed field "a"`) {
		t.Fatalf("got %s %q", in.Status, in.Failure)
	}
	if in.Steps[0].Comp != store.CompDone {
		t.Fatal("branch x not compensated")
	}
}

func TestParallelBranchFailure(t *testing.T) {
	v := newEnv(t)
	d := saga.Define[order]("parfail").
		Step("a", step("a"), saga.Compensate(undo("a"))).
		Parallel(
			saga.Par("ok", func(sc saga.Context, o *order) error { o.B = "done"; return nil }, saga.Compensate(undo("ok"))),
			saga.Par("bad", func(sc saga.Context, o *order) error { return saga.Permanent(errors.New("no")) }, saga.Compensate(undo("bad"))),
		).MustBuild()
	in := v.drive(v.start(d, order{ID: "f"}))
	if in.Status != saga.StatusCompensated || in.Steps[1].Comp != store.CompDone || in.Steps[2].Comp != store.CompNone {
		t.Fatalf("got %s %+v", in.Status, in.Steps)
	}
	if !strings.HasSuffix(trail(t, in), "~ok,~a") && !strings.HasSuffix(trail(t, in), "~a") {
		t.Fatalf("trail %s", trail(t, in))
	}
}

func TestCompensateAttempted(t *testing.T) {
	v := newEnv(t)
	d := saga.Define[order]("attempted").
		Step("send", func(sc saga.Context, o *order) error { return errors.New("timeout, maybe sent") },
			saga.Compensate(undo("send")), saga.CompensateAttempted(), saga.Retry(saga.RetryPolicy{MaxAttempts: 2, Initial: time.Millisecond})).
		Step("other", step("other"), saga.Compensate(undo("other"))).
		MustBuild()
	in := v.drive(v.start(d, order{ID: "a"}))
	if in.Status != saga.StatusCompensated || trail(t, in) != "~send" {
		t.Fatalf("got %s %s", in.Status, trail(t, in))
	}
}

func TestPanicIsRetried(t *testing.T) {
	v := newEnv(t)
	calls := 0
	d := saga.Define[order]("panic").Step("p", func(sc saga.Context, o *order) error {
		calls++
		if calls == 1 {
			panic("nil map")
		}
		return nil
	}, saga.Retry(saga.RetryPolicy{Initial: time.Millisecond})).MustBuild()
	in := v.drive(v.start(d, order{ID: "p"}))
	if in.Status != saga.StatusCompleted || calls != 2 {
		t.Fatalf("got %s %d", in.Status, calls)
	}
}

func TestStartIdempotency(t *testing.T) {
	v := newEnv(t)
	d := saga.Define[order]("idem").Step("a", step("a")).MustBuild()
	v.e.Register(d)
	a, created, err := v.e.Start(v.ctx, "idem", "k1", json.RawMessage(`{"id":"1"}`))
	if err != nil || !created {
		t.Fatal(err)
	}
	b, created, err := v.e.Start(v.ctx, "idem", "k1", json.RawMessage(`{"id":"1"}`))
	if err != nil || created || b.ID != a.ID {
		t.Fatalf("replay: %v %v", created, err)
	}
	if _, _, err := v.e.Start(v.ctx, "idem", "k1", json.RawMessage(`{"id":"2"}`)); !errors.Is(err, saga.ErrKeyConflict) {
		t.Fatalf("different input: %v", err)
	}
	if _, _, err := v.e.Start(v.ctx, "nope", "k", nil); !errors.Is(err, saga.ErrUnknownType) {
		t.Fatalf("unknown type: %v", err)
	}
	if _, _, err := v.e.Start(v.ctx, "idem", "k2", json.RawMessage(`[1]`)); err == nil {
		t.Fatal("input not matching the state type accepted")
	}
	c, _, _ := v.e.Start(v.ctx, "idem", "", nil)
	d2, _, _ := v.e.Start(v.ctx, "idem", "", nil)
	if c.ID == d2.ID || c.Key != c.ID {
		t.Fatal("empty key must always create")
	}
	if err := v.e.Register(d); err == nil {
		t.Fatal("double registration accepted")
	}
	if got := v.e.Types(); len(got) != 1 || got[0] != "idem" {
		t.Fatalf("types %v", got)
	}
	if v.e.Owner() != "e1" || v.e.Store() != v.st {
		t.Fatal("accessors")
	}
	type unmarshalable struct{ F func() }
	bad := saga.Define[unmarshalable]("bad").Step("a", func(sc saga.Context, u *unmarshalable) error { return nil }).MustBuild()
	v.e.Register(bad)
	if _, err := saga.Start(v.ctx, v.e, bad, "k", unmarshalable{F: func() {}}); err == nil {
		t.Fatal("unmarshalable input accepted")
	}
}

func TestDefinitionChangedNeedsAttention(t *testing.T) {
	clock := sagatest.NewClock(testStart)
	st := store.NewMemory()
	ctx := context.Background()
	old := saga.Define[order]("evolve").Step("a", step("a")).MustBuild()
	e1 := saga.New(st, saga.WithClock(clock), saga.WithLogger(saga.DiscardLogger()))
	e1.Register(old)
	id, _ := saga.Start(ctx, e1, old, "k", order{})

	changed := saga.Define[order]("evolve").Step("a", step("a")).Step("b", step("b")).MustBuild()
	e2 := saga.New(st, saga.WithClock(clock), saga.WithLogger(saga.DiscardLogger()))
	e2.Register(changed)
	e2.Tick(ctx)
	in, _ := e2.Get(ctx, id)
	if in.Status != saga.StatusNeedsAttention || !strings.Contains(in.Failure, "changed incompatibly") {
		t.Fatalf("got %s %q", in.Status, in.Failure)
	}
	if err := e2.Retry(ctx, id, ""); !errors.Is(err, saga.ErrInvalidTransition) {
		t.Fatalf("retry with changed definition: %v", err)
	}
}

func TestUnknownTypeIsLeftForOthers(t *testing.T) {
	clock := sagatest.NewClock(testStart)
	st := store.NewMemory()
	ctx := context.Background()
	d := saga.Define[order]("known-elsewhere").Step("a", step("a")).MustBuild()
	producer := saga.New(st, saga.WithClock(clock), saga.WithLogger(saga.DiscardLogger()))
	producer.Register(d)
	id, _ := saga.Start(ctx, producer, d, "k", order{})
	other := saga.New(st, saga.WithClock(clock), saga.WithLogger(saga.DiscardLogger()), saga.WithOwner("other"))
	other.Tick(ctx)
	in, _ := st.Get(ctx, id)
	if in.Owner != "" || in.Status != saga.StatusPending {
		t.Fatalf("instance of unknown type not released: %+v", in)
	}
	producer.Tick(ctx)
	if in, _ = st.Get(ctx, id); in.Status != saga.StatusCompleted {
		t.Fatal(in.Status)
	}
}

func TestPoisonStepNeedsAttention(t *testing.T) {
	clock := sagatest.NewClock(testStart)
	st := store.NewMemory()
	ctx := context.Background()
	d := saga.Define[order]("poison").Step("a", step("a"), saga.Compensate(undo("a"))).Step("kill", step("kill")).MustBuild()
	var id string
	for i := 0; i < 10; i++ {
		e := saga.New(st, saga.WithClock(clock), saga.WithLogger(saga.DiscardLogger()), saga.WithOwner(fmt.Sprint("w", i)),
			saga.WithMaxAbandoned(3), saga.WithLease(time.Second),
			saga.WithFaultInjector(func(p saga.CrashPoint, s string) bool { return p == saga.CrashBeforeStepInvoke && s == "kill" }))
		e.Register(d)
		if id == "" {
			id, _ = saga.Start(ctx, e, d, "k", order{})
		}
		if _, err := e.Tick(ctx); err != nil && !errors.Is(err, saga.ErrCrashInjected) {
			t.Fatal(err)
		}
		clock.Advance(2 * time.Second)
	}
	in, _ := st.Get(ctx, id)
	if in.Status != saga.StatusNeedsAttention || in.Steps[1].Abandoned != 4 || !strings.Contains(in.Failure, "abandoned") {
		t.Fatalf("got %s abandoned=%d %q", in.Status, in.Steps[1].Abandoned, in.Failure)
	}
	var b bytes.Buffer
	saga.RenderTimeline(&b, in, 20)
	if strings.Count(b.String(), "abandoned") < 4 || !strings.Contains(b.String(), "lease_takeover") {
		t.Fatalf("timeline:\n%s", b.String())
	}
}

func TestPoisonCompensationNeedsAttention(t *testing.T) {
	clock := sagatest.NewClock(testStart)
	st := store.NewMemory()
	ctx := context.Background()
	d := saga.Define[order]("poison-comp").
		Step("a", step("a"), saga.Compensate(undo("a"))).
		Step("b", func(sc saga.Context, o *order) error { return saga.Permanent(errors.New("x")) }).MustBuild()
	var id string
	for i := 0; i < 10; i++ {
		e := saga.New(st, saga.WithClock(clock), saga.WithLogger(saga.DiscardLogger()), saga.WithOwner(fmt.Sprint("w", i)),
			saga.WithMaxAbandoned(2), saga.WithLease(time.Second),
			saga.WithFaultInjector(func(p saga.CrashPoint, s string) bool { return p == saga.CrashBeforeCompInvoke }))
		e.Register(d)
		if id == "" {
			id, _ = saga.Start(ctx, e, d, "k", order{})
		}
		e.Tick(ctx)
		clock.Advance(2 * time.Second)
	}
	in, _ := st.Get(ctx, id)
	if in.Status != saga.StatusNeedsAttention || in.Steps[0].Comp != store.CompFailed {
		t.Fatalf("got %s %s", in.Status, in.Steps[0].Comp)
	}
}

func TestMetrics(t *testing.T) {
	v := newEnv(t)
	n := 0
	d := saga.Define[order]("metrics").
		Step("a", failing(&n, 1, errors.New("x")), saga.Retry(saga.RetryPolicy{Initial: time.Millisecond}), saga.Compensate(undo("a"))).
		Step("b", func(sc saga.Context, o *order) error { return saga.Permanent(errors.New("no")) }).
		MustBuild()
	v.drive(v.start(d, order{ID: "m"}))
	var b bytes.Buffer
	if err := v.e.WritePrometheus(v.ctx, &b); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`saga_instances{status="COMPENSATED"} 1`,
		`saga_started_total{type="metrics"} 1`,
		`saga_finished_total{type="metrics",status="COMPENSATED"} 1`,
		`saga_step_attempts_total{type="metrics",step="a",result="retry"} 1`,
		`saga_step_attempts_total{type="metrics",step="a",result="ok"} 1`,
		`saga_step_attempts_total{type="metrics",step="b",result="failed"} 1`,
		`saga_step_retries_total{type="metrics",step="a"} 1`,
		`saga_compensations_total{type="metrics",step="a",result="ok"} 1`,
		`saga_step_duration_seconds_count{type="metrics",step="a",phase="forward"} 2`,
		`# TYPE saga_step_duration_seconds histogram`,
	} {
		if !strings.Contains(b.String(), want) {
			t.Errorf("metrics missing %q\n%s", want, b.String())
		}
	}
	v.st.Close()
	if err := v.e.WritePrometheus(v.ctx, &b); err == nil {
		t.Fatal("closed store")
	}
}

func TestListAndWait(t *testing.T) {
	v := newEnv(t)
	d := saga.Define[order]("list").Step("a", step("a")).MustBuild()
	id := v.start(d, order{ID: "1"})
	v.start(d, order{ID: "2"})
	all, _ := v.e.List(v.ctx, store.Filter{Type: "list"})
	if len(all) != 2 {
		t.Fatal(len(all))
	}
	v.e.Tick(v.ctx)
	in, err := v.e.Wait(v.ctx, id)
	if err != nil || in.Status != saga.StatusCompleted {
		t.Fatal(err, in.Status)
	}
	d2 := saga.Define[order]("list-wait").Step("a", func(sc saga.Context, o *order) error { return sc.Await("never", nil) }).MustBuild()
	id = v.start(d2, order{ID: "3"})
	ctx, cancel := context.WithTimeout(v.ctx, 20*time.Millisecond)
	defer cancel()
	if _, err := v.e.Wait(ctx, id); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if _, err := v.e.Wait(v.ctx, "missing"); err == nil {
		t.Fatal("wait on missing")
	}
	v.clock.Advance(2 * time.Hour)
	stuck, _ := v.e.Stuck(v.ctx, time.Hour)
	if len(stuck) != 1 {
		t.Fatalf("stuck %d", len(stuck))
	}
}
