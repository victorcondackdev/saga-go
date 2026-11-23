package saga_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/victorcondackdev/saga-go/saga"
	"github.com/victorcondackdev/saga-go/sagatest"
	"github.com/victorcondackdev/saga-go/store"
)

// effects records side effects deduplicated by idempotency key, as a
// downstream system would, and counts raw executions.
type effects struct {
	mu       sync.Mutex
	applied  map[string]int
	running  map[string]bool
	overlaps int
	execs    atomic.Int64
}

func newEffects() *effects { return &effects{applied: map[string]int{}, running: map[string]bool{}} }

func (f *effects) do(sc saga.Context, d time.Duration) error {
	f.execs.Add(1)
	f.mu.Lock()
	if f.running[sc.SagaID()] {
		f.overlaps++ // two engines driving the same saga at once
	}
	f.running[sc.SagaID()] = true
	f.mu.Unlock()
	if d > 0 {
		time.Sleep(d)
	}
	f.mu.Lock()
	delete(f.running, sc.SagaID())
	f.applied[sc.IdempotencyKey()]++
	f.mu.Unlock()
	return nil
}

func fourSteps(name string, fx *effects, d time.Duration) *saga.Definition[order] {
	s := func(sc saga.Context, o *order) error { return fx.do(sc, d) }
	return saga.Define[order](name).
		Step("reserve", s, saga.Compensate(s)).
		Step("send", s, saga.Compensate(s)).
		Step("confirm", s, saga.Pivot()).
		Step("notify", s).
		MustBuild()
}

// TestManyEnginesShareOneStore runs several engines concurrently on one
// store (memory and WAL) and checks that every saga completes, that no saga
// is ever driven by two engines at once, and that every step took effect.
func TestManyEnginesShareOneStore(t *testing.T) {
	for _, kind := range []string{"memory", "wal"} {
		t.Run(kind, func(t *testing.T) {
			var st store.Store = store.NewMemory()
			if kind == "wal" {
				w, err := store.OpenWAL(store.WALOptions{Dir: t.TempDir(), Sync: store.SyncNever})
				if err != nil {
					t.Fatal(err)
				}
				st = w
			}
			defer st.Close()
			fx := newEffects()
			def := fourSteps("shared", fx, 0)
			ctx, cancel := context.WithCancel(context.Background())
			var wg sync.WaitGroup
			engines := make([]*saga.Engine, 4)
			for i := range engines {
				engines[i] = saga.New(st, saga.WithOwner(fmt.Sprint("engine-", i)), saga.WithLogger(saga.DiscardLogger()),
					saga.WithConcurrency(4), saga.WithPollInterval(time.Millisecond), saga.WithLease(2*time.Second))
				engines[i].Register(def)
				wg.Add(1)
				go func(e *saga.Engine) { defer wg.Done(); e.Run(ctx) }(engines[i])
			}
			const n = 200
			ids := make([]string, n)
			for i := range ids {
				id, err := saga.Start(ctx, engines[i%len(engines)], def, fmt.Sprint("k", i), order{ID: fmt.Sprint(i)})
				if err != nil {
					t.Fatal(err)
				}
				ids[i] = id
			}
			wctx, wcancel := context.WithTimeout(ctx, 20*time.Second)
			for _, id := range ids {
				in, err := engines[0].Wait(wctx, id)
				if err != nil || in.Status != saga.StatusCompleted {
					t.Fatalf("%s: %v %v", id, err, in.Status)
				}
			}
			wcancel()
			cancel()
			wg.Wait()
			if fx.overlaps != 0 {
				t.Fatalf("%d overlapping executions", fx.overlaps)
			}
			if len(fx.applied) != n*4 {
				t.Fatalf("applied %d effects, want %d", len(fx.applied), n*4)
			}
		})
	}
}

// TestStaleLeaseTakeover: an engine dies holding a lease; another takes the
// saga over once the lease expires and re-runs the in-doubt step.
func TestStaleLeaseTakeover(t *testing.T) {
	clock := sagatest.NewClock(testStart)
	st := store.NewMemory()
	ctx := context.Background()
	fx := newEffects()
	def := fourSteps("takeover", fx, 0)
	dead := saga.New(st, saga.WithClock(clock), saga.WithOwner("dead"), saga.WithLogger(saga.DiscardLogger()), saga.WithLease(10*time.Second),
		saga.WithFaultInjector(func(p saga.CrashPoint, s string) bool { return p == saga.CrashAfterStepInvoke && s == "send" }))
	dead.Register(def)
	id, _ := saga.Start(ctx, dead, def, "k", order{})
	if _, err := dead.Tick(ctx); !errors.Is(err, saga.ErrCrashInjected) {
		t.Fatal(err)
	}
	heir := saga.New(st, saga.WithClock(clock), saga.WithOwner("heir"), saga.WithLogger(saga.DiscardLogger()))
	heir.Register(def)
	if n, _ := heir.Tick(ctx); n != 0 {
		t.Fatal("took over a live lease")
	}
	clock.Advance(11 * time.Second)
	if n, _ := heir.Tick(ctx); n != 1 {
		t.Fatal("did not take over an expired lease")
	}
	in, _ := st.Get(ctx, id)
	if in.Status != saga.StatusCompleted {
		t.Fatal(in.Status)
	}
	if fx.applied[id+":send"] != 2 || fx.execs.Load() != 5 {
		t.Fatalf("send should have run twice (at-least-once), applied=%v", fx.applied)
	}
	var kinds []string
	for _, ev := range in.History {
		if ev.Kind == store.EvLeaseTakeover || ev.Kind == store.EvStepAbandoned {
			kinds = append(kinds, ev.Kind+":"+ev.Owner+ev.Step)
		}
	}
	if fmt.Sprint(kinds) != "[lease_takeover:dead step_attempt_abandoned:send]" {
		t.Fatalf("history: %v", kinds)
	}
}

// partitioned fails heartbeats for one owner, simulating an engine that is
// alive but cut off from the store.
type partitioned struct {
	store.Store
	owner string
	cut   atomic.Bool
}

func (p *partitioned) Heartbeat(ctx context.Context, id, owner string, until time.Time) error {
	if owner == p.owner && p.cut.Load() {
		return errors.New("network partition")
	}
	return p.Store.Heartbeat(ctx, id, owner, until)
}

// TestZombieEngineIsFenced: an engine loses its lease mid-step (heartbeats
// fail); another engine takes over. The zombie's in-flight step sees its
// context cancelled and it can no longer write.
func TestZombieEngineIsFenced(t *testing.T) {
	mem := store.NewMemory()
	p := &partitioned{Store: mem, owner: "zombie"}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	release := make(chan struct{})
	var zombieCtxErr atomic.Value
	var calls atomic.Int32
	def := saga.Define[order]("zombie").
		Step("slow", func(sc saga.Context, o *order) error {
			if calls.Add(1) == 1 {
				p.cut.Store(true)
				select {
				case <-sc.Done():
					zombieCtxErr.Store(sc.Err())
				case <-release:
				}
				return errors.New("interrupted")
			}
			return nil
		}).MustBuild()
	zombie := saga.New(p, saga.WithOwner("zombie"), saga.WithLogger(saga.DiscardLogger()), saga.WithLease(150*time.Millisecond), saga.WithPollInterval(5*time.Millisecond))
	heir := saga.New(p, saga.WithOwner("heir"), saga.WithLogger(saga.DiscardLogger()), saga.WithLease(150*time.Millisecond), saga.WithPollInterval(5*time.Millisecond))
	zombie.Register(def)
	heir.Register(def)
	id, _ := saga.Start(ctx, zombie, def, "k", order{})
	go zombie.Run(ctx)
	for calls.Load() == 0 {
		time.Sleep(time.Millisecond)
	}
	go heir.Run(ctx)
	wctx, wcancel := context.WithTimeout(ctx, 10*time.Second)
	defer wcancel()
	in, err := heir.Wait(wctx, id)
	close(release)
	if err != nil || in.Status != saga.StatusCompleted {
		t.Fatal(err, in.Status)
	}
	if zombieCtxErr.Load() == nil {
		t.Fatal("zombie step context was not cancelled on lease loss")
	}
	if in.Steps[0].Abandoned != 1 || in.Steps[0].Attempts != 1 {
		t.Fatalf("attempts=%d abandoned=%d", in.Steps[0].Attempts, in.Steps[0].Abandoned)
	}
}

// TestShutdownLeavesAttemptForTheNextOwner cancels Run while a step is in
// flight; the attempt stays RUNNING and the next engine re-runs it.
func TestShutdownLeavesAttemptForTheNextOwner(t *testing.T) {
	st := store.NewMemory()
	var calls atomic.Int32
	inStep := make(chan struct{})
	def := saga.Define[order]("shutdown").Step("s", func(sc saga.Context, o *order) error {
		if calls.Add(1) == 1 {
			close(inStep)
			<-sc.Done()
			return sc.Err()
		}
		return nil
	}).MustBuild()
	ctx1, stop := context.WithCancel(context.Background())
	e1 := saga.New(st, saga.WithOwner("e1"), saga.WithLogger(saga.DiscardLogger()), saga.WithPollInterval(time.Millisecond))
	e1.Register(def)
	id, _ := saga.Start(ctx1, e1, def, "k", order{})
	done := make(chan struct{})
	go func() { e1.Run(ctx1); close(done) }()
	<-inStep
	stop()
	<-done
	in, _ := st.Get(context.Background(), id)
	if in.Steps[0].Status != store.StepRunning || in.Owner != "" {
		t.Fatalf("after shutdown: step %s owner %q", in.Steps[0].Status, in.Owner)
	}
	e2 := saga.New(st, saga.WithOwner("e2"), saga.WithLogger(saga.DiscardLogger()))
	e2.Register(def)
	e2.Tick(context.Background())
	in, _ = st.Get(context.Background(), id)
	if in.Status != saga.StatusCompleted || in.Steps[0].Abandoned != 1 {
		t.Fatalf("got %s abandoned=%d", in.Status, in.Steps[0].Abandoned)
	}
}

// conflicting fails the n-th Update with ErrConflict, as if another engine
// had taken the saga over between two writes.
type conflicting struct {
	store.Store
	n, fail int
}

func (c *conflicting) Update(ctx context.Context, in *store.Instance, owner string) error {
	c.n++
	if c.n == c.fail {
		return store.ErrConflict
	}
	return c.Store.Update(ctx, in, owner)
}

func TestLostLeaseStopsWork(t *testing.T) {
	c := &conflicting{Store: store.NewMemory(), fail: 2}
	calls := 0
	def := saga.Define[order]("lost").Step("a", func(sc saga.Context, o *order) error { calls++; return nil }).Step("b", step("b")).MustBuild()
	e := saga.New(c, saga.WithLogger(saga.DiscardLogger()))
	e.Register(def)
	id, _ := saga.Start(context.Background(), e, def, "k", order{})
	e.Tick(context.Background())
	in, _ := c.Get(context.Background(), id)
	if calls != 1 || in.Steps[1].Status != store.StepPending {
		t.Fatalf("engine kept working after losing its lease: calls=%d b=%s", calls, in.Steps[1].Status)
	}
}
