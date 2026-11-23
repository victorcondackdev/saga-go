package sagatest_test

import (
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

type acct struct {
	Holds   int  `json:"holds"`
	Charged bool `json:"charged"`
}

// capture is a testing.TB that records failures instead of failing, so we
// can assert that the simulator does catch bugs.
type capture struct {
	*testing.T
	mu   sync.Mutex
	errs []string
}

func (c *capture) Errorf(format string, args ...any) {
	c.mu.Lock()
	c.errs = append(c.errs, fmt.Sprintf(format, args...))
	c.mu.Unlock()
}

func (c *capture) has(sub string) bool {
	for _, e := range c.errs {
		if strings.Contains(e, sub) {
			return true
		}
	}
	return false
}

// bank is a fake downstream system; dedupe toggles idempotency.
type bank struct {
	dedupe  bool
	seen    map[string]bool
	effects int
}

func (b *bank) hold(key string) {
	if b.dedupe && b.seen[key] {
		return
	}
	b.seen[key] = true
	b.effects++
}

func build(dedupe bool) func(uint64) sagatest.Harness[acct] {
	return func(seed uint64) sagatest.Harness[acct] {
		b := &bank{dedupe: dedupe, seen: map[string]bool{}}
		def := saga.Define[acct]("bank").
			Step("hold", func(sc saga.Context, a *acct) error { b.hold(sc.IdempotencyKey()); a.Holds++; return nil },
				saga.Compensate(func(sc saga.Context, a *acct) error { b.effects--; return nil })).
			Step("charge", func(sc saga.Context, a *acct) error { a.Charged = true; return nil }, saga.Pivot()).
			MustBuild()
		return sagatest.Harness[acct]{
			Definition: def,
			Check: func(t testing.TB, o *sagatest.Outcome[acct]) {
				if o.Instance.Status == store.StatusCompleted && b.effects != 1 {
					t.Errorf("hold applied %d times", b.effects)
				}
			},
		}
	}
}

func TestExploreCorrectSaga(t *testing.T) {
	rep := sagatest.Explore(t, sagatest.Config{Depth: 3}, build(true))
	if rep.Runs < 50 || rep.Baseline == 0 || rep.Outcomes[store.StatusCompleted] != rep.Runs {
		t.Fatalf("report: %s", rep)
	}
	if !strings.Contains(rep.String(), "runs") || len(rep.Sites) == 0 {
		t.Fatal(rep.String())
	}
}

func TestExploreCatchesNonIdempotentStep(t *testing.T) {
	c := &capture{T: t}
	sagatest.Explore(c, sagatest.Config{}, build(false))
	if !c.has("hold applied 2 times") || !c.has("after-step-invoke(hold)") {
		t.Fatalf("double effect after a crash not reported: %v", c.errs)
	}
}

// A step whose retry after a crash gives a different answer (the first call
// succeeded, the second fails): its effect is never compensated.
func TestExploreCatchesNonDeterministicRetry(t *testing.T) {
	c := &capture{T: t}
	sagatest.Explore(c, sagatest.Config{}, func(seed uint64) sagatest.Harness[acct] {
		calls := 0
		def := saga.Define[acct]("flip").
			Step("hold", func(sc saga.Context, a *acct) error {
				calls++
				if calls > 1 {
					return saga.Permanent(errors.New("already exists")) // should have returned the original result
				}
				return nil
			}, saga.Compensate(func(sc saga.Context, a *acct) error { return nil })).
			MustBuild()
		return sagatest.Harness[acct]{Definition: def}
	})
	if !c.has(`step "hold" succeeded but was never compensated`) {
		t.Fatalf("not reported: %v", c.errs)
	}
}

func TestExploreReportsNeedsAttention(t *testing.T) {
	c := &capture{T: t}
	sagatest.Explore(c, sagatest.Config{}, func(seed uint64) sagatest.Harness[acct] {
		def := saga.Define[acct]("att").
			Step("pivot", func(sc saga.Context, a *acct) error { return nil }, saga.Pivot()).
			Step("after", func(sc saga.Context, a *acct) error { return saga.Permanent(errors.New("down")) }).
			MustBuild()
		return sagatest.Harness[acct]{Definition: def}
	})
	if !c.has("FAILED_NEEDS_ATTENTION") {
		t.Fatalf("not reported: %v", c.errs)
	}
}

func TestSimulateSignalsAndNonConvergence(t *testing.T) {
	def := saga.Define[acct]("sig").
		Step("wait", func(sc saga.Context, a *acct) error {
			var n int
			if err := sc.Await("go", &n); err != nil {
				return err
			}
			a.Holds = n
			return nil
		}).MustBuild()
	h := sagatest.Harness[acct]{Definition: def, Signal: func(name string, a acct) (any, bool) { return 7, name == "go" }}
	o, err := sagatest.Simulate(sagatest.Config{}, h, 1, nil, t)
	if err != nil || o.State.Holds != 7 || o.Instance.Status != store.StatusCompleted {
		t.Fatalf("%v %+v", err, o)
	}
	// Crash right before the step is invoked, then recover.
	o, err = sagatest.Simulate(sagatest.Config{}, h, 1, []int{1}, t)
	if err != nil || len(o.Crashes) != 1 || o.Incarnations != 2 || o.State.Holds != 7 {
		t.Fatalf("%v %+v", err, o)
	}
	if !strings.Contains(o.Describe(), "before-step-invoke(wait)") || !strings.Contains(o.Crashes[0].String(), "#1") {
		t.Fatal(o.Describe())
	}

	never := sagatest.Harness[acct]{Definition: def, Signal: func(string, acct) (any, bool) { return nil, false }}
	if _, err := sagatest.Simulate(sagatest.Config{MaxTicks: 50}, never, 1, nil, t); err == nil || !strings.Contains(err.Error(), "did not come to rest") {
		t.Fatalf("non-convergence not reported: %v", err)
	}
	c := &capture{T: t}
	sagatest.Explore(c, sagatest.Config{MaxTicks: 50}, func(uint64) sagatest.Harness[acct] { return never })
	if !c.has("did not come to rest") {
		t.Fatal(c.errs)
	}
}

func TestOutcomeCounters(t *testing.T) {
	o := &sagatest.Outcome[acct]{Invocations: []saga.Invocation{
		{Step: "a"}, {Step: "a", Err: errors.New("x")}, {Step: "a", Compensation: true},
	}}
	if o.Succeeded("a") != 1 || o.Invoked("a") != 2 || o.Compensated("a") != 1 {
		t.Fatal("counters")
	}
	if s := (sagatest.CrashSite{Hit: 3, Point: saga.CrashBeforeTerminal}).String(); s != "#3 before-terminal-persist" {
		t.Fatal(s)
	}
	if !strings.Contains(o.Describe(), "comp a #0: ok") {
		t.Fatal(o.Describe())
	}
}

func TestClock(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	c := sagatest.NewClock(t0)
	c.Advance(time.Second)
	c.Set(t0) // never goes back
	if !c.Now().Equal(t0.Add(time.Second)) {
		t.Fatal(c.Now())
	}
	c.Set(t0.Add(time.Hour))
	if !c.Now().Equal(t0.Add(time.Hour)) {
		t.Fatal(c.Now())
	}
}

func TestCustomStoreAndStart(t *testing.T) {
	var made int
	cfg := sagatest.Config{Start: time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC), NewStore: func(t testing.TB) store.Store {
		made++
		return store.NewMemory()
	}}
	o, err := sagatest.Simulate(cfg, build(true)(1), 1, nil, t)
	if err != nil || made != 1 || o.Instance.CreatedAt.Year() != 2030 {
		t.Fatal(err, made)
	}
}
