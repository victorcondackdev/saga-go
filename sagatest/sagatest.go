// Package sagatest is a deterministic simulator for saga definitions.
//
// Explore runs a definition many times against an in-memory store with a
// fake clock. The first run is clean; it counts every crash point the
// engine passes through. Each following run crashes the engine at one of
// those points (or, with Depth > 1, at several: a crash during the recovery
// of a crash, and so on), starts a fresh engine on the same store, as a new
// process would, and drives the saga to rest. After every run the generic
// saga invariants are checked (see CheckInvariants) along with the
// harness's own domain checks:
//
//	func TestPayoutCrashSafety(t *testing.T) {
//		sagatest.Explore(t, sagatest.Config{Depth: 2}, func(seed uint64) sagatest.Harness[Payout] {
//			ledger, psp := newFakes(seed)
//			return sagatest.Harness[Payout]{
//				Definition: NewDefinition(ledger, psp),
//				Input:      Payout{Account: "acc-1", Amount: 100_00},
//				Check: func(t testing.TB, o *sagatest.Outcome[Payout]) {
//					// money is either moved exactly once or not at all
//				},
//			}
//		})
//	}
//
// Everything is deterministic for a given seed: step functions should take
// their randomness from the seed they are built with.
package sagatest

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/victorcondackdev/saga-go/saga"
	"github.com/victorcondackdev/saga-go/store"
)

// Clock is a manually advanced saga.Clock.
type Clock struct {
	mu sync.Mutex
	t  time.Time
}

// NewClock returns a clock set to t.
func NewClock(t time.Time) *Clock { return &Clock{t: t} }

// Now implements saga.Clock.
func (c *Clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

// Advance moves the clock forward by d.
func (c *Clock) Advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

// Set moves the clock to t if t is later than now.
func (c *Clock) Set(t time.Time) {
	c.mu.Lock()
	if t.After(c.t) {
		c.t = t
	}
	c.mu.Unlock()
}

// Harness is one scenario: a freshly built definition (with fresh fakes
// behind it), its input and the checks to run on the outcome.
type Harness[S any] struct {
	Definition *saga.Definition[S]
	Input      S
	// Signal is consulted when the saga is parked waiting for a named
	// signal. Return ok=false to never deliver it, so that the waiting
	// step runs into its Deadline.
	Signal func(name string, state S) (payload any, ok bool)
	// Check validates domain invariants, e.g. that money moved exactly
	// once or not at all. Report failures through t.
	Check func(t testing.TB, o *Outcome[S])
	// AllowNeedsAttention accepts FAILED_NEEDS_ATTENTION as a final status.
	AllowNeedsAttention bool
}

// Config tunes the exploration.
type Config struct {
	// Depth is the number of crashes injected per run. Default 1.
	Depth int
	// Seeds are explored one after another. Default {1}.
	Seeds []uint64
	// MaxRuns bounds the total number of runs. Default 20000.
	MaxRuns int
	// Lease is the engine lease. The fake clock jumps past it after a
	// crash. Default 30s.
	Lease time.Duration
	// MaxTicks bounds the number of engine ticks per run. Default 10000.
	MaxTicks int
	// NewStore returns the store for one run. Default store.NewMemory.
	NewStore func(t testing.TB) store.Store
	// Start is the fake clock's initial time. Default 2026-01-01 UTC.
	Start time.Time
}

func (c Config) withDefaults() Config {
	if c.Depth <= 0 {
		c.Depth = 1
	}
	if len(c.Seeds) == 0 {
		c.Seeds = []uint64{1}
	}
	if c.MaxRuns <= 0 {
		c.MaxRuns = 20000
	}
	if c.Lease <= 0 {
		c.Lease = 30 * time.Second
	}
	if c.MaxTicks <= 0 {
		c.MaxTicks = 10000
	}
	if c.Start.IsZero() {
		c.Start = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	}
	return c
}

// CrashSite is where a simulated crash happened.
type CrashSite struct {
	Hit   int // global index of the crash point hit in the run
	Point saga.CrashPoint
	Step  string
}

func (c CrashSite) String() string {
	if c.Step == "" {
		return fmt.Sprintf("#%d %s", c.Hit, c.Point)
	}
	return fmt.Sprintf("#%d %s(%s)", c.Hit, c.Point, c.Step)
}

// Outcome is the result of one simulated run.
type Outcome[S any] struct {
	Seed         uint64
	Crashes      []CrashSite
	Hits         int // crash points passed (including the crashing ones)
	Incarnations int // engines started (1 + number of crashes)
	Instance     *store.Instance
	State        S
	Invocations  []saga.Invocation
}

// Succeeded counts successful forward invocations of step.
func (o *Outcome[S]) Succeeded(step string) int { return o.count(step, false, true) }

// Compensated counts successful compensation invocations of step.
func (o *Outcome[S]) Compensated(step string) int { return o.count(step, true, true) }

// Invoked counts forward invocations of step, successful or not.
func (o *Outcome[S]) Invoked(step string) int { return o.count(step, false, false) }

func (o *Outcome[S]) count(step string, comp, okOnly bool) int {
	n := 0
	for _, inv := range o.Invocations {
		if inv.Step == step && inv.Compensation == comp && (!okOnly || inv.Err == nil) {
			n++
		}
	}
	return n
}

// Describe renders the run (crashes, invocations, timeline) for failure
// messages.
func (o *Outcome[S]) Describe() string {
	var b strings.Builder
	fmt.Fprintf(&b, "seed=%d crashes=%v incarnations=%d\ninvocations:\n", o.Seed, o.Crashes, o.Incarnations)
	for i, inv := range o.Invocations {
		kind := "step"
		if inv.Compensation {
			kind = "comp"
		}
		res := "ok"
		if inv.Err != nil {
			res = inv.Err.Error()
		}
		fmt.Fprintf(&b, "  %2d %s %s #%d: %s\n", i, kind, inv.Step, inv.Attempt, res)
	}
	if o.Instance != nil {
		_ = saga.RenderTimeline(&b, o.Instance, 30)
	}
	return b.String()
}

// Report summarises an exploration.
type Report struct {
	Runs int
	// Sites counts runs that crashed at each "point(step)".
	Sites map[string]int
	// Outcomes counts final statuses.
	Outcomes map[store.Status]int
	// Baseline is the number of crash points of the crash-free run of the
	// first seed.
	Baseline int
}

func (r Report) String() string {
	sites := make([]string, 0, len(r.Sites))
	for k := range r.Sites {
		sites = append(sites, k)
	}
	sort.Strings(sites)
	return fmt.Sprintf("%d runs, %d crash points in the clean run, %d distinct crash sites, outcomes %v",
		r.Runs, r.Baseline, len(sites), r.Outcomes)
}

// Simulate runs h once with crashes injected at the given hit indices and
// returns the outcome. It is deterministic for a given seed and plan.
func Simulate[S any](cfg Config, h Harness[S], seed uint64, plan []int, tb testing.TB) (*Outcome[S], error) {
	cfg = cfg.withDefaults()
	ctx := context.Background()
	clock := NewClock(cfg.Start)
	var st store.Store
	if cfg.NewStore != nil {
		st = cfg.NewStore(tb)
	} else {
		st = store.NewMemory()
	}
	defer st.Close()

	o := &Outcome[S]{Seed: seed}
	crashAt := map[int]bool{}
	for _, p := range plan {
		crashAt[p] = true
	}
	injector := func(p saga.CrashPoint, step string) bool {
		hit := o.Hits
		o.Hits++
		if crashAt[hit] {
			o.Crashes = append(o.Crashes, CrashSite{Hit: hit, Point: p, Step: step})
			return true
		}
		return false
	}
	var mu sync.Mutex
	observe := func(inv saga.Invocation) {
		mu.Lock()
		o.Invocations = append(o.Invocations, inv)
		mu.Unlock()
	}
	delivered := map[string]bool{}
	id := ""
	ticks := 0
	for {
		o.Incarnations++
		eng := saga.New(st,
			saga.WithClock(clock),
			saga.WithOwner(fmt.Sprintf("sim-%d", o.Incarnations)),
			saga.WithFaultInjector(injector),
			saga.WithSerialGroups(),
			saga.WithRandSeed(seed),
			saga.WithInvocationObserver(observe),
			saga.WithLease(cfg.Lease),
			saga.WithLogger(saga.DiscardLogger()),
		)
		if err := eng.Register(h.Definition); err != nil {
			return o, err
		}
		if id == "" {
			var err error
			if id, err = saga.Start(ctx, eng, h.Definition, "sim", h.Input); err != nil {
				return o, err
			}
		}
		crashed := false
		for !crashed {
			ticks++
			if ticks > cfg.MaxTicks {
				in, _ := st.Get(ctx, id)
				o.Instance = in
				return o, fmt.Errorf("saga did not come to rest after %d ticks", cfg.MaxTicks)
			}
			n, err := eng.Tick(ctx)
			if errors.Is(err, saga.ErrCrashInjected) {
				crashed = true
				// The dead engine's lease must expire before anyone
				// else may take over.
				clock.Advance(cfg.Lease + time.Second)
				continue
			}
			if err != nil {
				return o, err
			}
			in, err := st.Get(ctx, id)
			if err != nil {
				return o, err
			}
			if !in.Status.Runnable() {
				o.Instance = in
				o.State, err = saga.StateOf[S](in)
				return o, err
			}
			if n > 0 {
				continue
			}
			if name := waitingOn(in); name != "" && !delivered[name] && h.Signal != nil {
				state, _ := saga.StateOf[S](in)
				if p, ok := h.Signal(name, state); ok {
					delivered[name] = true
					if err := eng.Signal(ctx, id, name, p); err != nil {
						return o, err
					}
					continue
				}
			}
			next := in.NextRunAt
			if in.Owner != "" && in.LeaseUntil.After(next) {
				next = in.LeaseUntil
			}
			if next.After(clock.Now()) {
				clock.Set(next)
			} else {
				clock.Advance(time.Millisecond)
			}
		}
	}
}

func waitingOn(in *store.Instance) string {
	for _, r := range in.Steps {
		if r.Status == store.StepWaiting {
			if _, ok := in.Signals[r.Waiting]; !ok {
				return r.Waiting
			}
		}
	}
	return ""
}

// Explore runs the crash-free scenario and then every crash plan up to
// cfg.Depth crashes, for every seed, checking invariants after each run.
// build must return a fresh harness (fresh fakes) on every call.
func Explore[S any](t testing.TB, cfg Config, build func(seed uint64) Harness[S]) Report {
	t.Helper()
	cfg = cfg.withDefaults()
	rep := Report{Sites: map[string]int{}, Outcomes: map[store.Status]int{}}
	failures := 0
	for si, seed := range cfg.Seeds {
		var explore func(plan []int)
		explore = func(plan []int) {
			if rep.Runs >= cfg.MaxRuns || failures >= 5 {
				return
			}
			h := build(seed)
			o, err := Simulate(cfg, h, seed, plan, t)
			rep.Runs++
			if si == 0 && len(plan) == 0 {
				rep.Baseline = o.Hits
			}
			for _, c := range o.Crashes {
				rep.Sites[fmt.Sprintf("%s(%s)", c.Point, c.Step)]++
			}
			if err != nil {
				failures++
				t.Errorf("seed %d, crashes %v: %v\n%s", seed, o.Crashes, err, o.Describe())
				return
			}
			rep.Outcomes[o.Instance.Status]++
			errs := CheckInvariants(h, o)
			if h.Check != nil {
				rt := &recorder{TB: t}
				h.Check(rt, o)
				errs = append(errs, rt.errs...)
			}
			if len(errs) > 0 {
				failures++
				t.Errorf("seed %d, crashes %v: invariant violations:\n  %s\n%s", seed, o.Crashes, joinErrs(errs), o.Describe())
			}
			if len(plan) < cfg.Depth {
				start := 0
				if len(plan) > 0 {
					start = plan[len(plan)-1] + 1
				}
				for k := start; k < o.Hits; k++ {
					explore(append(append([]int(nil), plan...), k))
				}
			}
		}
		explore(nil)
	}
	return rep
}

// recorder collects Errorf calls from a harness Check so they can be
// reported with the run's context.
type recorder struct {
	testing.TB
	errs []error
}

func (r *recorder) Errorf(format string, args ...any) {
	r.errs = append(r.errs, fmt.Errorf(format, args...))
}
func (r *recorder) Error(args ...any)                 { r.errs = append(r.errs, errors.New(fmt.Sprint(args...))) }
func (r *recorder) Fatalf(format string, args ...any) { r.Errorf(format, args...) }
func (r *recorder) Fatal(args ...any)                 { r.Error(args...) }

func joinErrs(errs []error) string {
	s := make([]string, len(errs))
	for i, e := range errs {
		s[i] = e.Error()
	}
	return strings.Join(s, "\n  ")
}

// CheckInvariants verifies the guarantees the engine makes, from the
// invocations observed and the final record:
//
//  1. the saga is at rest in an allowed status;
//  2. every invocation of a step used the same idempotency key, and its
//     compensation a different, equally stable one;
//  3. COMPLETED: every step is DONE or SKIPPED and nothing was compensated;
//  4. nothing is compensated that was never attempted, and a step that
//     never succeeded is compensated only if it opted in with
//     CompensateAttempted;
//  5. COMPENSATED: every compensable step whose function ever succeeded was
//     compensated successfully (this catches non-idempotent steps whose
//     retry after a crash returns a different answer);
//  6. once the pivot succeeded, nothing is compensated and the saga is not
//     COMPENSATED;
//  7. no step starts before every step of the previous groups succeeded or
//     was skipped, and no step runs after compensation started;
//  8. compensations run in reverse order of the steps' first attempts.
func CheckInvariants[S any](h Harness[S], o *Outcome[S]) []error {
	var errs []error
	fail := func(format string, args ...any) { errs = append(errs, fmt.Errorf(format, args...)) }
	in := o.Instance
	infos := h.Definition.Steps()
	info := map[string]saga.StepInfo{}
	for _, s := range infos {
		info[s.Name] = s
	}
	rec := map[string]store.StepRecord{}
	for _, r := range in.Steps {
		rec[r.Name] = r
	}

	switch in.Status {
	case store.StatusCompleted, store.StatusCompensated:
	case store.StatusNeedsAttention:
		if !h.AllowNeedsAttention {
			fail("saga ended in FAILED_NEEDS_ATTENTION: %s", in.Failure)
		}
	default:
		fail("saga is not at rest: %s", in.Status)
	}

	firstComp := -1
	pivotOK := -1
	firstOK := map[string]int{}
	firstCompOK := map[string]int{}
	for i, inv := range o.Invocations {
		want := saga.IdempotencyKey(in.ID, inv.Step, inv.Compensation)
		if inv.Key != want {
			fail("invocation %d of %q used key %q, want %q", i, inv.Step, inv.Key, want)
		}
		if inv.Compensation {
			if firstComp < 0 {
				firstComp = i
			}
			if inv.Err == nil {
				if _, ok := firstCompOK[inv.Step]; !ok {
					firstCompOK[inv.Step] = i
				}
			}
			if pivotOK >= 0 {
				fail("compensation of %q (invocation %d) after the pivot succeeded (invocation %d)", inv.Step, i, pivotOK)
			}
			continue
		}
		if firstComp >= 0 {
			fail("step %q invoked (invocation %d) after compensation started (invocation %d)", inv.Step, i, firstComp)
		}
		if inv.Err == nil {
			if _, ok := firstOK[inv.Step]; !ok {
				firstOK[inv.Step] = i
			}
			if info[inv.Step].Pivot && pivotOK < 0 {
				pivotOK = i
			}
		}
		// Ordering: every non-skipped step of an earlier group must have
		// succeeded before this invocation.
		g := info[inv.Step].Group
		for _, s := range infos {
			if s.Group >= g || rec[s.Name].Status == store.StepSkipped {
				continue
			}
			if at, ok := firstOK[s.Name]; !ok || at > i {
				fail("step %q (group %d) invoked before %q (group %d) succeeded", inv.Step, g, s.Name, s.Group)
			}
		}
	}

	for _, s := range infos {
		r := rec[s.Name]
		compInvoked := o.count(s.Name, true, false) > 0
		if compInvoked && o.Invoked(s.Name) == 0 {
			fail("step %q compensated but never attempted", s.Name)
		}
		if compInvoked && o.Succeeded(s.Name) == 0 && !s.CompensateAttempted {
			fail("step %q compensated although it never succeeded and is not CompensateAttempted", s.Name)
		}
		switch in.Status {
		case store.StatusCompleted:
			if r.Status != store.StepDone && r.Status != store.StepSkipped {
				fail("saga COMPLETED but step %q is %s", s.Name, r.Status)
			}
			if r.Status == store.StepDone && o.Succeeded(s.Name) == 0 {
				fail("step %q is DONE without a successful invocation", s.Name)
			}
		case store.StatusCompensated:
			if s.Compensable && o.Succeeded(s.Name) > 0 && o.Compensated(s.Name) == 0 {
				fail("step %q succeeded but was never compensated", s.Name)
			}
		}
	}
	if in.Status == store.StatusCompleted && firstComp >= 0 {
		fail("saga COMPLETED but compensations were invoked")
	}
	if in.Status == store.StatusCompensated && pivotOK >= 0 {
		fail("saga COMPENSATED although the pivot succeeded")
	}

	// Compensation order: reverse StartSeq.
	type cs struct {
		name    string
		at, seq int
	}
	var order []cs
	for name, at := range firstCompOK {
		order = append(order, cs{name, at, rec[name].StartSeq})
	}
	sort.Slice(order, func(i, j int) bool { return order[i].at < order[j].at })
	for i := 1; i < len(order); i++ {
		if order[i].seq > order[i-1].seq {
			fail("compensation of %q ran after %q but %q started later", order[i].name, order[i-1].name, order[i].name)
		}
	}
	return errs
}
