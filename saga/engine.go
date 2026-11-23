package saga

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	mrand "math/rand/v2"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/victorcondackdev/saga-go/store"
)

// Status aliases store.Status so callers rarely need to import store.
type Status = store.Status

// Saga statuses, re-exported from package store.
const (
	StatusPending        = store.StatusPending
	StatusRunning        = store.StatusRunning
	StatusCompleted      = store.StatusCompleted
	StatusCompensating   = store.StatusCompensating
	StatusCompensated    = store.StatusCompensated
	StatusNeedsAttention = store.StatusNeedsAttention
)

// Clock abstracts time for the engine. Leases, backoff schedules, deadlines
// and history timestamps all come from it; sagatest drives it by hand.
type Clock interface{ Now() time.Time }

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }

// CrashPoint names a place in the engine where a FaultInjector can simulate
// the process dying.
type CrashPoint string

// Crash points, in the order a step goes through them.
const (
	// Before the attempt is recorded (nothing about it is durable).
	CrashBeforeStepStart CrashPoint = "before-step-start"
	// After the attempt is durably recorded as RUNNING, before the step
	// function is called.
	CrashBeforeStepInvoke CrashPoint = "before-step-invoke"
	// After the step function returned (its side effects happened), before
	// the outcome is persisted. The classic at-least-once window.
	CrashAfterStepInvoke CrashPoint = "after-step-invoke"
	// After the outcome is persisted.
	CrashAfterStepPersist CrashPoint = "after-step-persist"
	// After every branch of a parallel group is done, before the merged
	// state is persisted.
	CrashBeforeMerge CrashPoint = "before-merge"
	// The same four points for compensations.
	CrashBeforeCompStart  CrashPoint = "before-compensation-start"
	CrashBeforeCompInvoke CrashPoint = "before-compensation-invoke"
	CrashAfterCompInvoke  CrashPoint = "after-compensation-invoke"
	CrashAfterCompPersist CrashPoint = "after-compensation-persist"
	CrashBeforeCompensate CrashPoint = "before-compensating-persist"
	CrashBeforeTerminal   CrashPoint = "before-terminal-persist"
	CrashBeforeSuspend    CrashPoint = "before-suspend-persist"
)

// FaultInjector is consulted at every CrashPoint. Returning true makes the
// engine abandon the instance on the spot, without persisting or releasing
// anything, exactly as if the process had been killed. Used by sagatest.
type FaultInjector func(p CrashPoint, step string) bool

// Invocation describes one call of a step or compensation function. It is
// reported to InvocationObservers as soon as the function returns, before
// the outcome is persisted (so it is reported even if the engine then
// crashes).
type Invocation struct {
	SagaID       string
	Type         string
	Step         string
	Key          string
	Compensation bool
	Attempt      int
	Start, End   time.Time
	Err          error
}

// HookEvent is delivered to hooks after the event it carries is durably
// persisted.
type HookEvent struct {
	SagaID string
	Type   string
	Status Status
	Event  store.Event
}

// Option configures an Engine.
type Option func(*Engine)

// WithOwner sets the identity used for leases. It must be unique per engine
// sharing a store. Default: hostname-pid-random.
func WithOwner(id string) Option { return func(e *Engine) { e.owner = id } }

// WithClock replaces the wall clock.
func WithClock(c Clock) Option { return func(e *Engine) { e.clock = c } }

// WithLogger sets the structured logger. Default slog.Default().
func WithLogger(l *slog.Logger) Option { return func(e *Engine) { e.log = l } }

// WithLease sets the lease duration. Workers heartbeat every third of it.
// Default 30s.
func WithLease(d time.Duration) Option { return func(e *Engine) { e.lease = d } }

// WithConcurrency sets how many instances Run processes at once. Default 16.
func WithConcurrency(n int) Option { return func(e *Engine) { e.concurrency = n } }

// WithPollInterval sets how often Run polls the store when idle. Default
// 50ms.
func WithPollInterval(d time.Duration) Option { return func(e *Engine) { e.poll = d } }

// WithWaitRecheck sets how often a step parked on Context.Await is re-run
// even without a signal. Default one minute.
func WithWaitRecheck(d time.Duration) Option { return func(e *Engine) { e.waitRecheck = d } }

// WithMaxAbandoned sets how many attempts of a step may be abandoned by
// crashed workers before the saga is parked in FAILED_NEEDS_ATTENTION (a
// step that keeps killing its worker is a poison pill). Default 5.
func WithMaxAbandoned(n int) Option { return func(e *Engine) { e.maxAbandoned = n } }

// WithSerialGroups runs the branches of parallel groups one after another,
// in declaration order. Durable semantics are unchanged; execution becomes
// deterministic, which sagatest relies on.
func WithSerialGroups() Option { return func(e *Engine) { e.serialGroups = true } }

// WithRandSeed makes retry jitter deterministic.
func WithRandSeed(seed uint64) Option {
	return func(e *Engine) { e.rnd = mrand.New(mrand.NewPCG(seed, seed^0x5a6a)) }
}

// WithFaultInjector installs a crash simulator. Testing only.
func WithFaultInjector(f FaultInjector) Option { return func(e *Engine) { e.fault = f } }

// WithHook registers a function called for every persisted history event.
// Hooks run synchronously on the worker and must be fast.
func WithHook(h func(HookEvent)) Option { return func(e *Engine) { e.hooks = append(e.hooks, h) } }

// WithInvocationObserver registers a function called after every step or
// compensation invocation.
func WithInvocationObserver(f func(Invocation)) Option {
	return func(e *Engine) { e.invObs = append(e.invObs, f) }
}

// Engine runs saga instances stored in a store.Store. Any number of engines,
// in one process or several, can share a store: leases ensure that an
// instance is driven by one engine at a time and that a crashed engine's
// instances are taken over once its lease expires.
type Engine struct {
	st           store.Store
	owner        string
	clock        Clock
	log          *slog.Logger
	lease        time.Duration
	poll         time.Duration
	waitRecheck  time.Duration
	concurrency  int
	maxAbandoned int
	serialGroups bool
	fault        FaultInjector
	hooks        []func(HookEvent)
	invObs       []func(Invocation)

	rndMu sync.Mutex
	rnd   *mrand.Rand

	mu   sync.RWMutex
	defs map[string]*definition

	m    *engineMetrics
	wake chan struct{}
}

// New returns an engine over st.
func New(st store.Store, opts ...Option) *Engine {
	e := &Engine{
		st:           st,
		clock:        systemClock{},
		lease:        30 * time.Second,
		poll:         50 * time.Millisecond,
		waitRecheck:  time.Minute,
		concurrency:  16,
		maxAbandoned: 5,
		defs:         map[string]*definition{},
		wake:         make(chan struct{}, 1),
	}
	for _, o := range opts {
		o(e)
	}
	if e.owner == "" {
		host, _ := os.Hostname()
		e.owner = host + "-" + strconv.Itoa(os.Getpid()) + "-" + randHex(3)
	}
	if e.log == nil {
		e.log = slog.Default()
	}
	if e.rnd == nil {
		e.rnd = mrand.New(mrand.NewPCG(mrand.Uint64(), mrand.Uint64()))
	}
	if e.concurrency <= 0 {
		e.concurrency = 1
	}
	if e.waitRecheck <= 0 {
		e.waitRecheck = time.Minute
	}
	if e.lease <= 0 {
		e.lease = 30 * time.Second
	}
	e.m = newEngineMetrics()
	return e
}

// DiscardLogger returns a logger that drops everything, for tests.
func DiscardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// Owner returns the lease identity of the engine.
func (e *Engine) Owner() string { return e.owner }

// Store returns the engine's store.
func (e *Engine) Store() store.Store { return e.st }

// Register makes a definition runnable. Registering the same name twice is
// an error.
func (e *Engine) Register(defs ...Registrable) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, d := range defs {
		def := d.definition()
		if _, ok := e.defs[def.name]; ok {
			return fmt.Errorf("saga: type %q already registered", def.name)
		}
		e.defs[def.name] = def
	}
	return nil
}

// Types returns the registered saga type names.
func (e *Engine) Types() []string {
	e.mu.RLock()
	defer e.mu.RUnlock()
	out := make([]string, 0, len(e.defs))
	for k := range e.defs {
		out = append(out, k)
	}
	return out
}

func (e *Engine) def(name string) *definition {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.defs[name]
}

func (e *Engine) random() float64 {
	e.rndMu.Lock()
	defer e.rndMu.Unlock()
	return e.rnd.Float64()
}

func (e *Engine) nudge() {
	select {
	case e.wake <- struct{}{}:
	default:
	}
}

func randHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// Start creates an instance of saga type typ with the given JSON input. It
// is idempotent on (typ, key): starting again with the same key returns the
// existing instance and created=false, or ErrKeyConflict if the input
// differs. An empty key always creates a new instance.
func (e *Engine) Start(ctx context.Context, typ, key string, input json.RawMessage) (inst *store.Instance, created bool, err error) {
	d := e.def(typ)
	if d == nil {
		return nil, false, fmt.Errorf("%w: %q", ErrUnknownType, typ)
	}
	if len(input) == 0 {
		input = json.RawMessage("{}")
	}
	if err := d.check(input); err != nil {
		return nil, false, fmt.Errorf("%w for %q: %v", ErrInvalidInput, typ, err)
	}
	sum := sha256.Sum256(input)
	now := e.clock.Now()
	in := &store.Instance{
		ID:        "sg_" + randHex(10),
		Type:      typ,
		Key:       key,
		InputHash: hex.EncodeToString(sum[:]),
		DefHash:   d.hash,
		Status:    store.StatusPending,
		State:     append(json.RawMessage(nil), input...),
		CreatedAt: now,
		UpdatedAt: now,
		NextRunAt: now,
		Steps:     make([]store.StepRecord, len(d.steps)),
	}
	if in.Key == "" {
		in.Key = in.ID
	}
	if d.timeout > 0 {
		in.Deadline = now.Add(d.timeout)
	}
	for i, s := range d.steps {
		in.Steps[i] = store.StepRecord{Name: s.name, Status: store.StepPending}
	}
	ev := store.Event{Seq: 1, At: now, Kind: store.EvCreated, Detail: "key=" + in.Key}
	in.History = []store.Event{ev}
	out, created, err := e.st.Create(ctx, in)
	if err != nil {
		return nil, false, err
	}
	if !created {
		if out.InputHash != in.InputHash {
			return out, false, fmt.Errorf("%w (key %q, saga %s)", ErrKeyConflict, key, out.ID)
		}
		return out, false, nil
	}
	e.m.started.Inc(typ)
	e.emit(out, ev)
	e.nudge()
	return out, true, nil
}

// Start is the typed form of Engine.Start.
func Start[S any](ctx context.Context, e *Engine, d *Definition[S], key string, input S) (string, error) {
	b, err := json.Marshal(input)
	if err != nil {
		return "", err
	}
	in, _, err := e.Start(ctx, d.Name(), key, b)
	if in != nil {
		return in.ID, err
	}
	return "", err
}

// StateOf decodes the state of an instance.
func StateOf[S any](in *store.Instance) (S, error) {
	var s S
	err := json.Unmarshal(in.State, &s)
	return s, err
}

// Get returns an instance.
func (e *Engine) Get(ctx context.Context, id string) (*store.Instance, error) {
	return e.st.Get(ctx, id)
}

// List returns instances matching f.
func (e *Engine) List(ctx context.Context, f store.Filter) ([]*store.Instance, error) {
	return e.st.List(ctx, f)
}

// Stuck returns instances that need a human (FAILED_NEEDS_ATTENTION) and
// instances still in flight that have not been updated for olderThan.
func (e *Engine) Stuck(ctx context.Context, olderThan time.Duration) ([]*store.Instance, error) {
	att, err := e.st.List(ctx, store.Filter{Statuses: []store.Status{store.StatusNeedsAttention}})
	if err != nil {
		return nil, err
	}
	old, err := e.st.List(ctx, store.Filter{
		Statuses:      []store.Status{store.StatusPending, store.StatusRunning, store.StatusCompensating},
		UpdatedBefore: e.clock.Now().Add(-olderThan),
	})
	if err != nil {
		return nil, err
	}
	return append(att, old...), nil
}

// Signal delivers a named payload to an instance (for example a settlement
// webhook). A step blocked in Context.Await on that name is re-run promptly.
// Delivering the same name again overwrites the payload.
func (e *Engine) Signal(ctx context.Context, id, name string, payload any) error {
	var raw json.RawMessage
	switch p := payload.(type) {
	case json.RawMessage:
		raw = p
	case []byte:
		raw = p
	default:
		b, err := json.Marshal(p)
		if err != nil {
			return err
		}
		raw = b
	}
	if err := e.st.Signal(ctx, id, name, raw); err != nil {
		return err
	}
	e.nudge()
	return nil
}

// Retry moves an instance out of FAILED_NEEDS_ATTENTION by resetting the
// retry budget of whatever failed: a compensation (the saga resumes
// compensating) or a step past the pivot (the saga resumes forward).
func (e *Engine) Retry(ctx context.Context, id, note string) error {
	in, err := e.st.Get(ctx, id)
	if err != nil {
		return err
	}
	if in.Status != store.StatusNeedsAttention {
		return fmt.Errorf("%w: %s is %s", ErrInvalidTransition, id, in.Status)
	}
	if d := e.def(in.Type); d == nil || d.hash != in.DefHash {
		return fmt.Errorf("%w: definition for %q is missing or changed", ErrInvalidTransition, in.Type)
	}
	target, step := store.Status(""), ""
	for i := range in.Steps {
		r := &in.Steps[i]
		if r.Comp == store.CompFailed {
			r.Comp, r.CompAttempts, r.CompAbandoned, r.CompNextAt = store.CompRetry, 0, 0, time.Time{}
			target, step = store.StatusCompensating, r.Name
		}
	}
	if target == "" {
		for i := range in.Steps {
			r := &in.Steps[i]
			if r.Status == store.StepFailed {
				r.Status, r.Attempts, r.Abandoned, r.NextAttemptAt = store.StepRetry, 0, 0, time.Time{}
				r.FirstStartedAt = time.Time{}
				target, step = store.StatusRunning, r.Name
			}
		}
	}
	if target == "" {
		return fmt.Errorf("%w: nothing to retry in %s", ErrInvalidTransition, id)
	}
	now := e.clock.Now()
	in.Status, in.NextRunAt, in.UpdatedAt = target, time.Time{}, now
	ev := store.Event{Seq: len(in.History) + 1, At: now, Kind: store.EvManualRetry, Step: step, Detail: note}
	in.History = append(in.History, ev)
	if err := e.st.Update(ctx, in, ""); err != nil {
		return err
	}
	e.emit(in, ev)
	e.nudge()
	return nil
}

// Resolve closes an instance in FAILED_NEEDS_ATTENTION as COMPLETED or
// COMPENSATED after an operator fixed it out of band. The note is kept in
// the history.
func (e *Engine) Resolve(ctx context.Context, id string, outcome Status, note string) error {
	if outcome != store.StatusCompleted && outcome != store.StatusCompensated {
		return fmt.Errorf("%w: resolution must be COMPLETED or COMPENSATED, not %q", ErrInvalidTransition, outcome)
	}
	in, err := e.st.Get(ctx, id)
	if err != nil {
		return err
	}
	if in.Status != store.StatusNeedsAttention {
		return fmt.Errorf("%w: %s is %s", ErrInvalidTransition, id, in.Status)
	}
	now := e.clock.Now()
	in.Status, in.UpdatedAt = outcome, now
	ev := store.Event{Seq: len(in.History) + 1, At: now, Kind: store.EvManualResolution, Detail: string(outcome) + ": " + note}
	in.History = append(in.History, ev)
	if err := e.st.Update(ctx, in, ""); err != nil {
		return err
	}
	e.emit(in, ev)
	return nil
}

// Wait polls until the instance is no longer runnable (completed,
// compensated or needs attention) and returns it.
func (e *Engine) Wait(ctx context.Context, id string) (*store.Instance, error) {
	t := time.NewTicker(5 * time.Millisecond)
	defer t.Stop()
	for {
		in, err := e.st.Get(ctx, id)
		if err != nil {
			return nil, err
		}
		if !in.Status.Runnable() {
			return in, nil
		}
		select {
		case <-ctx.Done():
			return in, ctx.Err()
		case <-t.C:
		}
	}
}

// Tick acquires the instances that are due and drives each one, serially,
// as far as it can go. It returns how many it processed. It is the
// single-threaded building block of Run, convenient for tests and
// simulations. If a FaultInjector fires, Tick returns ErrCrashInjected at
// once.
func (e *Engine) Tick(ctx context.Context) (int, error) {
	ins, err := e.st.Acquire(ctx, e.owner, e.clock.Now(), e.lease, e.concurrency)
	for i, in := range ins {
		if xerr := e.execute(ctx, in, false); errors.Is(xerr, ErrCrashInjected) {
			return i + 1, xerr
		}
	}
	return len(ins), err
}

// Run processes instances until ctx is cancelled, with up to Concurrency
// instances in flight, each kept leased by a heartbeat. In-flight steps see
// their context cancelled on shutdown; their attempts are re-run by the
// next owner (they are durably marked as started).
func (e *Engine) Run(ctx context.Context) error {
	sem := make(chan struct{}, e.concurrency)
	var wg sync.WaitGroup
	defer wg.Wait()
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		free := cap(sem) - len(sem)
		full := false
		if free > 0 {
			ins, err := e.st.Acquire(ctx, e.owner, e.clock.Now(), e.lease, free)
			if err != nil && ctx.Err() == nil {
				e.log.Warn("acquire failed", "err", err)
			}
			for _, in := range ins {
				sem <- struct{}{}
				wg.Add(1)
				go func(in *store.Instance) {
					defer wg.Done()
					defer func() { <-sem; e.nudge() }()
					_ = e.execute(ctx, in, true)
				}(in)
			}
			full = len(ins) == free
		}
		if full {
			continue
		}
		timer.Reset(e.poll)
		select {
		case <-ctx.Done():
			return nil
		case <-e.wake:
		case <-timer.C:
		}
	}
}

func (e *Engine) execute(ctx context.Context, in *store.Instance, heartbeat bool) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var hb sync.WaitGroup
	if heartbeat {
		hb.Add(1)
		go func() {
			defer hb.Done()
			t := time.NewTicker(e.lease / 3)
			defer t.Stop()
			for {
				select {
				case <-runCtx.Done():
					return
				case <-t.C:
					err := e.st.Heartbeat(context.WithoutCancel(runCtx), in.ID, e.owner, e.clock.Now().Add(e.lease))
					if err != nil {
						e.log.Warn("lease lost", "saga_id", in.ID, "err", err)
						cancel()
						return
					}
				}
			}
		}()
	}
	err := e.process(runCtx, in)
	cancel()
	hb.Wait()
	switch {
	case err == nil, errors.Is(err, ErrCrashInjected):
	case errors.Is(err, store.ErrConflict):
		e.m.leaseLost.Inc(in.Type)
		e.log.Warn("lost lease while running; another engine owns the saga now", "saga_id", in.ID, "saga_type", in.Type)
	default:
		if ctx.Err() == nil && !errors.Is(err, context.Canceled) {
			e.log.Error("saga processing error", "saga_id", in.ID, "saga_type", in.Type, "err", err)
		}
		_ = e.st.Release(context.WithoutCancel(ctx), in.ID, e.owner)
	}
	return err
}

func (e *Engine) emit(in *store.Instance, ev store.Event) {
	e.m.observe(in.Type, ev)
	level := slog.LevelDebug
	switch ev.Kind {
	case store.EvNeedsAttention, store.EvCompFailed:
		level = slog.LevelError
	case store.EvStepRetry, store.EvStepFailed, store.EvCompRetry, store.EvStepAbandoned,
		store.EvCompAbandoned, store.EvCompensating, store.EvLeaseTakeover:
		level = slog.LevelWarn
	}
	if e.log.Enabled(context.Background(), level) {
		attrs := []any{"saga_id", in.ID, "saga_type", in.Type, "event", ev.Kind}
		if ev.Step != "" {
			attrs = append(attrs, "step", ev.Step, "attempt", ev.Attempt)
		}
		if ev.Error != "" {
			attrs = append(attrs, "err", ev.Error)
		}
		if ev.Detail != "" {
			attrs = append(attrs, "detail", ev.Detail)
		}
		e.log.Log(context.Background(), level, "saga event", attrs...)
	}
	for _, h := range e.hooks {
		h(HookEvent{SagaID: in.ID, Type: in.Type, Status: in.Status, Event: ev})
	}
}
