package saga

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/victorcondackdev/saga-go/store"
)

// run drives one leased instance. All mutations of in happen under mu so
// that parallel branches can share it; persist writes the whole instance
// through the store with the lease's Version as fencing token.
type run struct {
	e     *Engine
	d     *definition
	in    *store.Instance
	ctx   context.Context
	mu    sync.Mutex
	queue []store.Event // events waiting for a successful persist
}

func (e *Engine) process(ctx context.Context, in *store.Instance) error {
	d := e.def(in.Type)
	if d == nil {
		// Leave it for an engine that knows the type.
		_ = e.st.Release(ctx, in.ID, e.owner)
		return fmt.Errorf("%w: %q", ErrUnknownType, in.Type)
	}
	r := &run{e: e, d: d, in: in, ctx: ctx}
	if in.PreviousOwner != "" {
		r.event(store.Event{Kind: store.EvLeaseTakeover, Owner: in.PreviousOwner, Detail: "previous lease expired"})
	}
	if d.hash != in.DefHash {
		return r.needsAttention(fmt.Sprintf("definition of %q changed incompatibly since the saga started (hash %s, now %s)", in.Type, in.DefHash, d.hash))
	}
	r.recoverAbandoned()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		var cont bool
		var err error
		switch in.Status {
		case store.StatusPending:
			in.Status = store.StatusRunning
			r.event(store.Event{Kind: store.EvStarted})
			cont = true
		case store.StatusRunning:
			cont, err = r.forward()
		case store.StatusCompensating:
			cont, err = r.compensate()
		default:
			return e.st.Release(ctx, in.ID, e.owner)
		}
		if err != nil || !cont {
			return err
		}
	}
}

func (r *run) now() time.Time { return r.e.clock.Now() }

func (r *run) crash(p CrashPoint, step string) error {
	if r.e.fault != nil && r.e.fault(p, step) {
		return ErrCrashInjected
	}
	return nil
}

func (r *run) event(ev store.Event) {
	ev.Seq = len(r.in.History) + 1
	if ev.At.IsZero() {
		ev.At = r.now()
	}
	r.in.History = append(r.in.History, ev)
	r.queue = append(r.queue, ev)
}

// persist writes the instance. Callers hold mu (or run single-threaded).
func (r *run) persist() error {
	r.in.UpdatedAt = r.now()
	if err := r.e.st.Update(context.WithoutCancel(r.ctx), r.in, r.e.owner); err != nil {
		return err
	}
	q := r.queue
	r.queue = nil
	for _, ev := range q {
		r.e.emit(r.in, ev)
	}
	return nil
}

// release persists the instance with the lease dropped.
func (r *run) release() error {
	r.in.Owner, r.in.LeaseUntil = "", time.Time{}
	err := r.persist()
	r.e.nudge()
	return err
}

// recoverAbandoned turns attempts that were durably started but never
// finished (the previous owner died) into in-doubt retries.
func (r *run) recoverAbandoned() {
	now := r.now()
	for i := range r.in.Steps {
		rec := &r.in.Steps[i]
		if rec.Status == store.StepRunning {
			rec.Attempts--
			rec.Abandoned++
			rec.InDoubt = true
			rec.Status, rec.NextAttemptAt = store.StepRetry, now
			r.event(store.Event{Kind: store.EvStepAbandoned, Step: rec.Name, Attempt: rec.Attempts + rec.Abandoned,
				Detail: "attempt started by a previous owner never reported back; re-running"})
		}
		if rec.Comp == store.CompRunning {
			rec.CompAttempts--
			rec.CompAbandoned++
			rec.Comp, rec.CompNextAt = store.CompRetry, now
			r.event(store.Event{Kind: store.EvCompAbandoned, Step: rec.Name, Attempt: rec.CompAttempts + rec.CompAbandoned,
				Detail: "compensation started by a previous owner never reported back; re-running"})
		}
	}
}

func (r *run) suspend(wake time.Time) error {
	if !r.in.PivotPassed && !r.in.Deadline.IsZero() && r.in.Status == store.StatusRunning && wake.After(r.in.Deadline) {
		wake = r.in.Deadline
	}
	if err := r.crash(CrashBeforeSuspend, ""); err != nil {
		return err
	}
	r.in.NextRunAt = wake
	return r.release()
}

func (r *run) needsAttention(reason string) error {
	if err := r.crash(CrashBeforeTerminal, ""); err != nil {
		return err
	}
	r.event(store.Event{Kind: store.EvNeedsAttention, Error: reason})
	if r.in.Status == store.StatusCompensating && r.in.Failure != "" {
		reason = r.in.Failure + "; then " + reason
	}
	r.in.Status, r.in.Failure = store.StatusNeedsAttention, reason
	return r.release()
}

func (r *run) beginCompensation(reason string) error {
	if err := r.crash(CrashBeforeCompensate, ""); err != nil {
		return err
	}
	r.in.Status, r.in.Failure = store.StatusCompensating, reason
	r.in.NextRunAt = time.Time{}
	r.event(store.Event{Kind: store.EvCompensating, Error: reason})
	return r.persist()
}

type nodeKind int

const (
	nodeDone nodeKind = iota
	nodeSuspend
	nodeFailed
	nodeAttention
)

type nodeResult struct {
	kind   nodeKind
	wake   time.Time
	reason string
}

func (r *run) forward() (bool, error) {
	in := r.in
	for in.Cursor < len(r.d.nodes) {
		node := r.d.nodes[in.Cursor]
		if !in.PivotPassed && !in.Deadline.IsZero() && !r.now().Before(in.Deadline) && !r.inDoubt(node) {
			return true, r.beginCompensation("saga deadline exceeded")
		}
		res, err := r.runNode(node)
		if err != nil {
			return false, err
		}
		switch res.kind {
		case nodeDone:
			continue
		case nodeSuspend:
			if !res.wake.After(r.now()) {
				continue
			}
			return false, r.suspend(res.wake)
		case nodeAttention:
			return false, r.needsAttention(res.reason)
		case nodeFailed:
			if in.PivotPassed {
				return false, r.needsAttention("past the pivot, forward recovery failed: " + res.reason)
			}
			return true, r.beginCompensation(res.reason)
		}
	}
	if err := r.crash(CrashBeforeTerminal, ""); err != nil {
		return false, err
	}
	in.Status = store.StatusCompleted
	in.NextRunAt = time.Time{}
	r.event(store.Event{Kind: store.EvCompleted})
	return false, r.release()
}

func (r *run) inDoubt(node []int) bool {
	for _, i := range node {
		if r.in.Steps[i].InDoubt {
			return true
		}
	}
	return false
}

type stepResult struct {
	kind   nodeKind
	wake   time.Time
	reason string
}

func (r *run) runNode(node []int) (nodeResult, error) {
	in := r.in
	now := r.now()
	var eligible []int
	for _, i := range node {
		rec := &in.Steps[i]
		switch rec.Status {
		case store.StepDone, store.StepSkipped, store.StepFailed:
			continue
		case store.StepRetry, store.StepWaiting:
			_, signalled := in.Signals[rec.Waiting]
			if rec.NextAttemptAt.After(now) && !(rec.Waiting != "" && signalled) {
				continue
			}
		}
		eligible = append(eligible, i)
	}
	results := make([]stepResult, len(eligible))
	errs := make([]error, len(eligible))
	single := len(node) == 1
	if len(eligible) <= 1 || r.e.serialGroups {
		for k, i := range eligible {
			results[k], errs[k] = r.attempt(i, single)
			if errs[k] != nil {
				return nodeResult{}, errs[k]
			}
		}
	} else {
		var wg sync.WaitGroup
		for k, i := range eligible {
			wg.Add(1)
			go func(k, i int) {
				defer wg.Done()
				results[k], errs[k] = r.attempt(i, false)
			}(k, i)
		}
		wg.Wait()
		if err := errors.Join(errs...); err != nil {
			return nodeResult{}, err
		}
	}

	// Aggregate over the whole node, not only what ran in this pass.
	var wake time.Time
	var failed *store.StepRecord
	pending := false
	for _, i := range node {
		rec := &in.Steps[i]
		switch rec.Status {
		case store.StepDone, store.StepSkipped:
		case store.StepFailed:
			if rec.InDoubt {
				return nodeResult{kind: nodeAttention, reason: fmt.Sprintf("step %q: %s", rec.Name, rec.LastError)}, nil
			}
			if failed == nil {
				failed = rec
			}
		default:
			pending = true
			if wake.IsZero() || rec.NextAttemptAt.Before(wake) {
				wake = rec.NextAttemptAt
			}
		}
	}
	if failed != nil {
		if pending && r.inDoubt(node) {
			// A sibling's outcome is unknown; it decides what has to be
			// compensated, so settle it first.
			return nodeResult{kind: nodeSuspend, wake: wake}, nil
		}
		return nodeResult{kind: nodeFailed, reason: fmt.Sprintf("step %q failed: %s", failed.Name, failed.LastError)}, nil
	}
	if pending {
		return nodeResult{kind: nodeSuspend, wake: wake}, nil
	}
	if !single {
		if err := r.crash(CrashBeforeMerge, ""); err != nil {
			return nodeResult{}, err
		}
		merged, err := mergeBranches(in.State, node, in.Steps)
		if err != nil {
			return nodeResult{kind: nodeFailed, reason: err.Error()}, nil
		}
		in.State = merged
		for _, i := range node {
			in.Steps[i].Output = nil
		}
		in.Cursor++
		if err := r.persist(); err != nil {
			return nodeResult{}, err
		}
	}
	return nodeResult{kind: nodeDone}, nil
}

// attempt runs one attempt of step i. single means the step is a node of
// its own and its success advances the cursor in the same write.
func (r *run) attempt(i int, single bool) (stepResult, error) {
	st := r.d.steps[i]
	r.mu.Lock()
	in := r.in
	rec := &in.Steps[i]
	now := r.now()

	if st.when != nil && rec.Attempts == 0 && rec.Abandoned == 0 && rec.Status == store.StepPending {
		ok, err := st.when(in.State)
		if err != nil {
			defer r.mu.Unlock()
			return r.finalFailure(rec, Permanent(err), "condition")
		}
		if !ok {
			defer r.mu.Unlock()
			rec.Status, rec.FinishedAt = store.StepSkipped, now
			if single {
				in.Cursor++
			}
			r.event(store.Event{Kind: store.EvStepSkipped, Step: st.name, Detail: "condition false"})
			return stepResult{kind: nodeDone}, r.persist()
		}
	}
	if rec.Abandoned > r.e.maxAbandoned {
		defer r.mu.Unlock()
		rec.InDoubt = true
		return r.finalFailure(rec, fmt.Errorf("%d attempts abandoned by crashed or partitioned workers; outcome unknown", rec.Abandoned), "poison")
	}
	if st.deadline > 0 && !rec.InDoubt && !rec.FirstStartedAt.IsZero() && !now.Before(rec.FirstStartedAt.Add(st.deadline)) {
		defer r.mu.Unlock()
		return r.finalFailure(rec, fmt.Errorf("step deadline of %s exceeded: %s", st.deadline, rec.LastError), "deadline")
	}
	if rec.Status == store.StepWaiting {
		if _, ok := in.Signals[rec.Waiting]; ok {
			r.event(store.Event{Kind: store.EvSignal, Step: st.name, Detail: rec.Waiting})
		}
	}
	if err := r.crash(CrashBeforeStepStart, st.name); err != nil {
		r.mu.Unlock()
		return stepResult{}, err
	}
	rec.Attempts++
	rec.Status, rec.StartedAt = store.StepRunning, now
	if rec.FirstStartedAt.IsZero() {
		rec.FirstStartedAt = now
	}
	if rec.StartSeq == 0 {
		in.Seq++
		rec.StartSeq = in.Seq
	}
	attemptNo := rec.Attempts + rec.Abandoned
	r.event(store.Event{Kind: store.EvStepStarted, Step: st.name, Attempt: attemptNo})
	if err := r.persist(); err != nil {
		r.mu.Unlock()
		return stepResult{}, err
	}
	if err := r.crash(CrashBeforeStepInvoke, st.name); err != nil {
		r.mu.Unlock()
		return stepResult{}, err
	}
	input := in.State
	signals := in.Signals
	r.mu.Unlock()

	out, err, dur := r.invoke(st, false, attemptNo, input, signals)

	r.mu.Lock()
	defer r.mu.Unlock()
	if cerr := r.crash(CrashAfterStepInvoke, st.name); cerr != nil {
		return stepResult{}, cerr
	}
	if err != nil && r.ctx.Err() != nil && !IsPermanent(err) {
		// Shutdown or lease loss: leave the attempt RUNNING. Whoever runs
		// the saga next treats it as abandoned and re-runs it.
		return stepResult{}, r.ctx.Err()
	}
	now = r.now()
	rec.FinishedAt = now
	rec.InDoubt = false
	if err == nil {
		rec.Status, rec.LastError, rec.NextAttemptAt, rec.Waiting = store.StepDone, "", time.Time{}, ""
		if single {
			in.State = out
			in.Cursor++
		} else {
			rec.Output = out
		}
		r.event(store.Event{Kind: store.EvStepSucceeded, Step: st.name, Attempt: attemptNo, Duration: dur})
		if st.pivot {
			in.PivotPassed = true
			r.event(store.Event{Kind: store.EvPivot, Step: st.name, Detail: "point of no return: forward recovery only"})
		}
		if err := r.persist(); err != nil {
			return stepResult{}, err
		}
		if err := r.crash(CrashAfterStepPersist, st.name); err != nil {
			return stepResult{}, err
		}
		return stepResult{kind: nodeDone}, nil
	}
	if name, ok := waitingFor(err); ok {
		rec.Attempts--
		rec.Status, rec.Waiting = store.StepWaiting, name
		rec.NextAttemptAt = r.capToStepDeadline(st, rec, now.Add(r.e.waitRecheck))
		r.event(store.Event{Kind: store.EvStepWaiting, Step: st.name, Attempt: attemptNo, Detail: "awaiting signal " + name})
		if err := r.persist(); err != nil {
			return stepResult{}, err
		}
		return stepResult{kind: nodeSuspend, wake: rec.NextAttemptAt}, nil
	}
	rec.LastError = err.Error()
	if IsPermanent(err) || rec.Attempts >= st.retry.MaxAttempts {
		why := "retries exhausted"
		if IsPermanent(err) {
			why = "permanent error"
		}
		return r.finalFailure(rec, err, why)
	}
	backoff := st.retry.Backoff(rec.Attempts, r.e.random)
	rec.Status = store.StepRetry
	rec.NextAttemptAt = r.capToStepDeadline(st, rec, now.Add(backoff))
	r.event(store.Event{Kind: store.EvStepRetry, Step: st.name, Attempt: attemptNo, Duration: dur, Error: err.Error(),
		Detail: "next attempt in " + backoff.Round(time.Millisecond).String()})
	if err := r.persist(); err != nil {
		return stepResult{}, err
	}
	if err := r.crash(CrashAfterStepPersist, st.name); err != nil {
		return stepResult{}, err
	}
	return stepResult{kind: nodeSuspend, wake: rec.NextAttemptAt}, nil
}

func (r *run) capToStepDeadline(st *erasedStep, rec *store.StepRecord, t time.Time) time.Time {
	if st.deadline > 0 && !rec.FirstStartedAt.IsZero() {
		if dl := rec.FirstStartedAt.Add(st.deadline); t.After(dl) {
			return dl
		}
	}
	return t
}

func (r *run) finalFailure(rec *store.StepRecord, err error, why string) (stepResult, error) {
	rec.Status, rec.LastError, rec.FinishedAt = store.StepFailed, err.Error(), r.now()
	r.event(store.Event{Kind: store.EvStepFailed, Step: rec.Name, Attempt: rec.Attempts + rec.Abandoned, Error: err.Error(), Detail: why})
	if perr := r.persist(); perr != nil {
		return stepResult{}, perr
	}
	if cerr := r.crash(CrashAfterStepPersist, rec.Name); cerr != nil {
		return stepResult{}, cerr
	}
	return stepResult{kind: nodeFailed}, nil
}

// invoke calls a step or compensation with its own timeout, converting
// panics into errors.
func (r *run) invoke(st *erasedStep, comp bool, attempt int, input json.RawMessage, signals map[string]json.RawMessage) (out json.RawMessage, err error, dur time.Duration) {
	ctx := r.ctx
	if st.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, st.timeout)
		defer cancel()
	}
	sc := &stepCtx{Context: ctx, id: r.in.ID, typ: r.in.Type, step: st.name, attempt: attempt, comp: comp, signals: signals, base: r.e.log}
	fn := st.run
	if comp {
		fn = st.comp
	}
	start := r.now()
	func() {
		defer func() {
			if p := recover(); p != nil {
				err = fmt.Errorf("panic: %v", p)
			}
		}()
		out, err = fn(sc, input)
	}()
	end := r.now()
	if err != nil && errors.Is(ctx.Err(), context.DeadlineExceeded) && r.ctx.Err() == nil && !IsPermanent(err) {
		err = fmt.Errorf("attempt timed out after %s: %w", st.timeout, err)
	}
	for _, o := range r.e.invObs {
		o(Invocation{SagaID: sc.id, Type: sc.typ, Step: st.name, Key: sc.IdempotencyKey(), Compensation: comp,
			Attempt: attempt, Start: start, End: end, Err: err})
	}
	return out, err, end.Sub(start)
}

// compCandidates returns the steps still to compensate, most recently
// started first.
func (r *run) compCandidates() []int {
	var out []int
	for i, st := range r.d.steps {
		rec := &r.in.Steps[i]
		if st.comp == nil || rec.Comp == store.CompDone {
			continue
		}
		if rec.Status == store.StepDone || (st.compAttempted && rec.Attempts+rec.Abandoned > 0) {
			out = append(out, i)
		}
	}
	sort.Slice(out, func(a, b int) bool { return r.in.Steps[out[a]].StartSeq > r.in.Steps[out[b]].StartSeq })
	return out
}

func (r *run) compensate() (bool, error) {
	in := r.in
	for {
		cands := r.compCandidates()
		if len(cands) == 0 {
			if err := r.crash(CrashBeforeTerminal, ""); err != nil {
				return false, err
			}
			in.Status, in.NextRunAt = store.StatusCompensated, time.Time{}
			r.event(store.Event{Kind: store.EvCompensated, Detail: in.Failure})
			return false, r.release()
		}
		i := cands[0]
		st, rec := r.d.steps[i], &in.Steps[i]
		now := r.now()
		if rec.Comp == store.CompRetry && rec.CompNextAt.After(now) {
			return false, r.suspend(rec.CompNextAt)
		}
		if rec.CompAbandoned > r.e.maxAbandoned {
			rec.Comp, rec.CompError = store.CompFailed, fmt.Sprintf("%d compensation attempts abandoned", rec.CompAbandoned)
			r.event(store.Event{Kind: store.EvCompFailed, Step: st.name, Error: rec.CompError, Detail: "poison"})
			return false, r.needsAttention(fmt.Sprintf("compensation of %q failed: %s", st.name, rec.CompError))
		}
		if err := r.crash(CrashBeforeCompStart, st.name); err != nil {
			return false, err
		}
		rec.CompAttempts++
		rec.Comp = store.CompRunning
		attemptNo := rec.CompAttempts + rec.CompAbandoned
		r.event(store.Event{Kind: store.EvCompStarted, Step: st.name, Attempt: attemptNo})
		if err := r.persist(); err != nil {
			return false, err
		}
		if err := r.crash(CrashBeforeCompInvoke, st.name); err != nil {
			return false, err
		}
		out, err, dur := r.invoke(st, true, attemptNo, in.State, in.Signals)
		if cerr := r.crash(CrashAfterCompInvoke, st.name); cerr != nil {
			return false, cerr
		}
		if err != nil && r.ctx.Err() != nil && !IsPermanent(err) {
			return false, r.ctx.Err()
		}
		now = r.now()
		if err == nil {
			rec.Comp, rec.CompError, rec.CompNextAt = store.CompDone, "", time.Time{}
			in.State = out
			r.event(store.Event{Kind: store.EvCompSucceeded, Step: st.name, Attempt: attemptNo, Duration: dur})
			if err := r.persist(); err != nil {
				return false, err
			}
			if err := r.crash(CrashAfterCompPersist, st.name); err != nil {
				return false, err
			}
			continue
		}
		rec.CompError = err.Error()
		if IsPermanent(err) || rec.CompAttempts >= st.compRetry.MaxAttempts {
			rec.Comp = store.CompFailed
			r.event(store.Event{Kind: store.EvCompFailed, Step: st.name, Attempt: attemptNo, Duration: dur, Error: err.Error()})
			return false, r.needsAttention(fmt.Sprintf("compensation of %q failed: %s", st.name, err))
		}
		backoff := st.compRetry.Backoff(rec.CompAttempts, r.e.random)
		rec.Comp, rec.CompNextAt = store.CompRetry, now.Add(backoff)
		r.event(store.Event{Kind: store.EvCompRetry, Step: st.name, Attempt: attemptNo, Duration: dur, Error: err.Error(),
			Detail: "next attempt in " + backoff.Round(time.Millisecond).String()})
		if err := r.persist(); err != nil {
			return false, err
		}
		if err := r.crash(CrashAfterCompPersist, st.name); err != nil {
			return false, err
		}
	}
}
