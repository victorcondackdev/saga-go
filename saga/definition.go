package saga

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// StepFunc is the signature of a step or compensation over state S. It must
// be idempotent with respect to sc.IdempotencyKey(): the engine guarantees
// at-least-once execution, so the same key can be seen more than once. State
// mutations made by an attempt are persisted only if it returns nil.
type StepFunc[S any] func(sc Context, s *S) error

// StepOption configures one step. Options that carry a function
// (Compensate, When) are typed by the state; using one built for a different
// state type is reported by Build.
type StepOption interface{ apply(*stepSpec) }

type optFunc func(*stepSpec)

func (f optFunc) apply(s *stepSpec) { f(s) }

type stepSpec struct {
	name          string
	run           any
	comp          any
	when          any
	retry         RetryPolicy
	compRetry     RetryPolicy
	timeout       time.Duration
	deadline      time.Duration
	pivot         bool
	compAttempted bool
}

// Compensate sets the step's compensation: the semantic undo that runs, in
// reverse order, if a later step fails before the pivot. Compensations are
// retried with CompensateRetry and must be idempotent.
func Compensate[S any](fn StepFunc[S]) StepOption {
	return optFunc(func(s *stepSpec) { s.comp = fn })
}

// When makes the step conditional. The predicate is evaluated once, against
// the state the step would receive, and the decision is persisted: a skipped
// step stays skipped after a crash.
func When[S any](pred func(*S) bool) StepOption {
	return optFunc(func(s *stepSpec) { s.when = pred })
}

// Retry sets the retry policy for the step's forward attempts.
func Retry(p RetryPolicy) StepOption {
	return optFunc(func(s *stepSpec) { s.retry = p })
}

// CompensateRetry sets the retry policy for the step's compensation.
func CompensateRetry(p RetryPolicy) StepOption {
	return optFunc(func(s *stepSpec) { s.compRetry = p })
}

// Timeout bounds each attempt (forward or compensation). An attempt that
// exceeds it fails with context.DeadlineExceeded and is retried.
func Timeout(d time.Duration) StepOption {
	return optFunc(func(s *stepSpec) { s.timeout = d })
}

// Deadline bounds the step as a whole, from its first attempt, across
// retries and signal waits. When it passes the step fails for good.
func Deadline(d time.Duration) StepOption {
	return optFunc(func(s *stepSpec) { s.deadline = d })
}

// Pivot marks the step as the saga's point of no return. Once it has
// durably succeeded the saga can only move forward: later steps cannot have
// compensations, the saga timeout no longer triggers compensation, and a
// later step that exhausts its retries moves the saga to
// FAILED_NEEDS_ATTENTION instead of undoing anything. If the pivot itself
// fails, the steps before it are compensated. A saga has at most one pivot
// and it cannot be part of a parallel group.
func Pivot() StepOption {
	return optFunc(func(s *stepSpec) { s.pivot = true })
}

// CompensateAttempted makes the compensation run even if the step never
// succeeded, as long as it was attempted at least once. Use it for steps
// whose failures are ambiguous (a timeout calling a payment provider may
// still have moved money): the compensation must then tolerate "nothing to
// undo".
func CompensateAttempted() StepOption {
	return optFunc(func(s *stepSpec) { s.compAttempted = true })
}

// Branch is one step of a parallel group. Build it with Par.
type Branch[S any] struct{ spec stepSpec }

// Par declares a branch for Builder.Parallel. It takes the same options as
// Builder.Step except Pivot.
func Par[S any](name string, fn StepFunc[S], opts ...StepOption) Branch[S] {
	return Branch[S]{spec: newSpec(name, fn, opts)}
}

func newSpec(name string, fn any, opts []StepOption) stepSpec {
	s := stepSpec{name: name, run: fn}
	for _, o := range opts {
		if o != nil {
			o.apply(&s)
		}
	}
	return s
}

// Builder accumulates a saga definition. Errors are collected and reported
// by Build, so definitions read as a single chained expression.
type Builder[S any] struct {
	name    string
	nodes   [][]stepSpec
	timeout time.Duration
	errs    []error
}

// Define starts a definition for saga type name over state S. S must
// round-trip through encoding/json: it is persisted after every step.
func Define[S any](name string) *Builder[S] {
	return &Builder[S]{name: name}
}

// Step appends a sequential step.
func (b *Builder[S]) Step(name string, fn StepFunc[S], opts ...StepOption) *Builder[S] {
	b.nodes = append(b.nodes, []stepSpec{newSpec(name, fn, opts)})
	return b
}

// Parallel appends a group of branches that run concurrently (fan-out) and
// must all finish before the next step starts (fan-in). Each branch works
// on its own copy of the state; when all are done their changes are merged
// field by field (top-level JSON fields) in declaration order. Two branches
// changing the same field to different values is a definition bug and fails
// the group permanently.
func (b *Builder[S]) Parallel(branches ...Branch[S]) *Builder[S] {
	if len(branches) == 0 {
		b.errs = append(b.errs, errors.New("parallel group without branches"))
		return b
	}
	g := make([]stepSpec, len(branches))
	for i, br := range branches {
		g[i] = br.spec
	}
	b.nodes = append(b.nodes, g)
	return b
}

// Timeout sets the saga deadline, measured from Start. A saga that has not
// reached its pivot (or its end, without a pivot) by then is compensated.
func (b *Builder[S]) Timeout(d time.Duration) *Builder[S] {
	b.timeout = d
	return b
}

// Build validates the definition.
func (b *Builder[S]) Build() (*Definition[S], error) {
	errs := append([]error(nil), b.errs...)
	if b.name == "" {
		errs = append(errs, errors.New("saga type name is empty"))
	}
	if len(b.nodes) == 0 {
		errs = append(errs, errors.New("definition has no steps"))
	}
	d := &definition{name: b.name, timeout: b.timeout, pivot: -1, check: func(raw json.RawMessage) error {
		var s S
		return json.Unmarshal(raw, &s)
	}}
	seen := map[string]bool{}
	var shape strings.Builder
	shape.WriteString(b.name)
	for ni, node := range b.nodes {
		var idx []int
		shape.WriteString("|")
		for _, sp := range node {
			es, err := erase[S](sp)
			if err != nil {
				errs = append(errs, fmt.Errorf("step %q: %w", sp.name, err))
				continue
			}
			if seen[sp.name] {
				errs = append(errs, fmt.Errorf("duplicate step name %q", sp.name))
			}
			seen[sp.name] = true
			if sp.pivot {
				switch {
				case len(node) > 1:
					errs = append(errs, fmt.Errorf("step %q: a pivot cannot be part of a parallel group", sp.name))
				case d.pivot >= 0:
					errs = append(errs, fmt.Errorf("step %q: a saga can have only one pivot", sp.name))
				case es.comp != nil:
					errs = append(errs, fmt.Errorf("step %q: a pivot cannot have a compensation", sp.name))
				}
				d.pivot = ni
			} else if d.pivot >= 0 && es.comp != nil {
				errs = append(errs, fmt.Errorf("step %q: steps after the pivot cannot have compensations (forward recovery only)", sp.name))
			}
			es.node, es.index = ni, len(d.steps)
			idx = append(idx, es.index)
			d.steps = append(d.steps, es)
			shape.WriteString(sp.name + ",")
		}
		d.nodes = append(d.nodes, idx)
	}
	if len(errs) > 0 {
		return nil, fmt.Errorf("saga %q: %w", b.name, errors.Join(errs...))
	}
	sum := sha256.Sum256([]byte(shape.String()))
	d.hash = hex.EncodeToString(sum[:8])
	return &Definition[S]{def: d}, nil
}

// MustBuild is Build that panics on error, for package-level definitions.
func (b *Builder[S]) MustBuild() *Definition[S] {
	d, err := b.Build()
	if err != nil {
		panic(err)
	}
	return d
}

func erase[S any](sp stepSpec) (*erasedStep, error) {
	if sp.name == "" {
		return nil, errors.New("empty step name")
	}
	if strings.ContainsAny(sp.name, ":/ ") {
		return nil, errors.New("step names cannot contain ':', '/' or spaces")
	}
	run, ok := sp.run.(StepFunc[S])
	if !ok || run == nil {
		return nil, errors.New("nil step function")
	}
	es := &erasedStep{
		name:          sp.name,
		run:           adapt(run),
		retry:         sp.retry.withDefaults(DefaultRetry),
		compRetry:     sp.compRetry.withDefaults(DefaultCompensationRetry),
		timeout:       sp.timeout,
		deadline:      sp.deadline,
		pivot:         sp.pivot,
		compAttempted: sp.compAttempted,
	}
	if sp.comp != nil {
		c, ok := sp.comp.(StepFunc[S])
		if !ok || c == nil {
			return nil, fmt.Errorf("compensation is not a StepFunc over %T", *new(S))
		}
		es.comp = adapt(c)
	}
	if sp.compAttempted && es.comp == nil {
		return nil, errors.New("CompensateAttempted without Compensate")
	}
	if sp.when != nil {
		p, ok := sp.when.(func(*S) bool)
		if !ok || p == nil {
			return nil, fmt.Errorf("When predicate is not a func(*%T) bool", *new(S))
		}
		es.when = func(state json.RawMessage) (bool, error) {
			var s S
			if err := json.Unmarshal(state, &s); err != nil {
				return false, err
			}
			return p(&s), nil
		}
	}
	return es, nil
}

// adapt turns a typed step into one over the persisted JSON state.
func adapt[S any](fn StepFunc[S]) rawStep {
	return func(sc Context, state json.RawMessage) (json.RawMessage, error) {
		var s S
		if err := json.Unmarshal(state, &s); err != nil {
			return nil, Permanent(fmt.Errorf("decode state: %w", err))
		}
		if err := fn(sc, &s); err != nil {
			return nil, err
		}
		out, err := json.Marshal(&s)
		if err != nil {
			return nil, Permanent(fmt.Errorf("encode state: %w", err))
		}
		return out, nil
	}
}

type rawStep func(Context, json.RawMessage) (json.RawMessage, error)

type erasedStep struct {
	name          string
	node, index   int
	run, comp     rawStep
	when          func(json.RawMessage) (bool, error)
	retry         RetryPolicy
	compRetry     RetryPolicy
	timeout       time.Duration
	deadline      time.Duration
	pivot         bool
	compAttempted bool
}

type definition struct {
	name    string
	nodes   [][]int
	steps   []*erasedStep
	timeout time.Duration
	pivot   int // node index of the pivot, -1 if none
	hash    string
	check   func(json.RawMessage) error // validates an input against S
}

// Definition is a validated saga definition over state S. Register it with
// an Engine and start instances with Start.
type Definition[S any] struct{ def *definition }

// Name returns the saga type name.
func (d *Definition[S]) Name() string { return d.def.name }

// StepInfo describes one step of a definition.
type StepInfo struct {
	Name                string
	Group               int // index of the sequential position; branches share it
	Compensable         bool
	Pivot               bool
	Conditional         bool
	MaxAttempts         int
	Timeout             time.Duration
	CompensateAttempted bool // compensation also runs after failed attempts
}

// Steps describes the definition, in execution order.
func (d *Definition[S]) Steps() []StepInfo {
	out := make([]StepInfo, len(d.def.steps))
	for i, s := range d.def.steps {
		out[i] = StepInfo{Name: s.name, Group: s.node, Compensable: s.comp != nil, Pivot: s.pivot,
			Conditional: s.when != nil, MaxAttempts: s.retry.MaxAttempts, Timeout: s.timeout, CompensateAttempted: s.compAttempted}
	}
	return out
}

func (d *Definition[S]) definition() *definition { return d.def }

// Registrable is implemented by every *Definition[S]; it lets one Engine run
// definitions over different state types.
type Registrable interface {
	Name() string
	definition() *definition
}
