// Package store defines the durable model of a saga instance and the Store
// interface the engine persists it through.
//
// The engine never keeps authoritative state in memory: every transition is
// written through a Store before (write-ahead) and after a step runs, so any
// process holding a valid lease can resume an instance from what the store
// returns.
//
// # Concurrency model
//
// Instances are claimed with a lease (Owner, LeaseUntil). Acquire hands out
// runnable instances whose lease is free or expired and bumps Version, which
// acts as a fencing token: an engine whose lease was taken over still holds
// the old Version, so its next Update fails with ErrConflict and it stops
// before doing more work. Heartbeat extends a lease without changing Version.
//
// Two implementations ship with the module: Memory (tests, embedding,
// ephemeral workloads) and WAL (a single-process, file-backed store built on
// an append-only, checksummed, segmented log). migrations/postgres.sql shows
// how the same contract maps onto PostgreSQL for multi-process deployments.
package store

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

// Status is the lifecycle state of a saga instance.
type Status string

// Saga statuses. COMPLETED, COMPENSATED and FAILED_NEEDS_ATTENTION are the
// resting states; only the last one can be moved again, by an operator.
const (
	StatusPending        Status = "PENDING"
	StatusRunning        Status = "RUNNING"
	StatusCompleted      Status = "COMPLETED"
	StatusCompensating   Status = "COMPENSATING"
	StatusCompensated    Status = "COMPENSATED"
	StatusNeedsAttention Status = "FAILED_NEEDS_ATTENTION"
)

// Runnable reports whether the engine should pick up an instance in s.
func (s Status) Runnable() bool {
	return s == StatusPending || s == StatusRunning || s == StatusCompensating
}

// Terminal reports whether s is COMPLETED or COMPENSATED.
func (s Status) Terminal() bool {
	return s == StatusCompleted || s == StatusCompensated
}

// StepStatus is the forward state of one step.
type StepStatus string

// Forward step statuses.
const (
	StepPending StepStatus = "PENDING"
	StepRunning StepStatus = "RUNNING" // attempt started (write-ahead), outcome not yet recorded
	StepRetry   StepStatus = "RETRY"   // last attempt failed, next attempt scheduled at NextAttemptAt
	StepWaiting StepStatus = "WAITING" // waiting for the signal named in Waiting
	StepDone    StepStatus = "DONE"
	StepFailed  StepStatus = "FAILED"
	StepSkipped StepStatus = "SKIPPED"
)

// CompStatus is the state of a step's compensation.
type CompStatus string

// Compensation statuses. The zero value means the compensation has not
// started.
const (
	CompNone    CompStatus = ""
	CompRunning CompStatus = "RUNNING"
	CompRetry   CompStatus = "RETRY"
	CompDone    CompStatus = "DONE"
	CompFailed  CompStatus = "FAILED"
)

// StepRecord is the persisted progress of one step of an instance.
type StepRecord struct {
	Name   string     `json:"name"`
	Status StepStatus `json:"status"`
	// Attempts counts attempts whose outcome was observed. Abandoned counts
	// attempts that were started but never reported back (the worker died or
	// lost its lease); they are re-run to learn their outcome and do not
	// consume the retry budget.
	Attempts  int `json:"attempts,omitempty"`
	Abandoned int `json:"abandoned,omitempty"`
	// InDoubt is set while the latest attempt was abandoned: the step may
	// or may not have taken effect, so it must be re-run (idempotently)
	// before the saga may decide anything that depends on its outcome.
	InDoubt        bool            `json:"in_doubt,omitempty"`
	StartSeq       int             `json:"start_seq,omitempty"` // order of first attempt across the instance
	FirstStartedAt time.Time       `json:"first_started_at,omitempty"`
	StartedAt      time.Time       `json:"started_at,omitempty"`
	FinishedAt     time.Time       `json:"finished_at,omitempty"`
	NextAttemptAt  time.Time       `json:"next_attempt_at,omitempty"`
	Waiting        string          `json:"waiting,omitempty"`
	LastError      string          `json:"last_error,omitempty"`
	Output         json.RawMessage `json:"output,omitempty"` // branch state of a parallel step

	Comp          CompStatus `json:"comp,omitempty"`
	CompAttempts  int        `json:"comp_attempts,omitempty"`
	CompAbandoned int        `json:"comp_abandoned,omitempty"`
	CompNextAt    time.Time  `json:"comp_next_at,omitempty"`
	CompError     string     `json:"comp_error,omitempty"`
}

// Event kinds recorded in an instance's history.
const (
	EvCreated          = "saga_created"
	EvStarted          = "saga_started"
	EvStepStarted      = "step_started"
	EvStepSucceeded    = "step_succeeded"
	EvStepFailed       = "step_failed"
	EvStepRetry        = "step_retry_scheduled"
	EvStepWaiting      = "step_waiting"
	EvStepSkipped      = "step_skipped"
	EvStepAbandoned    = "step_attempt_abandoned"
	EvCompensating     = "saga_compensating"
	EvCompStarted      = "compensation_started"
	EvCompSucceeded    = "compensation_succeeded"
	EvCompFailed       = "compensation_failed"
	EvCompRetry        = "compensation_retry_scheduled"
	EvCompAbandoned    = "compensation_attempt_abandoned"
	EvPivot            = "pivot_passed"
	EvCompleted        = "saga_completed"
	EvCompensated      = "saga_compensated"
	EvNeedsAttention   = "saga_needs_attention"
	EvSignal           = "signal_received"
	EvManualRetry      = "manual_retry"
	EvManualResolution = "manual_resolution"
	EvLeaseTakeover    = "lease_takeover"
)

// Event is one entry of an instance's execution history.
type Event struct {
	Seq      int           `json:"seq"`
	At       time.Time     `json:"at"`
	Kind     string        `json:"kind"`
	Step     string        `json:"step,omitempty"`
	Attempt  int           `json:"attempt,omitempty"`
	Duration time.Duration `json:"duration,omitempty"`
	Error    string        `json:"error,omitempty"`
	Detail   string        `json:"detail,omitempty"`
	Owner    string        `json:"owner,omitempty"`
}

// Instance is the durable record of one saga execution.
type Instance struct {
	ID        string `json:"id"`
	Type      string `json:"type"`
	Key       string `json:"key"`        // client idempotency key, unique per Type
	InputHash string `json:"input_hash"` // detects a Key reused with a different input
	DefHash   string `json:"def_hash"`   // shape of the definition that created it

	Status      Status          `json:"status"`
	State       json.RawMessage `json:"state"`
	Cursor      int             `json:"cursor"` // index of the current forward node
	PivotPassed bool            `json:"pivot_passed,omitempty"`
	Failure     string          `json:"failure,omitempty"`
	Steps       []StepRecord    `json:"steps"`
	Seq         int             `json:"seq,omitempty"` // allocator for StartSeq

	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
	Deadline  time.Time `json:"deadline,omitempty"`
	NextRunAt time.Time `json:"next_run_at,omitempty"`

	Owner string `json:"owner,omitempty"`
	// PreviousOwner is set by Acquire (never persisted) when the instance
	// was taken over from an expired lease held by someone else.
	PreviousOwner string    `json:"-"`
	LeaseUntil    time.Time `json:"lease_until,omitempty"`
	Version       int64     `json:"version"`

	// Signals and SignalSeq are owned by the store: Signal writes them and
	// Update preserves the stored values, so a signal delivered while an
	// engine holds the lease is never lost.
	Signals   map[string]json.RawMessage `json:"signals,omitempty"`
	SignalSeq int64                      `json:"signal_seq,omitempty"`

	History []Event `json:"history"`
}

// Clone returns a deep copy of in.
func (in *Instance) Clone() *Instance {
	if in == nil {
		return nil
	}
	c := *in
	c.State = cloneRaw(in.State)
	c.Steps = make([]StepRecord, len(in.Steps))
	for i, s := range in.Steps {
		s.Output = cloneRaw(s.Output)
		c.Steps[i] = s
	}
	c.Signals = cloneSignals(in.Signals)
	c.History = append([]Event(nil), in.History...)
	return &c
}

func cloneSignals(m map[string]json.RawMessage) map[string]json.RawMessage {
	if m == nil {
		return nil
	}
	c := make(map[string]json.RawMessage, len(m))
	for k, v := range m {
		c[k] = cloneRaw(v)
	}
	return c
}

func cloneRaw(b json.RawMessage) json.RawMessage {
	if b == nil {
		return nil
	}
	return append(json.RawMessage(nil), b...)
}

// Filter selects instances in List. Zero fields match everything.
type Filter struct {
	Type          string
	Statuses      []Status
	UpdatedBefore time.Time
	Limit         int
}

func (f Filter) match(in *Instance) bool {
	if f.Type != "" && in.Type != f.Type {
		return false
	}
	if !f.UpdatedBefore.IsZero() && !in.UpdatedAt.Before(f.UpdatedBefore) {
		return false
	}
	if len(f.Statuses) == 0 {
		return true
	}
	for _, s := range f.Statuses {
		if in.Status == s {
			return true
		}
	}
	return false
}

// Errors returned by stores.
var (
	ErrNotFound = errors.New("store: instance not found")
	// ErrConflict means the caller's Version or lease is stale: another
	// writer got there first. The caller must stop working on the instance.
	ErrConflict = errors.New("store: version or lease conflict")
	ErrClosed   = errors.New("store: closed")
)

// Store persists saga instances. Implementations must be safe for concurrent
// use and must return copies: callers may mutate what they receive.
type Store interface {
	// Create inserts in unless an instance with the same (Type, Key)
	// exists, in which case it returns the existing one and created=false.
	Create(ctx context.Context, in *Instance) (out *Instance, created bool, err error)
	// Get returns the instance with the given id or ErrNotFound.
	Get(ctx context.Context, id string) (*Instance, error)
	// Update replaces the stored instance. It succeeds only if the stored
	// Version equals in.Version and the stored Owner equals owner ("" for an
	// unleased instance). On success in.Version is incremented to match the
	// store. Signals and SignalSeq are preserved from the stored copy; if a
	// signal arrived after in was read, NextRunAt is cleared so the instance
	// runs again immediately.
	Update(ctx context.Context, in *Instance, owner string) error
	// Acquire leases up to limit runnable instances (status PENDING, RUNNING
	// or COMPENSATING) that are due (NextRunAt <= now) and unleased or whose
	// lease expired, ordered by NextRunAt. Each returned instance has Owner,
	// LeaseUntil and a bumped Version.
	Acquire(ctx context.Context, owner string, now time.Time, lease time.Duration, limit int) ([]*Instance, error)
	// Heartbeat extends the lease of id held by owner until the given time.
	// It returns ErrConflict if owner no longer holds the lease.
	Heartbeat(ctx context.Context, id, owner string, until time.Time) error
	// Release drops owner's lease on id without changing anything else.
	Release(ctx context.Context, id, owner string) error
	// Signal records a named payload for id and makes it due now.
	Signal(ctx context.Context, id, name string, payload json.RawMessage) error
	// List returns instances matching f ordered by CreatedAt then ID.
	List(ctx context.Context, f Filter) ([]*Instance, error)
	// Stats counts instances by status.
	Stats(ctx context.Context) (map[Status]int, error)
	// Close releases resources.
	Close() error
}
