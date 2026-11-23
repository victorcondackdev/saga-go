package saga

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
)

// Context is what a step or compensation receives. It is a
// context.Context (cancelled on the attempt's Timeout, on lease loss and on
// shutdown) plus the identity of the attempt.
type Context interface {
	context.Context
	// SagaID is the instance id.
	SagaID() string
	// SagaType is the definition name.
	SagaType() string
	// Step is the step name.
	Step() string
	// Attempt is the 1-based number of this invocation of the step (or of
	// its compensation), counting attempts abandoned by crashed workers.
	Attempt() int
	// Compensating reports whether this is a compensation.
	Compensating() bool
	// IdempotencyKey is stable for a (saga, step) pair across attempts,
	// retries, crashes and engine processes, and differs between a step
	// and its compensation. Pass it to downstream systems so that repeated
	// deliveries are deduplicated there: that is what turns the engine's
	// at-least-once execution into an effectively-once effect.
	IdempotencyKey() string
	// Await decodes the payload of the named signal into v and returns nil
	// if it has been delivered (see Engine.Signal). Otherwise it returns an
	// error that the step must return as is: the step is parked without
	// consuming an attempt and re-run when the signal arrives.
	Await(name string, v any) error
	// Logger returns a logger annotated with the saga id, type, step and
	// attempt.
	Logger() *slog.Logger
}

type stepCtx struct {
	context.Context
	id, typ, step string
	attempt       int
	comp          bool
	signals       map[string]json.RawMessage
	base          *slog.Logger
	log           *slog.Logger
}

func (c *stepCtx) SagaID() string     { return c.id }
func (c *stepCtx) SagaType() string   { return c.typ }
func (c *stepCtx) Step() string       { return c.step }
func (c *stepCtx) Attempt() int       { return c.attempt }
func (c *stepCtx) Compensating() bool { return c.comp }
func (c *stepCtx) Logger() *slog.Logger {
	if c.log == nil {
		c.log = c.base.With("saga_id", c.id, "saga_type", c.typ, "step", c.step, "attempt", c.attempt, "compensation", c.comp)
	}
	return c.log
}

func (c *stepCtx) IdempotencyKey() string { return IdempotencyKey(c.id, c.step, c.comp) }

func (c *stepCtx) Await(name string, v any) error {
	p, ok := c.signals[name]
	if !ok {
		return &waitError{name: name}
	}
	if v == nil {
		return nil
	}
	if err := json.Unmarshal(p, v); err != nil {
		return Permanent(fmt.Errorf("decode signal %q: %w", name, err))
	}
	return nil
}

// IdempotencyKey returns the key a step (or, with compensation set, its
// compensation) of saga id sees in Context.IdempotencyKey.
func IdempotencyKey(sagaID, step string, compensation bool) string {
	if compensation {
		return sagaID + ":" + step + ":compensate"
	}
	return sagaID + ":" + step
}
