package saga

import (
	"errors"
	"fmt"
)

// Errors returned by the engine.
var (
	// ErrUnknownType is returned for a saga type that was never registered.
	ErrUnknownType = errors.New("saga: unknown saga type")
	// ErrKeyConflict is returned by Start when an idempotency key is reused
	// with a different input.
	ErrKeyConflict = errors.New("saga: idempotency key reused with a different input")
	// ErrInvalidInput is returned by Start when the input does not decode
	// into the definition's state type.
	ErrInvalidInput = errors.New("saga: invalid input")
	// ErrInvalidTransition is returned by Retry and Resolve when the
	// instance is not in a state that allows the operation.
	ErrInvalidTransition = errors.New("saga: operation not allowed in the current state")
	// ErrCrashInjected is returned by Tick when a FaultInjector simulated a
	// process crash. The engine stops touching the instance immediately, as
	// a dead process would.
	ErrCrashInjected = errors.New("saga: crash injected")
)

type permanentError struct{ err error }

func (e *permanentError) Error() string { return "permanent: " + e.err.Error() }
func (e *permanentError) Unwrap() error { return e.err }

// Permanent marks err as non-retryable. A step that returns a permanent
// error is not retried: the saga starts compensating immediately (or, past
// the pivot, moves to FAILED_NEEDS_ATTENTION). A compensation that returns a
// permanent error moves the saga to FAILED_NEEDS_ATTENTION.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &permanentError{err: err}
}

// IsPermanent reports whether err was marked with Permanent.
func IsPermanent(err error) bool {
	var p *permanentError
	return errors.As(err, &p)
}

// waitError is returned by Context.Await while the signal is absent.
type waitError struct{ name string }

func (e *waitError) Error() string { return fmt.Sprintf("saga: waiting for signal %q", e.name) }

// IsWaiting reports whether err means a step is waiting for a signal.
func IsWaiting(err error) bool {
	var w *waitError
	return errors.As(err, &w)
}

func waitingFor(err error) (string, bool) {
	var w *waitError
	if errors.As(err, &w) {
		return w.name, true
	}
	return "", false
}
