package saga

import (
	"math"
	"time"
)

// RetryPolicy describes how a failed attempt is retried: exponential backoff
// from Initial, multiplied by Multiplier per attempt and capped at Max, with
// a random jitter of up to Jitter (a fraction, 0..1) subtracted so that many
// instances failing together do not retry in lockstep.
type RetryPolicy struct {
	// MaxAttempts is the total number of observed attempts, including the
	// first. 1 disables retries. Zero means the default (3 for steps, 20
	// for compensations).
	MaxAttempts int
	Initial     time.Duration
	Max         time.Duration
	Multiplier  float64
	Jitter      float64
}

// DefaultRetry is used by steps without a Retry option.
var DefaultRetry = RetryPolicy{MaxAttempts: 3, Initial: 100 * time.Millisecond, Max: 10 * time.Second, Multiplier: 2, Jitter: 0.2}

// DefaultCompensationRetry is used by compensations without a
// CompensateRetry option. Compensations must eventually succeed, so the
// budget is deliberately larger; when it runs out the saga needs an operator.
var DefaultCompensationRetry = RetryPolicy{MaxAttempts: 20, Initial: 200 * time.Millisecond, Max: time.Minute, Multiplier: 2, Jitter: 0.2}

func (p RetryPolicy) withDefaults(d RetryPolicy) RetryPolicy {
	if p.MaxAttempts <= 0 {
		p.MaxAttempts = d.MaxAttempts
	}
	if p.Initial <= 0 {
		p.Initial = d.Initial
	}
	if p.Max <= 0 {
		p.Max = d.Max
	}
	if p.Max < p.Initial {
		p.Max = p.Initial
	}
	if p.Multiplier < 1 {
		p.Multiplier = d.Multiplier
	}
	if p.Jitter < 0 || p.Jitter > 1 {
		p.Jitter = d.Jitter
	}
	return p
}

// Backoff returns the delay before the attempt that follows attempt (1-based)
// failed. r must return a value in [0, 1); pass nil for no jitter.
func (p RetryPolicy) Backoff(attempt int, r func() float64) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	d := float64(p.Initial) * math.Pow(p.Multiplier, float64(attempt-1))
	if d > float64(p.Max) || math.IsInf(d, 0) || math.IsNaN(d) {
		d = float64(p.Max)
	}
	if r != nil && p.Jitter > 0 {
		d -= d * p.Jitter * r()
	}
	if d < 0 {
		d = 0
	}
	return time.Duration(d)
}
