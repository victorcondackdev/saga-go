package saga

import (
	"context"
	"io"
	"sort"

	"github.com/victorcondackdev/saga-go/internal/metrics"
	"github.com/victorcondackdev/saga-go/store"
)

type engineMetrics struct {
	reg           *metrics.Registry
	started       *metrics.Counter
	finished      *metrics.Counter
	attempts      *metrics.Counter
	retries       *metrics.Counter
	compensations *metrics.Counter
	takeovers     *metrics.Counter
	leaseLost     *metrics.Counter
	duration      *metrics.Histogram
	instances     *metrics.Gauge
}

func newEngineMetrics() *engineMetrics {
	r := &metrics.Registry{}
	return &engineMetrics{
		reg:           r,
		instances:     r.Gauge("saga_instances", "Saga instances in the store by status.", "status"),
		started:       r.Counter("saga_started_total", "Sagas started.", "type"),
		finished:      r.Counter("saga_finished_total", "Sagas that reached a resting state.", "type", "status"),
		attempts:      r.Counter("saga_step_attempts_total", "Step attempts by outcome (ok, retry, failed, abandoned).", "type", "step", "result"),
		retries:       r.Counter("saga_step_retries_total", "Step attempts that failed and were rescheduled.", "type", "step"),
		compensations: r.Counter("saga_compensations_total", "Compensation attempts by outcome.", "type", "step", "result"),
		takeovers:     r.Counter("saga_lease_takeovers_total", "Instances taken over from an expired lease.", "type"),
		leaseLost:     r.Counter("saga_lease_lost_total", "Instances this engine stopped driving because its lease was taken.", "type"),
		duration:      r.Histogram("saga_step_duration_seconds", "Duration of step and compensation invocations.", metrics.DefBuckets, "type", "step", "phase"),
	}
}

func (m *engineMetrics) observe(typ string, ev store.Event) {
	switch ev.Kind {
	case store.EvStepSucceeded:
		m.attempts.Inc(typ, ev.Step, "ok")
		m.duration.Observe(ev.Duration.Seconds(), typ, ev.Step, "forward")
	case store.EvStepRetry:
		m.attempts.Inc(typ, ev.Step, "retry")
		m.retries.Inc(typ, ev.Step)
		m.duration.Observe(ev.Duration.Seconds(), typ, ev.Step, "forward")
	case store.EvStepFailed:
		m.attempts.Inc(typ, ev.Step, "failed")
	case store.EvStepAbandoned:
		m.attempts.Inc(typ, ev.Step, "abandoned")
	case store.EvCompSucceeded:
		m.compensations.Inc(typ, ev.Step, "ok")
		m.duration.Observe(ev.Duration.Seconds(), typ, ev.Step, "compensation")
	case store.EvCompRetry:
		m.compensations.Inc(typ, ev.Step, "retry")
	case store.EvCompFailed:
		m.compensations.Inc(typ, ev.Step, "failed")
	case store.EvCompleted:
		m.finished.Inc(typ, string(store.StatusCompleted))
	case store.EvCompensated:
		m.finished.Inc(typ, string(store.StatusCompensated))
	case store.EvNeedsAttention:
		m.finished.Inc(typ, string(store.StatusNeedsAttention))
	case store.EvLeaseTakeover:
		m.takeovers.Inc(typ)
	}
}

// WritePrometheus writes the engine's metrics, plus a gauge of instances by
// status read from the store, in the Prometheus text format.
func (e *Engine) WritePrometheus(ctx context.Context, w io.Writer) error {
	stats, err := e.st.Stats(ctx)
	if err != nil {
		return err
	}
	e.m.instances.Reset()
	keys := make([]string, 0, len(stats))
	for s := range stats {
		keys = append(keys, string(s))
	}
	sort.Strings(keys)
	for _, k := range keys {
		e.m.instances.Set(float64(stats[store.Status(k)]), k)
	}
	return e.m.reg.Write(w)
}
