package saga_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/victorcondackdev/saga-go/saga"
	"github.com/victorcondackdev/saga-go/store"
)

func noop(sc saga.Context, o *order) error { return nil }

func benchDef() *saga.Definition[order] {
	return saga.Define[order]("bench").
		Step("reserve", noop, saga.Compensate(noop)).
		Step("send", noop, saga.Compensate(noop)).
		Step("confirm", noop, saga.Pivot()).
		Step("notify", noop).
		MustBuild()
}

// runBench pushes b.N four-step sagas through Run with at most inflight
// outstanding at a time and reports sagas per second.
func runBench(b *testing.B, st store.Store, workers, inflight int) {
	def := benchDef()
	slots := make(chan struct{}, inflight)
	e := saga.New(st, saga.WithLogger(saga.DiscardLogger()), saga.WithConcurrency(workers), saga.WithPollInterval(time.Millisecond),
		saga.WithHook(func(h saga.HookEvent) {
			if h.Event.Kind == store.EvCompleted {
				<-slots
			}
		}))
	e.Register(def)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { e.Run(ctx); close(done) }()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		slots <- struct{}{}
		if _, _, err := e.Start(ctx, "bench", fmt.Sprint(i), []byte(`{}`)); err != nil {
			b.Fatal(err)
		}
	}
	for i := 0; i < inflight; i++ {
		slots <- struct{}{}
	}
	b.StopTimer()
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "sagas/s")
	cancel()
	<-done
}

func BenchmarkFourStepSaga(b *testing.B) {
	b.Run("memory", func(b *testing.B) { runBench(b, store.NewMemory(), 16, 256) })
	for _, p := range []store.SyncPolicy{store.SyncNever, store.SyncAlways} {
		b.Run("wal-sync="+p.String(), func(b *testing.B) {
			w, err := store.OpenWAL(store.WALOptions{Dir: b.TempDir(), Sync: p})
			if err != nil {
				b.Fatal(err)
			}
			defer w.Close()
			runBench(b, w, 16, 256)
		})
	}
}

// BenchmarkTickSerial measures the engine's own overhead per saga with no
// concurrency: start one saga and drive it with Tick.
func BenchmarkTickSerial(b *testing.B) {
	e := saga.New(store.NewMemory(), saga.WithLogger(saga.DiscardLogger()))
	def := benchDef()
	e.Register(def)
	ctx := context.Background()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		e.Start(ctx, "bench", fmt.Sprint(i), []byte(`{}`))
		if n, err := e.Tick(ctx); n != 1 || err != nil {
			b.Fatal(n, err)
		}
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "sagas/s")
}
