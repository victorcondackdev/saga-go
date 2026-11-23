package store_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/victorcondackdev/saga-go/store"
)

var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func inst(id, key string) *store.Instance {
	return &store.Instance{
		ID: id, Type: "t", Key: key, InputHash: "h", Status: store.StatusPending,
		State: json.RawMessage(`{"n":1}`), CreatedAt: t0, UpdatedAt: t0, NextRunAt: t0,
		Steps:   []store.StepRecord{{Name: "a", Status: store.StepPending, Output: json.RawMessage(`{}`)}},
		History: []store.Event{{Seq: 1, At: t0, Kind: store.EvCreated}},
	}
}

type factory func(t *testing.T) store.Store

func stores() map[string]factory {
	return map[string]factory{
		"memory": func(t *testing.T) store.Store { return store.NewMemory() },
		"wal": func(t *testing.T) store.Store {
			w, err := store.OpenWAL(store.WALOptions{Dir: t.TempDir(), Sync: store.SyncNever})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { w.Close() })
			return w
		},
	}
}

func TestContract(t *testing.T) {
	for name, mk := range stores() {
		t.Run(name, func(t *testing.T) {
			t.Run("create is idempotent on type and key", func(t *testing.T) { testCreate(t, mk(t)) })
			t.Run("update fences on version and owner", func(t *testing.T) { testUpdate(t, mk(t)) })
			t.Run("acquire leases due instances", func(t *testing.T) { testAcquire(t, mk(t)) })
			t.Run("heartbeat and release", func(t *testing.T) { testHeartbeat(t, mk(t)) })
			t.Run("signal survives concurrent update", func(t *testing.T) { testSignal(t, mk(t)) })
			t.Run("list and stats", func(t *testing.T) { testList(t, mk(t)) })
			t.Run("copies are isolated", func(t *testing.T) { testIsolation(t, mk(t)) })
			t.Run("concurrent acquire never double-leases", func(t *testing.T) { testConcurrentAcquire(t, mk(t)) })
			t.Run("closed", func(t *testing.T) { testClosed(t, mk(t)) })
		})
	}
}

func testCreate(t *testing.T, s store.Store) {
	ctx := context.Background()
	in := inst("a", "k1")
	out, created, err := s.Create(ctx, in)
	if err != nil || !created || out.Version != 1 || in.Version != 1 {
		t.Fatalf("create: %v %v %+v", created, err, out)
	}
	dup := inst("b", "k1")
	out, created, err = s.Create(ctx, dup)
	if err != nil || created || out.ID != "a" {
		t.Fatalf("duplicate key: created=%v id=%s err=%v", created, out.ID, err)
	}
	other := inst("a", "k2")
	if _, _, err := s.Create(ctx, other); err == nil {
		t.Fatal("duplicate id accepted")
	}
	if _, err := s.Get(ctx, "nope"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("get missing: %v", err)
	}
}

func testUpdate(t *testing.T, s store.Store) {
	ctx := context.Background()
	s.Create(ctx, inst("a", "k"))
	got, _ := s.Get(ctx, "a")
	got.Status = store.StatusRunning
	if err := s.Update(ctx, got, ""); err != nil || got.Version != 2 {
		t.Fatalf("update: %v v%d", err, got.Version)
	}
	stale, _ := s.Get(ctx, "a")
	stale.Version = 1
	if err := s.Update(ctx, stale, ""); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("stale version: %v", err)
	}
	cur, _ := s.Get(ctx, "a")
	if err := s.Update(ctx, cur, "someone"); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("wrong owner: %v", err)
	}
	missing := inst("zzz", "zzz")
	if err := s.Update(ctx, missing, ""); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("missing: %v", err)
	}
}

func testAcquire(t *testing.T, s store.Store) {
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		in := inst(fmt.Sprint("i", i), fmt.Sprint("k", i))
		in.NextRunAt = t0.Add(time.Duration(i) * time.Second)
		s.Create(ctx, in)
	}
	done := inst("done", "kd")
	done.Status = store.StatusCompleted
	s.Create(ctx, done)

	got, err := s.Acquire(ctx, "e1", t0.Add(2*time.Second), time.Minute, 10)
	if err != nil || len(got) != 3 {
		t.Fatalf("acquire due: %d %v", len(got), err)
	}
	for i, in := range got {
		if in.ID != fmt.Sprint("i", i) || in.Owner != "e1" || in.Version != 2 || in.PreviousOwner != "" {
			t.Fatalf("acquired %+v", in)
		}
	}
	// Leased: nobody else gets them until the lease expires.
	if again, _ := s.Acquire(ctx, "e2", t0.Add(2*time.Second), time.Minute, 10); len(again) != 0 {
		t.Fatalf("double lease: %d", len(again))
	}
	late, _ := s.Acquire(ctx, "e2", t0.Add(2*time.Minute), time.Minute, 2)
	if len(late) != 2 || late[0].PreviousOwner != "e1" {
		t.Fatalf("takeover: %+v", late)
	}
	// The fenced engine's update fails.
	if err := s.Update(ctx, got[0], "e1"); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("fenced update: %v", err)
	}
	if n, _ := s.Acquire(ctx, "", t0, time.Minute, 5); n != nil {
		t.Fatal("empty owner acquired")
	}
}

func testHeartbeat(t *testing.T, s store.Store) {
	ctx := context.Background()
	s.Create(ctx, inst("a", "k"))
	got, _ := s.Acquire(ctx, "e1", t0, time.Second, 1)
	if err := s.Heartbeat(ctx, "a", "e1", t0.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if n, _ := s.Acquire(ctx, "e2", t0.Add(time.Minute), time.Second, 1); len(n) != 0 {
		t.Fatal("heartbeat did not extend the lease")
	}
	if err := s.Heartbeat(ctx, "a", "e2", t0); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("foreign heartbeat: %v", err)
	}
	if err := s.Heartbeat(ctx, "x", "e1", t0); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("missing heartbeat: %v", err)
	}
	if err := s.Release(ctx, "a", "e2"); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("foreign release: %v", err)
	}
	if err := s.Release(ctx, "x", "e2"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("missing release: %v", err)
	}
	if err := s.Release(ctx, "a", "e1"); err != nil {
		t.Fatal(err)
	}
	if err := s.Update(ctx, got[0], "e1"); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("update after release: %v", err)
	}
	if n, _ := s.Acquire(ctx, "e2", t0, time.Second, 1); len(n) != 1 {
		t.Fatal("released instance not acquirable")
	}
}

func testSignal(t *testing.T, s store.Store) {
	ctx := context.Background()
	s.Create(ctx, inst("a", "k"))
	held, _ := s.Acquire(ctx, "e1", t0, time.Minute, 1)
	if err := s.Signal(ctx, "a", "paid", json.RawMessage(`{"ok":true}`)); err != nil {
		t.Fatal(err)
	}
	if err := s.Signal(ctx, "a", "empty", nil); err != nil {
		t.Fatal(err)
	}
	if err := s.Signal(ctx, "x", "paid", nil); !errors.Is(err, store.ErrNotFound) {
		t.Fatal(err)
	}
	in := held[0]
	in.NextRunAt = t0.Add(time.Hour) // engine suspends, unaware of the signal
	in.Owner = ""
	if err := s.Update(ctx, in, "e1"); err != nil {
		t.Fatal(err)
	}
	got, _ := s.Get(ctx, "a")
	if string(got.Signals["paid"]) != `{"ok":true}` || string(got.Signals["empty"]) != "null" || !got.NextRunAt.IsZero() || got.SignalSeq != 2 {
		t.Fatalf("signal lost: %+v", got)
	}
}

func testList(t *testing.T, s store.Store) {
	ctx := context.Background()
	for i, st := range []store.Status{store.StatusRunning, store.StatusCompleted, store.StatusNeedsAttention, store.StatusRunning} {
		in := inst(fmt.Sprint("i", i), fmt.Sprint("k", i))
		in.Status, in.CreatedAt, in.UpdatedAt = st, t0.Add(time.Duration(i)*time.Second), t0.Add(time.Duration(i)*time.Second)
		s.Create(ctx, in)
	}
	other := inst("o", "ko")
	other.Type = "other"
	other.CreatedAt = t0.Add(time.Hour)
	s.Create(ctx, other)
	all, _ := s.List(ctx, store.Filter{})
	if len(all) != 5 || all[0].ID != "i0" || all[4].ID != "o" {
		t.Fatalf("list all: %d", len(all))
	}
	run, _ := s.List(ctx, store.Filter{Type: "t", Statuses: []store.Status{store.StatusRunning}})
	if len(run) != 2 {
		t.Fatalf("running: %d", len(run))
	}
	old, _ := s.List(ctx, store.Filter{UpdatedBefore: t0.Add(2 * time.Second), Limit: 1})
	if len(old) != 1 || old[0].ID != "i0" {
		t.Fatalf("old: %+v", old)
	}
	st, _ := s.Stats(ctx)
	if st[store.StatusRunning] != 2 || st[store.StatusPending] != 1 || st[store.StatusCompleted] != 1 {
		t.Fatalf("stats: %v", st)
	}
}

func testIsolation(t *testing.T, s store.Store) {
	ctx := context.Background()
	in := inst("a", "k")
	s.Create(ctx, in)
	in.State[0] = 'X'
	in.Steps[0].Name = "mutated"
	got, _ := s.Get(ctx, "a")
	got.History[0].Kind = "mutated"
	got.Steps[0].Output[0] = 'X'
	again, _ := s.Get(ctx, "a")
	if string(again.State) != `{"n":1}` || again.Steps[0].Name != "a" || again.History[0].Kind != store.EvCreated || string(again.Steps[0].Output) != "{}" {
		t.Fatalf("store aliased caller memory: %+v", again)
	}
}

func testConcurrentAcquire(t *testing.T, s store.Store) {
	ctx := context.Background()
	for i := 0; i < 50; i++ {
		s.Create(ctx, inst(fmt.Sprint("i", i), fmt.Sprint("k", i)))
	}
	var mu sync.Mutex
	owners := map[string]string{}
	var wg sync.WaitGroup
	for e := 0; e < 8; e++ {
		wg.Add(1)
		go func(e int) {
			defer wg.Done()
			for {
				got, err := s.Acquire(ctx, fmt.Sprint("e", e), t0, time.Hour, 3)
				if err != nil || len(got) == 0 {
					return
				}
				mu.Lock()
				for _, in := range got {
					if prev, ok := owners[in.ID]; ok {
						t.Errorf("%s leased by %s and e%d", in.ID, prev, e)
					}
					owners[in.ID] = in.Owner
				}
				mu.Unlock()
			}
		}(e)
	}
	wg.Wait()
	if len(owners) != 50 {
		t.Fatalf("leased %d of 50", len(owners))
	}
}

func testClosed(t *testing.T, s store.Store) {
	ctx := context.Background()
	s.Create(ctx, inst("a", "k"))
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal("second close:", err)
	}
	checks := []error{
		func() error { _, _, err := s.Create(ctx, inst("b", "b")); return err }(),
		func() error { _, err := s.Get(ctx, "a"); return err }(),
		s.Update(ctx, inst("a", "k"), ""),
		func() error { _, err := s.Acquire(ctx, "e", t0, time.Second, 1); return err }(),
		s.Heartbeat(ctx, "a", "e", t0),
		s.Release(ctx, "a", "e"),
		s.Signal(ctx, "a", "x", nil),
		func() error { _, err := s.List(ctx, store.Filter{}); return err }(),
		func() error { _, err := s.Stats(ctx); return err }(),
	}
	for i, err := range checks {
		if !errors.Is(err, store.ErrClosed) {
			t.Errorf("op %d after close: %v", i, err)
		}
	}
}

func TestStatusPredicates(t *testing.T) {
	tests := []struct {
		s                  store.Status
		runnable, terminal bool
	}{
		{store.StatusPending, true, false},
		{store.StatusRunning, true, false},
		{store.StatusCompensating, true, false},
		{store.StatusCompleted, false, true},
		{store.StatusCompensated, false, true},
		{store.StatusNeedsAttention, false, false},
	}
	for _, tt := range tests {
		if tt.s.Runnable() != tt.runnable || tt.s.Terminal() != tt.terminal {
			t.Errorf("%s: runnable=%v terminal=%v", tt.s, tt.s.Runnable(), tt.s.Terminal())
		}
	}
	var nilInst *store.Instance
	if nilInst.Clone() != nil {
		t.Fatal("nil clone")
	}
}
