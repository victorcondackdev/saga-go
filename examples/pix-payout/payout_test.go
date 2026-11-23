package payout_test

import (
	"context"
	"fmt"
	"testing"

	payout "github.com/victorcondackdev/saga-go/examples/pix-payout"
	"github.com/victorcondackdev/saga-go/saga"
	"github.com/victorcondackdev/saga-go/sagatest"
	"github.com/victorcondackdev/saga-go/store"
)

const opening = 1_000_00

type world struct {
	ledger   *payout.Ledger
	psp      *payout.PSP
	notifier *payout.Notifier
}

func newWorld(seed uint64, flaky bool) world {
	w := world{
		ledger:   payout.NewLedger(map[string]int64{"acc-1": opening}),
		psp:      payout.NewPSP(seed),
		notifier: payout.NewNotifier(seed),
	}
	if flaky {
		w.psp.FailRate, w.psp.TimeoutRate, w.psp.RejectRate, w.psp.SettleRejectRate = 0.25, 0.25, 0.05, 0.15
		w.notifier.FailRate = 0.3
	}
	return w
}

func harness(seed uint64, amount int64) sagatest.Harness[payout.Payout] {
	w := newWorld(seed, true)
	def := payout.NewDefinition(payout.Deps{Ledger: w.ledger, PSP: w.psp, Notifier: w.notifier})
	return sagatest.Harness[payout.Payout]{
		Definition: def,
		Input:      payout.Payout{PayoutID: "po-1", Account: "acc-1", PixKey: "maria@example.com", Amount: amount},
		Signal: func(name string, p payout.Payout) (any, bool) {
			if seed%3 == 0 { // webhook lost: polling fallback must cope
				return nil, false
			}
			return w.psp.Settle(p.SendKey)
		},
		Check: func(t testing.TB, o *sagatest.Outcome[payout.Payout]) {
			avail, held := w.ledger.Balance("acc-1")
			if held != 0 {
				t.Errorf("%d cents still on hold", held)
			}
			fx := w.ledger.Effects()
			if fx["hold"] > 1 || fx["capture"] > 1 || fx["release"] > 1 {
				t.Errorf("ledger effects not effectively-once: %v", fx)
			}
			if w.psp.Payments() > 1 {
				t.Errorf("%d Pix payments sent for one payout", w.psp.Payments())
			}
			sendKey := saga.IdempotencyKey(o.Instance.ID, "send-pix", false)
			s, sent := w.psp.Status(sendKey)
			switch o.Instance.Status {
			case store.StatusCompleted:
				if avail != opening-amount || fx["capture"] != 1 {
					t.Errorf("completed payout: balance %d, effects %v", avail, fx)
				}
				if !sent || s.Status != "SETTLED" || w.psp.Refunded(sendKey) {
					t.Errorf("completed payout but PSP shows sent=%v %+v refunded=%v", sent, s, w.psp.Refunded(sendKey))
				}
				if w.notifier.Sent() != 1 {
					t.Errorf("%d notifications", w.notifier.Sent())
				}
			case store.StatusCompensated:
				if avail != opening {
					t.Errorf("compensated payout but balance is %d", avail)
				}
				if sent && s.Status == "SETTLED" && !w.psp.Refunded(sendKey) {
					t.Errorf("compensated payout left a settled Pix unrefunded")
				}
				if fx["capture"] != 0 {
					t.Errorf("compensated payout captured funds")
				}
			case store.StatusNeedsAttention:
				// Only reachable past the pivot: the money moved.
				if !o.Instance.PivotPassed || fx["capture"] != 1 {
					t.Errorf("needs attention before the pivot: %s", o.Instance.Failure)
				}
			}
		},
		AllowNeedsAttention: true,
	}
}

// TestCrashPointsExhaustive crashes the engine at every crash point (and at
// every pair of crash points) of the payout saga, under random PSP
// failures, lost responses and lost webhooks, and checks that money moves
// exactly once or not at all.
func TestCrashPointsExhaustive(t *testing.T) {
	seeds := []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}
	depth := 2
	if testing.Short() {
		seeds, depth = seeds[:4], 1
	}
	rep := sagatest.Explore(t, sagatest.Config{Depth: depth, Seeds: seeds}, func(seed uint64) sagatest.Harness[payout.Payout] {
		return harness(seed, 250_00)
	})
	t.Log(rep)
	if rep.Outcomes[store.StatusCompleted] == 0 || rep.Outcomes[store.StatusCompensated] == 0 {
		t.Errorf("exploration did not cover both outcomes: %v", rep.Outcomes)
	}
}

func TestInsufficientFundsCompensatesNothing(t *testing.T) {
	sagatest.Explore(t, sagatest.Config{}, func(seed uint64) sagatest.Harness[payout.Payout] {
		h := harness(seed, opening+1)
		check := h.Check
		h.Check = func(t testing.TB, o *sagatest.Outcome[payout.Payout]) {
			check(t, o)
			if o.Instance.Status != store.StatusCompensated || o.Invoked("send-pix") != 0 || o.Succeeded("reserve-funds") != 0 {
				t.Errorf("want only failed reserve attempts, got status %s", o.Instance.Status)
			}
		}
		return h
	})
}

func TestOnWALStore(t *testing.T) {
	cfg := sagatest.Config{Seeds: []uint64{2, 3}, NewStore: func(t testing.TB) store.Store {
		w, err := store.OpenWAL(store.WALOptions{Dir: t.TempDir(), Sync: store.SyncNever})
		if err != nil {
			t.Fatal(err)
		}
		return w
	}}
	sagatest.Explore(t, cfg, func(seed uint64) sagatest.Harness[payout.Payout] { return harness(seed, 10_00) })
}

func Example() {
	ctx := context.Background()
	ledger := payout.NewLedger(map[string]int64{"acc-1": 1_000_00})
	psp := payout.NewPSP(1)
	def := payout.NewDefinition(payout.Deps{Ledger: ledger, PSP: psp, Notifier: payout.NewNotifier(1)})

	eng := saga.New(store.NewMemory(), saga.WithLogger(saga.DiscardLogger()))
	if err := eng.Register(def); err != nil {
		panic(err)
	}
	id, err := saga.Start(ctx, eng, def, "payout-42", payout.Payout{
		PayoutID: "42", Account: "acc-1", PixKey: "maria@example.com", Amount: 150_00,
	})
	if err != nil {
		panic(err)
	}
	eng.Tick(ctx) // runs until it waits for the settlement webhook

	in, _ := eng.Get(ctx, id)
	fmt.Println(in.Status, in.Steps[2].Status, in.Steps[2].Waiting)

	// The PSP's webhook arrives.
	s, _ := psp.Settle(saga.IdempotencyKey(id, "send-pix", false))
	_ = eng.Signal(ctx, id, "settlement", s)
	eng.Tick(ctx)

	in, _ = eng.Get(ctx, id)
	p, _ := saga.StateOf[payout.Payout](in)
	avail, held := ledger.Balance("acc-1")
	fmt.Println(in.Status, p.Settlement, p.Notified, avail, held)
	// Output:
	// RUNNING WAITING settlement
	// COMPLETED SETTLED true 85000 0
}
