package boleto_test

import (
	"context"
	"fmt"
	"testing"

	boleto "github.com/victorcondackdev/saga-go/examples/boleto-payment"
	"github.com/victorcondackdev/saga-go/saga"
	"github.com/victorcondackdev/saga-go/sagatest"
	"github.com/victorcondackdev/saga-go/store"
)

var line = boleto.MakeLine("23793381286000000000300000000400000000012345")

func TestParseLine(t *testing.T) {
	tests := []struct {
		name, line, bank string
		ok               bool
	}{
		{"valid", line, "237", true},
		{"formatted", line[:5] + "." + line[5:10] + " " + line[10:], "237", true},
		{"short", line[:46], "", false},
		{"letters", "a" + line[1:], "", false},
		{"bad check digit", line[:9] + string('0'+(line[9]-'0'+1)%10) + line[10:], "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bank, err := boleto.ParseLine(tt.line)
			if (err == nil) != tt.ok || bank != tt.bank {
				t.Fatalf("ParseLine = %q, %v", bank, err)
			}
		})
	}
}

func harness(seed uint64, amount int64, bankRejects ...bool) sagatest.Harness[boleto.Payment] {
	const opening = 20_000_00
	acc := boleto.NewAccounts(map[string]int64{"acc-1": opening})
	svc := boleto.NewServices(seed)
	svc.FailRate, svc.FraudDeny, svc.BankRejected = 0.2, 0.3, 0.1
	if len(bankRejects) > 0 && bankRejects[0] {
		svc.BankRejected = 1 // the pivot itself fails: everything before it is undone
	}
	return sagatest.Harness[boleto.Payment]{
		Definition: boleto.NewDefinition(acc, svc),
		Input:      boleto.Payment{Account: "acc-1", Line: line, Amount: amount},
		Check: func(t testing.TB, o *sagatest.Outcome[boleto.Payment]) {
			cb := svc.Cashback(saga.IdempotencyKey(o.Instance.ID, "reserve-cashback", false))
			switch o.Instance.Status {
			case store.StatusCompleted:
				if acc.Balance("acc-1") != opening-amount || svc.Paid() != 1 || cb != "granted" {
					t.Errorf("completed: balance %d, paid %d, cashback %q", acc.Balance("acc-1"), svc.Paid(), cb)
				}
				if (amount > boleto.ReviewThreshold) != o.State.Reviewed {
					t.Errorf("fraud review ran=%v for amount %d", o.State.Reviewed, amount)
				}
				if o.State.CashbackID == "" || o.State.ReceiptID == "" {
					t.Errorf("parallel branches not merged: %+v", o.State)
				}
			case store.StatusCompensated:
				if acc.Balance("acc-1") != opening || svc.Paid() != 0 || cb == "granted" {
					t.Errorf("compensated: balance %d, paid %d, cashback %q", acc.Balance("acc-1"), svc.Paid(), cb)
				}
			case store.StatusNeedsAttention:
				if !o.Instance.PivotPassed {
					t.Errorf("needs attention before the pivot: %s", o.Instance.Failure)
				}
			}
		},
		AllowNeedsAttention: true,
	}
}

func TestCrashPointsExhaustive(t *testing.T) {
	for _, amount := range []int64{1_200_00, 7_500_00} { // below and above the review threshold
		t.Run(fmt.Sprint(amount), func(t *testing.T) {
			depth, seeds := 2, []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
			if testing.Short() {
				depth, seeds = 1, seeds[:2]
			}
			rep := sagatest.Explore(t, sagatest.Config{Depth: depth, Seeds: seeds}, func(seed uint64) sagatest.Harness[boleto.Payment] {
				return harness(seed, amount)
			})
			t.Log(rep)
		})
	}
}

func TestPivotRejectedCompensatesEverything(t *testing.T) {
	rep := sagatest.Explore(t, sagatest.Config{Depth: 2}, func(seed uint64) sagatest.Harness[boleto.Payment] {
		return harness(seed, 7_500_00, true)
	})
	if rep.Outcomes[store.StatusCompleted] != 0 || rep.Outcomes[store.StatusCompensated] == 0 {
		t.Fatalf("outcomes %v", rep.Outcomes)
	}
	t.Log(rep)
}

func TestInvalidLineFailsFast(t *testing.T) {
	sagatest.Explore(t, sagatest.Config{}, func(seed uint64) sagatest.Harness[boleto.Payment] {
		h := harness(seed, 10_00)
		h.Input.Line = "123"
		check := h.Check
		h.Check = func(t testing.TB, o *sagatest.Outcome[boleto.Payment]) {
			check(t, o)
			if o.Instance.Status != store.StatusCompensated || o.Invoked("debit-account") != 0 {
				t.Errorf("want immediate compensation, got %s", o.Instance.Status)
			}
		}
		return h
	})
}

func Example() {
	ctx := context.Background()
	acc := boleto.NewAccounts(map[string]int64{"acc-1": 10_000_00})
	def := boleto.NewDefinition(acc, boleto.NewServices(1))
	eng := saga.New(store.NewMemory(), saga.WithLogger(saga.DiscardLogger()))
	_ = eng.Register(def)

	id, _ := saga.Start(ctx, eng, def, "bol-1", boleto.Payment{Account: "acc-1", Line: line, Amount: 350_00})
	eng.Tick(ctx)
	in, _ := eng.Get(ctx, id)
	for _, s := range in.Steps {
		fmt.Printf("%-16s %s\n", s.Name, s.Status)
	}
	fmt.Println(in.Status, acc.Balance("acc-1"))
	// Output:
	// validate         DONE
	// debit-account    DONE
	// reserve-cashback DONE
	// draft-receipt    DONE
	// fraud-review     SKIPPED
	// pay-at-bank      DONE
	// send-receipt     DONE
	// COMPLETED 965000
}
