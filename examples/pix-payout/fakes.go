package payout

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"
)

// Ledger is an in-memory account ledger with holds. Every mutating call is
// idempotent on its key, as a real ledger API would be.
type Ledger struct {
	mu       sync.Mutex
	balances map[string]int64 // available balance, cents
	holds    map[string]*hold // by hold id
	byKey    map[string]string
	effects  map[string]int // effect counters, for tests
}

type hold struct {
	Account  string
	Amount   int64
	State    string // held, released, captured
	Released string
}

// ErrInsufficientFunds is returned by Hold.
var ErrInsufficientFunds = errors.New("ledger: insufficient funds")

// NewLedger returns a ledger with the given opening balances.
func NewLedger(balances map[string]int64) *Ledger {
	l := &Ledger{balances: map[string]int64{}, holds: map[string]*hold{}, byKey: map[string]string{}, effects: map[string]int{}}
	for k, v := range balances {
		l.balances[k] = v
	}
	return l
}

// Hold moves amount from the available balance into a hold.
func (l *Ledger) Hold(key, account string, amount int64) (string, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if id, ok := l.byKey[key]; ok {
		return id, nil
	}
	if l.balances[account] < amount {
		return "", ErrInsufficientFunds
	}
	id := fmt.Sprintf("hold-%d", len(l.holds)+1)
	l.balances[account] -= amount
	l.holds[id] = &hold{Account: account, Amount: amount, State: "held"}
	l.byKey[key] = id
	l.effects["hold"]++
	return id, nil
}

// Release returns a held amount to the available balance. Releasing an
// unknown, released or captured hold is a no-op for released holds and an
// error for captured ones.
func (l *Ledger) Release(holdID string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	h, ok := l.holds[holdID]
	if !ok || h.State == "released" {
		return nil
	}
	if h.State == "captured" {
		return fmt.Errorf("ledger: hold %s already captured", holdID)
	}
	h.State = "released"
	l.balances[h.Account] += h.Amount
	l.effects["release"]++
	return nil
}

// Capture turns a hold into a definitive debit.
func (l *Ledger) Capture(holdID string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	h, ok := l.holds[holdID]
	if !ok {
		return fmt.Errorf("ledger: unknown hold %s", holdID)
	}
	switch h.State {
	case "captured":
		return nil
	case "released":
		return fmt.Errorf("ledger: hold %s already released", holdID)
	}
	h.State = "captured"
	l.effects["capture"]++
	return nil
}

// Balance returns the available balance and the amount currently held.
func (l *Ledger) Balance(account string) (available, held int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, h := range l.holds {
		if h.Account == account && h.State == "held" {
			held += h.Amount
		}
	}
	return l.balances[account], held
}

// Effects returns how many times each kind of effect happened.
func (l *Ledger) Effects() map[string]int {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := map[string]int{}
	for k, v := range l.effects {
		out[k] = v
	}
	return out
}

// Settlement is the outcome of a Pix payment at the central bank (SPI).
type Settlement struct {
	E2EID  string `json:"e2e_id"`
	Status string `json:"status"` // SETTLED or REJECTED
	Reason string `json:"reason,omitempty"`
}

// PSP is a mock payment service provider. Failures are drawn from a seeded
// source, so a simulation is reproducible:
//
//   - FailRate: the request fails before anything happens (retryable);
//   - TimeoutRate: the payment IS sent but the response is lost, the
//     nastiest case, which only idempotency keys make safe;
//   - RejectRate: the request is refused for good (invalid key);
//   - SettleRejectRate: the payment is accepted but later rejected by SPI.
type PSP struct {
	FailRate, TimeoutRate, RejectRate, SettleRejectRate float64
	// Hang is how long a timed-out call blocks (bounded by its context).
	// Zero returns the timeout immediately, for simulations.
	Hang time.Duration
	// OnSettle, if set, is called (in a goroutine) after SettleAfter with
	// the settlement, as a webhook would be.
	OnSettle    func(sendKey string, s Settlement)
	SettleAfter time.Duration

	mu       sync.Mutex
	rnd      *rand.Rand
	payments map[string]*payment // by send key
	refunds  map[string]bool     // by send key
	sends    int
}

type payment struct {
	Settlement
	Amount int64
	PixKey string
	final  bool // settlement known to the outside world
	polls  int
}

// NewPSP returns a PSP drawing failures from seed.
func NewPSP(seed uint64) *PSP {
	return &PSP{rnd: rand.New(rand.NewPCG(seed, 7)), payments: map[string]*payment{}, refunds: map[string]bool{}}
}

func (p *PSP) roll() float64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.rnd.Float64()
}

// ErrPSPUnavailable is a transient failure.
var ErrPSPUnavailable = errors.New("psp: 503 service unavailable")

// ErrInvalidPixKey is a permanent rejection.
var ErrInvalidPixKey = errors.New("psp: 422 pix key not found in DICT")

// Send initiates a payment. Calls with the same key return the original
// end-to-end id without paying twice.
func (p *PSP) Send(ctx context.Context, key, pixKey string, amount int64) (string, error) {
	p.mu.Lock()
	if pay, ok := p.payments[key]; ok {
		p.mu.Unlock()
		return pay.E2EID, nil
	}
	p.mu.Unlock()
	r := p.roll()
	switch {
	case r < p.FailRate:
		return "", ErrPSPUnavailable
	case r < p.FailRate+p.RejectRate:
		return "", ErrInvalidPixKey
	}
	p.mu.Lock()
	p.sends++
	e2e := fmt.Sprintf("E1234567820260101%011d", p.sends)
	pay := &payment{Settlement: Settlement{E2EID: e2e, Status: "SETTLED"}, Amount: amount, PixKey: pixKey}
	if p.rnd.Float64() < p.SettleRejectRate {
		pay.Status, pay.Reason = "REJECTED", "AC03 creditor account invalid"
	}
	p.payments[key] = pay
	timeout := p.rnd.Float64() < p.TimeoutRate
	onSettle, after := p.OnSettle, p.SettleAfter
	p.mu.Unlock()
	if onSettle != nil {
		go func() {
			time.Sleep(after)
			if s, ok := p.Settle(key); ok {
				onSettle(key, s)
			}
		}()
	}
	if timeout {
		if p.Hang > 0 {
			t := time.NewTimer(p.Hang)
			defer t.Stop()
			select {
			case <-ctx.Done():
				return "", ctx.Err()
			case <-t.C:
			}
		}
		return "", fmt.Errorf("psp: response lost: %w", context.DeadlineExceeded)
	}
	return e2e, nil
}

// PollsToSettle is how many Status calls it takes for a payment whose
// webhook was lost to show its final settlement.
const PollsToSettle = 3

// Status returns the settlement of the payment sent with key, if any. Until
// the payment settles (Settle, or PollsToSettle polls) its status is
// PENDING.
func (p *PSP) Status(key string) (Settlement, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	pay, ok := p.payments[key]
	if !ok {
		return Settlement{}, false
	}
	if !pay.final {
		pay.polls++
		if pay.polls < PollsToSettle {
			return Settlement{E2EID: pay.E2EID, Status: "PENDING"}, true
		}
		pay.final = true
	}
	return pay.Settlement, true
}

// Settle makes the settlement of the payment sent with key final and
// returns it, as the SPI would when it notifies the PSP.
func (p *PSP) Settle(key string) (Settlement, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	pay, ok := p.payments[key]
	if !ok {
		return Settlement{}, false
	}
	pay.final = true
	return pay.Settlement, true
}

// Refund requests the return of the payment sent with sendKey. It is a
// no-op if no such payment exists or it was rejected, and idempotent.
func (p *PSP) Refund(sendKey string) error {
	if p.roll() < p.FailRate {
		return ErrPSPUnavailable
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if pay, ok := p.payments[sendKey]; ok && pay.Status == "SETTLED" {
		p.refunds[sendKey] = true
	}
	return nil
}

// Refunded reports whether the payment sent with key was refunded.
func (p *PSP) Refunded(key string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.refunds[key]
}

// Payments returns how many distinct payments were sent.
func (p *PSP) Payments() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.payments)
}

// Notifier is a mock customer notification service.
type Notifier struct {
	FailRate float64
	mu       sync.Mutex
	rnd      *rand.Rand
	sent     map[string]string
}

// NewNotifier returns a notifier drawing failures from seed.
func NewNotifier(seed uint64) *Notifier {
	return &Notifier{rnd: rand.New(rand.NewPCG(seed, 11)), sent: map[string]string{}}
}

// Notify delivers msg once per key.
func (n *Notifier) Notify(key, msg string) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if _, ok := n.sent[key]; ok {
		return nil
	}
	if n.rnd.Float64() < n.FailRate {
		return errors.New("notifier: push gateway timeout")
	}
	n.sent[key] = msg
	return nil
}

// Sent returns the number of distinct notifications delivered.
func (n *Notifier) Sent() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return len(n.sent)
}
