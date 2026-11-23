// Package boleto is an example saga: paying a boleto bancário from a
// customer's account. It exercises a parallel group, a conditional step
// and a pivot:
//
//	validate -> debit-account -> [reserve-cashback | draft-receipt]
//	         -> fraud-review (only above R$ 5.000,00) -> pay-at-bank (pivot)
//	         -> send-receipt
//
// Debit and cashback are compensable. Paying at the bank (liquidation
// through the interbank clearing) cannot be undone by the payer, so it is
// the pivot; the receipt is retried forward only.
package boleto

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"
	"time"

	"github.com/victorcondackdev/saga-go/saga"
)

// Type is the saga type name.
const Type = "boleto-payment"

// ReviewThreshold is the amount, in cents, above which fraud review runs.
const ReviewThreshold = 5_000_00

// Payment is the saga state.
type Payment struct {
	Account string `json:"account"`
	Line    string `json:"line"` // linha digitável, 47 digits
	Amount  int64  `json:"amount"`

	BankCode   string `json:"bank_code,omitempty"`
	DebitID    string `json:"debit_id,omitempty"`
	CashbackID string `json:"cashback_id,omitempty"`
	ReceiptID  string `json:"receipt_id,omitempty"`
	Reviewed   bool   `json:"reviewed,omitempty"`
	BankAuth   string `json:"bank_auth,omitempty"`
	Sent       bool   `json:"sent,omitempty"`
}

// ParseLine validates a 47-digit linha digitável (three mod-10 field check
// digits) and returns the issuing bank code.
func ParseLine(line string) (bank string, err error) {
	line = strings.NewReplacer(".", "", " ", "").Replace(line)
	if len(line) != 47 {
		return "", fmt.Errorf("linha digitável must have 47 digits, got %d", len(line))
	}
	for _, c := range line {
		if c < '0' || c > '9' {
			return "", errors.New("linha digitável must be numeric")
		}
	}
	fields := [][2]int{{0, 9}, {10, 20}, {21, 31}}
	for i, f := range fields {
		if mod10(line[f[0]:f[1]]) != int(line[f[1]]-'0') {
			return "", fmt.Errorf("field %d check digit mismatch", i+1)
		}
	}
	return line[:3], nil
}

func mod10(s string) int {
	sum, w := 0, 2
	for i := len(s) - 1; i >= 0; i-- {
		p := int(s[i]-'0') * w
		sum += p/10 + p%10
		w = 3 - w
	}
	return (10 - sum%10) % 10
}

// MakeLine builds a valid linha digitável for tests and demos from 44
// arbitrary digits.
func MakeLine(digits string) string {
	b := []byte(digits)
	out := make([]byte, 0, 47)
	for _, f := range [][2]int{{0, 9}, {9, 19}, {19, 29}} {
		seg := string(b[f[0]:f[1]])
		out = append(out, seg...)
		out = append(out, byte('0'+mod10(seg)))
	}
	out = append(out, b[29:]...)
	return string(out)
}

// Accounts is a fake core-banking ledger, idempotent on keys.
type Accounts struct {
	mu       sync.Mutex
	balances map[string]int64
	debits   map[string]int64 // key -> amount
	credits  map[string]bool  // reversed debit keys
}

// NewAccounts returns accounts with opening balances.
func NewAccounts(b map[string]int64) *Accounts {
	a := &Accounts{balances: map[string]int64{}, debits: map[string]int64{}, credits: map[string]bool{}}
	for k, v := range b {
		a.balances[k] = v
	}
	return a
}

// Debit removes amount once per key.
func (a *Accounts) Debit(key, acct string, amount int64) (string, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, ok := a.debits[key]; ok {
		return "dbt-" + key, nil
	}
	if a.balances[acct] < amount {
		return "", saga.Permanent(errors.New("insufficient funds"))
	}
	a.balances[acct] -= amount
	a.debits[key] = amount
	return "dbt-" + key, nil
}

// Reverse credits back the debit made with debitKey, once.
func (a *Accounts) Reverse(debitKey, acct string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	amt, ok := a.debits[debitKey]
	if !ok || a.credits[debitKey] {
		return
	}
	a.credits[debitKey] = true
	a.balances[acct] += amt
}

// Balance returns an account balance.
func (a *Accounts) Balance(acct string) int64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.balances[acct]
}

// Services bundles the other fakes: cashback program, fraud engine and the
// bank. Failure rates are drawn from a seeded source.
type Services struct {
	FailRate     float64 // transient failures of every call
	FraudDeny    float64 // probability fraud review denies
	BankRejected float64 // probability the bank refuses the boleto for good

	mu       sync.Mutex
	rnd      *rand.Rand
	cashback map[string]string // key -> state (granted, revoked)
	paid     map[string]string // key -> auth code
	fraud    map[string]bool
	receipts map[string]bool
}

// NewServices returns services drawing failures from seed.
func NewServices(seed uint64) *Services {
	return &Services{rnd: rand.New(rand.NewPCG(seed, 3)), cashback: map[string]string{}, paid: map[string]string{}, fraud: map[string]bool{}, receipts: map[string]bool{}}
}

func (s *Services) flaky() error {
	if s.rnd.Float64() < s.FailRate {
		return errors.New("upstream 503")
	}
	return nil
}

// GrantCashback reserves cashback once per key.
func (s *Services) GrantCashback(key string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.cashback[key]; ok {
		return "cb-" + key, nil
	}
	if err := s.flaky(); err != nil {
		return "", err
	}
	s.cashback[key] = "granted"
	return "cb-" + key, nil
}

// RevokeCashback revokes the cashback granted with key, if any.
func (s *Services) RevokeCashback(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.flaky(); err != nil {
		return err
	}
	if _, ok := s.cashback[key]; ok {
		s.cashback[key] = "revoked"
	}
	return nil
}

// Cashback returns the state of the cashback granted with key.
func (s *Services) Cashback(key string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cashback[key]
}

// Review runs fraud review once per key; the decision is remembered.
func (s *Services) Review(key string) (approved bool, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if v, ok := s.fraud[key]; ok {
		return v, nil
	}
	if err := s.flaky(); err != nil {
		return false, err
	}
	s.fraud[key] = s.rnd.Float64() >= s.FraudDeny
	return s.fraud[key], nil
}

// Pay liquidates the boleto at the bank once per key.
func (s *Services) Pay(key, line string, amount int64) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if auth, ok := s.paid[key]; ok {
		if auth == "" {
			return "", saga.Permanent(errors.New("bank: boleto already paid or cancelled"))
		}
		return auth, nil
	}
	if err := s.flaky(); err != nil {
		return "", err
	}
	if s.rnd.Float64() < s.BankRejected {
		s.paid[key] = ""
		return "", saga.Permanent(errors.New("bank: boleto already paid or cancelled"))
	}
	auth := fmt.Sprintf("AUT%06d", len(s.paid)+1)
	s.paid[key] = auth
	return auth, nil
}

// Paid returns how many boletos were liquidated.
func (s *Services) Paid() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, a := range s.paid {
		if a != "" {
			n++
		}
	}
	return n
}

// SendReceipt emails the receipt once per key.
func (s *Services) SendReceipt(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.receipts[key] {
		return nil
	}
	if err := s.flaky(); err != nil {
		return err
	}
	s.receipts[key] = true
	return nil
}

// NewDefinition builds the saga.
func NewDefinition(acc *Accounts, svc *Services) *saga.Definition[Payment] {
	retry := saga.RetryPolicy{MaxAttempts: 6, Initial: 100 * time.Millisecond, Max: 2 * time.Second}
	return saga.Define[Payment](Type).
		Step("validate", func(sc saga.Context, p *Payment) error {
			bank, err := ParseLine(p.Line)
			if err != nil {
				return saga.Permanent(err)
			}
			if p.Amount <= 0 {
				return saga.Permanent(errors.New("amount must be positive"))
			}
			p.BankCode = bank
			return nil
		}).
		Step("debit-account", func(sc saga.Context, p *Payment) error {
			id, err := acc.Debit(sc.IdempotencyKey(), p.Account, p.Amount)
			p.DebitID = id
			return err
		}, saga.Compensate(func(sc saga.Context, p *Payment) error {
			acc.Reverse(saga.IdempotencyKey(sc.SagaID(), "debit-account", false), p.Account)
			return nil
		})).
		Parallel(
			saga.Par("reserve-cashback", func(sc saga.Context, p *Payment) error {
				id, err := svc.GrantCashback(sc.IdempotencyKey())
				p.CashbackID = id
				return err
			}, saga.Retry(retry), saga.Compensate(func(sc saga.Context, p *Payment) error {
				return svc.RevokeCashback(saga.IdempotencyKey(sc.SagaID(), "reserve-cashback", false))
			})),
			saga.Par("draft-receipt", func(sc saga.Context, p *Payment) error {
				p.ReceiptID = "rcpt-" + sc.SagaID()
				return nil
			}),
		).
		Step("fraud-review", func(sc saga.Context, p *Payment) error {
			ok, err := svc.Review(sc.IdempotencyKey())
			if err != nil {
				return err
			}
			if !ok {
				return saga.Permanent(errors.New("fraud review denied the payment"))
			}
			p.Reviewed = true
			return nil
		}, saga.When(func(p *Payment) bool { return p.Amount > ReviewThreshold }), saga.Retry(retry)).
		Step("pay-at-bank", func(sc saga.Context, p *Payment) error {
			auth, err := svc.Pay(sc.IdempotencyKey(), p.Line, p.Amount)
			p.BankAuth = auth
			return err
		}, saga.Pivot(), saga.Retry(retry)).
		Step("send-receipt", func(sc saga.Context, p *Payment) error {
			if err := svc.SendReceipt(sc.IdempotencyKey()); err != nil {
				return err
			}
			p.Sent = true
			return nil
		}, saga.Retry(saga.RetryPolicy{MaxAttempts: 20, Initial: time.Second, Max: time.Minute})).
		MustBuild()
}
