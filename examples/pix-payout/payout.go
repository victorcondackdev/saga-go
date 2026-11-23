// Package payout is an example saga: a Pix payout from a customer's
// account.
//
//	reserve-funds  ->  send-pix  ->  confirm-settlement (pivot)  ->  notify
//	release-funds <-  request-refund <-
//
// Funds are put on hold first, so a failure later can simply release them.
// The Pix is then sent through a PSP. Sending is ambiguous (the response
// can be lost after the money moved), so its compensation, a refund request
// keyed by the original send key, runs even if no attempt was confirmed
// (CompensateAttempted). Settlement is confirmed by a webhook signal, with
// polling as a fallback; capturing the hold at that point is the pivot:
// money has left the bank, so from there on the saga only moves forward and
// a failing notification calls an operator instead of undoing a payment.
package payout

import (
	"errors"
	"fmt"
	"time"

	"github.com/victorcondackdev/saga-go/saga"
)

// Type is the saga type name.
const Type = "pix-payout"

// Payout is the saga state.
type Payout struct {
	PayoutID string `json:"payout_id"`
	Account  string `json:"account"`
	PixKey   string `json:"pix_key"`
	Amount   int64  `json:"amount"` // cents

	HoldID     string `json:"hold_id,omitempty"`
	SendKey    string `json:"send_key,omitempty"`
	E2EID      string `json:"e2e_id,omitempty"`
	Settlement string `json:"settlement,omitempty"`
	Refunded   bool   `json:"refunded,omitempty"`
	Notified   bool   `json:"notified,omitempty"`
}

// Deps are the systems the saga talks to.
type Deps struct {
	Ledger   *Ledger
	PSP      *PSP
	Notifier *Notifier
	// SendTimeout bounds one call to the PSP. Default 2s.
	SendTimeout time.Duration
	// SettlementDeadline bounds how long to wait for settlement before
	// refunding. Default 5 minutes.
	SettlementDeadline time.Duration
}

// NewDefinition builds the saga over deps.
func NewDefinition(d Deps) *saga.Definition[Payout] {
	if d.SendTimeout == 0 {
		d.SendTimeout = 2 * time.Second
	}
	if d.SettlementDeadline == 0 {
		d.SettlementDeadline = 5 * time.Minute
	}
	reserve := func(sc saga.Context, p *Payout) error {
		if p.Amount <= 0 {
			return saga.Permanent(fmt.Errorf("invalid amount %d", p.Amount))
		}
		id, err := d.Ledger.Hold(sc.IdempotencyKey(), p.Account, p.Amount)
		if errors.Is(err, ErrInsufficientFunds) {
			return saga.Permanent(err)
		}
		p.HoldID = id
		return err
	}
	release := func(sc saga.Context, p *Payout) error {
		return d.Ledger.Release(p.HoldID)
	}
	send := func(sc saga.Context, p *Payout) error {
		e2e, err := d.PSP.Send(sc, sc.IdempotencyKey(), p.PixKey, p.Amount)
		if errors.Is(err, ErrInvalidPixKey) {
			return saga.Permanent(err)
		}
		if err != nil {
			return err
		}
		p.SendKey, p.E2EID = sc.IdempotencyKey(), e2e
		return nil
	}
	refund := func(sc saga.Context, p *Payout) error {
		// The forward key is deterministic, so the refund can target the
		// payment even when no send attempt ever reported back.
		if err := d.PSP.Refund(saga.IdempotencyKey(sc.SagaID(), "send-pix", false)); err != nil {
			return err
		}
		p.Refunded = true
		return nil
	}
	confirm := func(sc saga.Context, p *Payout) error {
		var s Settlement
		if err := sc.Await("settlement", &s); err != nil {
			if !saga.IsWaiting(err) {
				return err
			}
			// Webhooks get lost: fall back to asking the PSP.
			polled, ok := d.PSP.Status(p.SendKey)
			if !ok || polled.Status == "PENDING" {
				return err
			}
			s = polled
		}
		p.Settlement = s.Status
		if s.Status != "SETTLED" {
			return saga.Permanent(fmt.Errorf("settlement %s: %s", s.Status, s.Reason))
		}
		return d.Ledger.Capture(p.HoldID)
	}
	notify := func(sc saga.Context, p *Payout) error {
		msg := fmt.Sprintf("Pix of R$ %d,%02d sent (%s)", p.Amount/100, p.Amount%100, p.E2EID)
		if err := d.Notifier.Notify(sc.IdempotencyKey(), msg); err != nil {
			return err
		}
		p.Notified = true
		return nil
	}
	fast := saga.RetryPolicy{MaxAttempts: 5, Initial: 200 * time.Millisecond, Max: 5 * time.Second, Multiplier: 2, Jitter: 0.2}
	return saga.Define[Payout](Type).
		Step("reserve-funds", reserve, saga.Compensate(release)).
		Step("send-pix", send, saga.Compensate(refund), saga.CompensateAttempted(),
			saga.Retry(fast), saga.Timeout(d.SendTimeout)).
		Step("confirm-settlement", confirm, saga.Pivot(), saga.Deadline(d.SettlementDeadline), saga.Retry(fast)).
		Step("notify", notify, saga.Retry(saga.RetryPolicy{MaxAttempts: 12, Initial: time.Second, Max: time.Minute})).
		MustBuild()
}
