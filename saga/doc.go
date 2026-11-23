// Package saga is a durable saga orchestrator: it runs a sequence of local
// transactions (steps) against other systems and, if one fails for good,
// undoes the ones that succeeded by running their compensations in reverse
// order.
//
// Definitions are code, typed by a state struct that is persisted after
// every step:
//
//	def := saga.Define[Payout]("pix-payout").
//		Step("reserve-funds", reserve, saga.Compensate(release)).
//		Step("send-pix", send, saga.Compensate(refund), saga.CompensateAttempted(),
//			saga.Retry(saga.RetryPolicy{MaxAttempts: 5}), saga.Timeout(2*time.Second)).
//		Step("confirm-settlement", confirm, saga.Pivot()).
//		Step("notify", notify, saga.Retry(saga.RetryPolicy{MaxAttempts: 50})).
//		MustBuild()
//
// An Engine executes instances through a store.Store. Every transition is
// persisted before a step runs (write-ahead) and after it returns, so a
// crash at any point resumes from the last durable state; steps are executed
// at least once and receive a stable idempotency key so the systems they
// call can make the effect happen exactly once.
//
// # Lifecycle
//
//	PENDING -> RUNNING -> COMPLETED
//	                   -> COMPENSATING -> COMPENSATED
//	                                   -> FAILED_NEEDS_ATTENTION
//	RUNNING (past the pivot)           -> FAILED_NEEDS_ATTENTION
//
// FAILED_NEEDS_ATTENTION is reached only when the engine cannot make a safe
// decision on its own: a compensation exhausted its retries, a step past the
// pivot did, or a step kept killing its worker. Engine.Retry and
// Engine.Resolve are the manual way out.
//
// # Pivot
//
// A step marked Pivot is the point of no return. Before it, any permanent
// failure is handled backwards (compensation). After it has durably
// succeeded, the saga only moves forward: later steps are retried, and if
// they still fail an operator is called instead of undoing work that can no
// longer be undone.
//
// See package sagatest for a deterministic simulator that crashes the engine
// at every crash point of a definition and checks the invariants above.
package saga
