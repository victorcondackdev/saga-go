# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses
[Semantic Versioning](https://semver.org/).

## [0.1.0] - 2026-11-22

First public release.

### Added

- `saga`: type-safe, code-first definitions with generics
  (`saga.Define[State](name).Step(...).Parallel(...).Build()`); step options
  `Compensate`, `CompensateAttempted`, `Retry`, `CompensateRetry`, `Timeout`,
  `Deadline`, `When` and `Pivot`; saga-level `Timeout`.
- Durable execution engine: write-ahead persistence of every attempt,
  at-least-once execution with stable idempotency keys, exponential backoff
  with jitter, permanent errors, compensations in reverse order with their
  own retry budget, pivot / forward-recovery semantics, signals with
  `Context.Await`, conditional steps, parallel groups with field-level merge.
- Leasing with heartbeats and fencing (version as fencing token); stale lease
  takeover; poison-pill protection for steps that keep killing their worker.
- `FAILED_NEEDS_ATTENTION` with `Engine.Retry` and `Engine.Resolve` for
  manual resolution; `Engine.Stuck`.
- Observability: hooks, invocation observers, structured `slog` logs,
  Prometheus text metrics, per-saga execution history and an ASCII timeline
  renderer.
- `store`: `Store` interface, in-memory store, and a file-backed WAL store
  (segmented, CRC32-C framed, torn-tail recovery, group commit, compaction).
  Reference PostgreSQL schema in `migrations/postgres.sql` (untested).
- `sagatest`: deterministic simulator with a fake clock and exhaustive crash
  injection at every crash point (and combinations of them), checking saga
  invariants plus user-supplied domain checks.
- Examples: `examples/pix-payout` and `examples/boleto-payment`.
- `cmd/sagad` (HTTP API service) and `cmd/sagactl` (operator CLI).
