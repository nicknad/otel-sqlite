# TODO — otel-sqlite backlog

> Single source of truth for open work. Supersedes `docs/HANDOFF.md`,
> `docs/HANDOFF-PERFORMANCE.md`, and `docs/HANDOFF-PRODUCTION.md`.
> Completed items live in git history and the design/ops docs
> (`docs/architecture.md`, `docs/performance.md`, `README.md`,
> `docs/operations/`); this file tracks only what is still to be done.

Status legend: `[ ]` open · `[x]` done (kept only for context). Effort:
S (< 1 day) · M (1–5 days) · L (> 1 week).

## P0 — Pre-production release gates

The build is beta-quality and every P0/P1 backlog item is implemented. These
release-gate requirements (from the old HANDOFF-PRODUCTION §1/§4/§5) are still
open:

- [ ] **Agree acceptance budgets before release** — sustained records/second,
  request p99/p99.9 latency, maximum queue depth, restart recovery time,
  maximum WAL growth, memory limit, and the allowed loss policy for ambiguous
  requests.
- [ ] **Soak test** — meets the agreed throughput, latency, memory, and
  WAL-growth budgets under sustained external production-mode load.
- [ ] **Prometheus alerts** — storage errors, quarantined records, queue
  saturation, unhealthy state, WAL growth, failed backups.
- [ ] **Runbooks** — restart, rollback, restore, disk-full, credential
  rotation, certificate rotation.
- [ ] **Disk-full and read-only storage behavior tests** — bounded, observable
  failure (never a hung request or silent loss).

## P1 — Production pilot

- [ ] **P1-2 (M): test and tune maintenance under load.** Sustained
  ingestion with checkpoint, retention, ANALYZE, and VACUUM enabled; each
  operation completes within an agreed deadline or emits an explicit
  missed-deadline signal; durable-ack latency and queue rejection stay within
  budget; queue-full retry never duplicates or spins; disk/WAL growth stays
  bounded; maintenance behavior when the writer exits. Prefer gating/deferring
  full VACUUM first; add a priority lane only if these tests demonstrate an
  operational failure (preserve FIFO barriers, prove maintenance cannot starve
  ingestion).
- [ ] **P1-4 · OTLP proto provenance (S).** Record the pinned upstream
  OTLP proto version/tag and define the update + compatibility process.
- [ ] **P1-4 · CHANGELOG + upgrade guide (S).** Start a changelog and
  user-facing upgrade guide before publishing a production release.

## P2 — Performance (after safety gates)

- [ ] **P2-1 (PERF-006 · S–M): metric dimension memoization.** Transaction-local
  cache for `resource → scope → metric → series` resolution; cache keys cover
  every fingerprint input; lifetime is one storage batch; fingerprint/JSON
  bytes stay byte-identical; no process-lifetime cache. Validation: batches of
  1k/5k/10k points, repetitive + high-cardinality identities, all five metric
  kinds, no ID collisions, no cross-batch leakage. Design detail:
  `docs/performance.md`.
- [ ] **P2-2 (PERF-007 stage 2 · M–L): prepared rows.** Move hashing/encoding/
  preparation onto the batcher thread (bind/step/commit only in the writer),
  migrating poison/salvage semantics for `ToSqlConversionFailure` to the
  batcher. Only after PERF-006 + profiling prove the writer stays CPU-bound
  while the batcher has headroom. Design detail: `docs/performance.md`.
- [ ] **P2-3 (PERF-007 stage 3 · L): ingress parallelization.** Defer
  indefinitely unless profiling shows the single batcher saturated after
  stage-2 prepared-row work. Design detail: `docs/performance.md`.

## P3 — Low / cosmetic

| Item | Where |
|---|---|
| [ ] Watchdog resets `sidecar_health_state` to Healthy on stop even right after halting | `runtime/src/watchdog.rs` |
| [ ] `queue_depth` gauge only updated at loop-top (slightly stale) | `storage/src/writer.rs` |
| [ ] Histogram/exp-histogram `count` becomes NULL above i64 (silent) | `ingress/src/mapping/metrics.rs` |
| [ ] No keepalive/tcp_nodelay/request-timeout tuning knobs on gRPC server | `ingress/src/grpc.rs` |
| [ ] No schedule jitter: maintenance tasks phase-lock at worker startup | `runtime/src/scheduler.rs` |

Closed without action: boot-time FTS scans removed, deprecated aliases removed,
stale runtime docs fixed, watchdog final-sample logging.

## Review findings — correctness / test / doc nits (2026-08-30 verification)

Found while verifying the production handoff against the code. Small, but each
is either a real (bounded) discrepancy or a test-coverage gap.

### Correctness / behavior

- [ ] **Control-send retry holds the admission gate (S).** `IngestSender::send`
  and `send_timeout` sleep (`CONTROL_RETRY_PAUSE`, 100 µs) *while holding* the
  admission gate (`ingress/src/lib.rs`), contradicting the "waits outside the
  gate" doc. Bounded, not a deadlock; either wait outside the gate or correct
  the doc comment.

Closed: **default listen stance** — `listen_address` now defaults to
`127.0.0.1:4317` (loopback); a non-loopback bind without `[tls]` still prints
the loud startup warning in `main.rs`.

### Test coverage gaps

- [ ] **`otlp_mapping_loss_total` never asserted (S).** Counters exist but no
  test captures them; add unknown-`SeverityNumber` and unknown
  `aggregation_temporality` tests that assert the counter.
- [ ] **Exemplar exact-output for histogram/exp-histogram (S).** Persisted
  `exemplars_json` exact-output is only storage-tested for gauge; add
  histogram and exp-histogram cases.
- [ ] **Config validation: security-file paths (S).** `Config::validate` checks
  TLS cert/client-CA/token-file readability, but tests only break the key file;
  add missing-cert, missing-client-CA, and missing-token-file cases.
- [ ] **Canonical-default assertions (XS).** `grpc_max_recv_msg_size` and
  `batcher_max_batch_records` defaults lack a canonical-value test.
- [ ] **Trace `UNIMPLEMENTED` not directly tested (S).** No test calls the
  trace export RPC; add one asserting `UNIMPLEMENTED`.

### CI / tooling hygiene

- [ ] **`cargo-deny` job lacks `actions/cache` (XS).** Other pure-Rust jobs
  cache Cargo; add it.
- [ ] **`deny.toml` warn-vs-deny policy (XS).** `sources.unknown-registry/git`
  and `bans.multiple-versions` are `warn`; decide whether the release gate
  should hard-fail them.
- [ ] **Stale doc comments (XS).** `storage/src/batcher.rs` still says the loop
  uses `crossbeam_channel::select` (it uses `recv_timeout`); `lib.rs` gate
  wording (see correctness item above).