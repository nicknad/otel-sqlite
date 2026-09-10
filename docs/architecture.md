# Architecture

Design contract and invariants of otel-sqlite. Operational guides live in
[`README.md`](../README.md) and [`docs/operations/`](operations/); performance
background in [`docs/performance.md`](performance.md); open work in
[`TODO.md`](../TODO.md).

## 1. Product contract

The system is a single-writer SQLite OTLP logs/metrics sink:

```text
OTLP gRPC -> bounded ingress queue -> insert batcher -> bounded command queue
          -> single SQLite writer (WAL) -> SQLite database
```

The production contract:

- In the default `durability.mode = "commit"`, a successful export response
  means every accepted record has committed to SQLite.
- A request that cannot be accepted completely must not cause retrying clients
  to duplicate records.
- A writer, batcher, or storage failure must produce a bounded, observable
  failure, never a hung request or silent loss.
- The service fails closed for configured authentication and TLS requirements.
- Data retention, backup, restore, schema migration, and disk-full behavior are
  documented and tested.
- Performance claims are based on external production-mode runs, not only
  embedded benchmarks or tmpfs.

Acceptance budgets (sustained records/second, p99/p99.9 latency, maximum queue
depth, restart recovery time, maximum WAL growth, memory limit, allowed loss
policy for ambiguous requests) must be agreed before release — tracked in
[`TODO.md`](../TODO.md).

## 2. Design invariants

These properties must be preserved by all future work:

- Exactly one component owns and mutates the SQLite connection.
- Ingress and command queues are bounded.
- Ingress uses non-blocking enqueue and exposes backpressure.
- Insert batching supports size and age limits, oversized splitting, FIFO
  ordering, and origin isolation.
- The commit ledger provides contiguous durable-acknowledgement watermarks and
  fails pending waiters when the pipeline closes.
- SQLite errors are classified into retryable, poisonous, and fatal classes.
- Poisoned rows can be quarantined while healthy rows continue to commit.
- Retention supports logs and metrics, transactional orphan-dimension
  collection, and incremental FTS maintenance.
- TLS/mTLS, bearer-token authentication, gRPC health, Prometheus metrics,
  startup storage readiness, Docker volume placement, and an in-binary
  healthcheck exist.
- All-or-nothing request admission: no partial success is ever emitted, so a
  retried request is guaranteed clean.
- OTLP values are preserved with no silent conversion; malformed identity
  fields reject the whole request.

## 3. Internal design decisions

Condensed from the closed reliability gaps; the full change history is in git.

### 3.1 Durable acknowledgements

`CommitLedger` (`core/src/storage/commit.rs`) is a commit-ticket registry with
a contiguous watermark published via `tokio::sync::watch`. Ingress issues
tickets for accepted chunks and voids rejected ones; the writer completes every
ticket folded into a committed batch; the ledger closes when the writer/batcher
dies so pending durable acks fail fast with `UNAVAILABLE` instead of hanging.

Handlers hold the response until `ledger.committed(ticket)` resolves for the
request's highest accepted ticket in `DurabilityMode::Commit`. `Enqueue` mode
preserves the old fire-and-forget behaviour and may lose acknowledged records
on a crash. Batching folds the full sorted set of absorbed tickets into the
commit (`LogWriteBatch::commit_seqs`); claiming only the maximum strands
intermediate tickets on the watermark forever.

`[durability] mode = "commit" | "enqueue"` and `synchronous = "normal" | "full"`
(defaults `commit` + `normal`) are wired through `StorageConfig`/`IngressConfig`.
`full` extends durability to OS/power crashes; `normal` under WAL only syncs at
checkpoints.

### 3.2 All-or-nothing admission

`IngestSender` (`ingress/src/lib.rs`) wraps the crossbeam bounded channel with
an admission gate (`Arc<Mutex<()>>` shared by every clone) and a
`reserve(n) -> Result<AdmissionGuard, QueueFull>` primitive. `enqueue` reserves
room for the whole request up front, then issues tickets and sends every chunk
under one held gate, so no other producer (including control messages such as
Flush barriers) can interleave or steal a reserved slot. On `QueueFull` nothing
is issued or sent and the watermark is untouched; on disconnect mid-batch every
issued ticket is voided. Handlers return `UNAVAILABLE` on rejection, checked
before the durable-ack wait.

This was chosen over the tokio `mpsc` alternative (`try_reserve`/`Permit`
would be cleaner at the call site but forces the storage crate's blocking
`recv_timeout` batcher loop and the writer's blocking `recv` to become
async/try-poll). The wrapper confines the subtlety to the ingress crate and
keeps the whole pipeline on one blocking-thread/crossbeam model.

### 3.3 Error classification and quarantine

`storage/src/error.rs` derives `FailureClass::{Retryable, Poison, Fatal}` from
rusqlite codes: BUSY/LOCKED → Retryable; constraint violations and row-local
binding failures → Poison; everything else → Fatal by design (unknown shapes
may be defects in our own SQL — fail loud rather than silently drop user data).

- Retryable transactions re-attempt up to 4× with exponential backoff
  (250 ms → 4 s), counted in `storage_transient_retries_total`.
- The salvage pass re-runs a poisoned batch in tolerant mode: healthy rows
  commit, offending rows are dropped-and-counted
  (`storage_quarantined_records_total`). All durability tickets complete either
  way — acks stay truthful.
- Fatal errors propagate → writer exit → ledger closed → pending durable acks
  fail fast → watchdog halts for supervisor restart.

### 3.4 Crash durability and how it is tested

Durability is proven against the external binary, not only the embedded e2e
harness: `crates/otel-sqlite/tests/crash_durability.rs` boots the real binary,
runs closed-loop load, kills mid-WAL-write, restarts on the same database, and
validates. Writer/batcher death is tested independently through inert-unless-set
env-gated fault knobs (`OTEL_SQLITE_FAULT_WRITER_AFTER_N` /
`OTEL_SQLITE_FAULT_BATCHER_AFTER_N` in `storage/src/fault.rs`) that make the
thread exit via its real fatal path — clearing liveness, closing the ledger
(writer), and letting the watchdog halt ingestion. A container-level test
(`tests/docker/crash-e2e.sh`) docker-kills the sidecar mid-load and restarts on the
same volume. The embedded harness remains useful for fast correctness tests but
cannot model process death accurately.

### 3.5 Configuration and validation

Defaults are centralized through shared ingress/storage constants and the
application configuration. Validation runs from `Config::from_env` and
`Config::from_file`, before metrics, listeners, storage, or workers start.
`max_records_per_request` is configurable and propagated into ingress. Remote
metrics require explicit `allow_remote_metrics = true`. TLS/auth files are
checked for readability. The effective configuration is emitted as a
machine-readable JSON startup dump.

### 3.6 Performance model

All writes funnel through one SQLite writer thread. With
`durability.synchronous = "normal"` in WAL mode a commit is an append to the
WAL plus a lock operation — no fsync per transaction — so the writer is
predominantly CPU-bound, not I/O-bound. Every microsecond of CPU inside a
transaction is direct throughput loss; CPU work moved off the writer onto the
batcher overlaps almost for free. The ingest boundary (tokio multi-thread
runtime, one task per OTLP request) is the only genuinely parallel stage today.
The open performance backlog (PERF-006/007) and its measurement methodology are
detailed in [`docs/performance.md`](performance.md).