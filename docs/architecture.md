# Architecture

Design contract and invariants of otel-sqlite. Operations in
[`README.md`](../README.md) and [`docs/operations/`](operations/);
performance background in [`docs/performance.md`](performance.md).

## 1. Product contract

Single-writer SQLite OTLP logs sink:

```text
OTLP gRPC -> bounded ingress queue -> insert batcher -> bounded command queue
          -> single SQLite writer (WAL) -> SQLite database
```

- Default `durability.mode = "commit"`: a successful export means every
  record it reports as accepted has committed to SQLite.
- Queue admission is all-or-nothing: a request that cannot fit is rejected
  without accepting any chunk. If storage disconnects mid-batch, the
  accepted prefix is reported with OTLP `partial_success` instead of a
  retryable error, so retrying clients do not duplicate committed records.
- Writer, batcher, or storage failure produces a bounded, observable
  failure — never a hung request or silent loss.
- Fails closed for configured authentication and TLS requirements.
- Retention, backup, restore, migration, and disk-full behavior are
  documented and tested.
- Performance claims come from external production-mode runs, not only
  embedded benchmarks or tmpfs.

## 2. Design invariants

Preserved by all future work:

- Exactly one component owns and mutates the SQLite connection.
- Ingress and command queues are bounded; ingress uses non-blocking
  enqueue and exposes backpressure.
- Insert batching supports size and age limits, oversized splitting, FIFO
  ordering, and origin isolation.
- Commit ledger provides contiguous durable-ack watermarks, tracks
  per-ticket partial progress, fails tickets whose rows were quarantined,
  and fails pending waiters when the pipeline closes.
- SQLite errors classify into retryable, poisonous, and fatal; poisoned
  rows quarantine while healthy rows commit, and the affected request is
  reported as a partial success, never a plain 2xx.
- Retention covers log events, transactional orphan-resource collection,
  and incremental FTS maintenance.
- TLS/mTLS, bearer-token auth, gRPC health, Prometheus metrics, startup
  storage readiness, Docker volume placement, and in-binary healthcheck exist.
- Partial acceptance is reported with OTLP `partial_success` (rejected
  record count), so a retried request never resends committed records.
  A retryable `UNAVAILABLE` is reserved for requests with no durable
  records.
- OTLP values persist with no silent conversion; malformed trace/span ids
  map to no trace context and count as mapping losses, not request failures.

## 3. Internal design decisions

### 3.1 Durable acknowledgements

`CommitLedger` (`core/src/storage/commit.rs`): commit-ticket registry with
a contiguous watermark over `tokio::sync::watch`. Ingress issues tickets
for accepted chunks and voids rejected ones; the writer records committed
progress for each ticket segment and completes a ticket only when its final
record is written. A ticket whose chunk was split across storage batches
therefore stays outstanding until the whole chunk is durable. The ledger
closes when writer/batcher dies so pending acks fail fast with
`UNAVAILABLE`.

Handlers wait on `ledger.wait_watermark(ticket)` for the request's highest
ticket in `DurabilityMode::Commit` (so a later out-of-order failure cannot
release the report early), then inspect every accepted ticket. A completed
ticket contributes all its records; a failed ticket contributes only its
committed records; the handler reports the rest in `partial_success`.
`Enqueue` mode keeps fire-and-forget and may lose acked records on crash.
Batches carry `RecordSegment`s plus the sorted ticket set
(`LogWriteBatch::commit_seqs`); completing only the maximum would strand
intermediate tickets on the commit watermark forever.

`[durability] mode = "commit" | "enqueue"`, `synchronous = "normal" | "full"`
(defaults `commit` + `normal`). `full` extends durability to OS/power
crashes; `normal` under WAL syncs only at checkpoints.

### 3.2 All-or-nothing admission

`IngestSender` (`ingress/src/lib.rs`) wraps the crossbeam bounded channel
with an admission gate (`Arc<Mutex<()>>` shared by every clone). `enqueue`
reserves room for the whole request up front, then sends every chunk under
one held gate — no other producer (including Flush barriers) can interleave
or steal a slot. On `QueueFull` nothing is issued or sent and the handler
returns `UNAVAILABLE`. A disconnect mid-batch cannot recall chunks already
sent: their tickets stay issued, the unsent ticket is voided, and the
handler reports the accepted prefix with `partial_success` rather than
asking the client to resend records that may already be durable. OTLP
`partial_success` is count-based, so a client that resends the whole request
can still duplicate the committed rows; per-record idempotency is not
implemented.

Chose this over tokio `mpsc` `try_reserve`/`Permit` to keep the storage
blocker `recv_timeout` batcher loop and writer `recv` on the single
blocking-thread/crossbeam model; subtlety stays confined to ingress.

### 3.3 Error classification and quarantine

`storage/src/error.rs`: `BUSY`/`LOCKED` → Retryable; constraint violations
and row-local binding failures → Poison; everything else → Fatal (unknown
shapes may be defects in our SQL — fail loud, don't silently drop data).

- Retryable: up to 4× with exponential backoff (250 ms doubling to 2 s),
  counted in `storage_transient_retries_total`.
- Poisoned batch re-runs in tolerant mode: healthy rows commit, offending
  rows drop-and-count (`storage_quarantined_records_total`). The affected
  tickets settle as failed, and the ingress reports the dropped records as
  `partial_success`; a plain 2xx never covers a quarantined row.
- Fatal: writer exits → ledger closes → pending acks fail fast → watchdog
  halts for supervisor restart.

### 3.4 Crash durability and how it is tested

Proven against the external binary, not just the embedded harness:
`crates/otel-sqlite/tests/crash_durability.rs` boots the real binary, runs
closed-loop load, kills mid-WAL-write, restarts on the same DB, and
validates. Writer/batcher death uses env-gated fault knobs
(`OTEL_SQLITE_FAULT_WRITER_AFTER_N` / `OTEL_SQLITE_FAULT_BATCHER_AFTER_N` in
`storage/src/fault.rs`) exercising the real fatal path. A container test
(`tests/docker/crash-e2e.sh`) docker-kills mid-load and restarts on the
same volume.

### 3.5 Configuration and validation

Defaults centralize through shared ingress/storage constants.
`Config::from_env` / `from_file` validates before metrics, listeners,
storage, or workers start. `max_records_per_request` propagates into
ingress. Mapping runs under a process-wide `mapping_concurrency` budget:
because tokio cannot cancel `spawn_blocking`, the permit is held by the
blocking job until it really finishes, so a timed-out export frees its
stream slot while its abandoned mapping stays counted — retained mapping
memory is bounded by `mapping_concurrency × grpc_max_recv_msg_size`.
Remote metrics need `allow_remote_metrics = true`. TLS/auth files are
readability-checked. Effective config prints as JSON at startup.

### 3.6 Performance model

All writes funnel through one SQLite writer thread. With
`synchronous = "normal"` in WAL mode a commit is a WAL append plus a lock —
no fsync per transaction — so the writer is CPU-bound, not I/O-bound. Every
microsecond of CPU inside a transaction is throughput loss; work moved onto
the batcher overlaps almost for free. The ingest boundary (tokio
multi-thread runtime, one task per request) is the only parallel stage.
Scope metadata follows this: one `LogScope` renders per `ScopeLogs` group
at mapping and is shared by reference, so the writer binds precomputed
bytes. Open bottlenecks and methodology:
[`docs/performance.md`](performance.md).
