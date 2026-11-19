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
- The writer may commit several consecutive insert commands in one transaction
  up to `group_commit_max_records`; collection never waits, `Flush`/checkpoint/
  maintenance are hard boundaries, and ticket settlement still happens only
  after the shared commit.
- The log search index is derived data with an operator-selected mode
  (`off`, `body`, `full`). Insert triggers keep the configured shape current;
  a bounded, resumable rebuild covers rows that predate a mode change.
- Commit ledger provides contiguous durable-ack watermarks, tracks
  per-ticket partial progress, fails tickets whose rows were quarantined,
  and fails pending waiters when the pipeline closes.
- SQLite errors classify into retryable, poisonous, and fatal; poisoned
  rows quarantine while healthy rows commit, and the affected request is
  reported as a partial success, never a plain 2xx.
- Retention covers log records, transactional orphan-resource collection,
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

Each crate owns its config section end to end: defaults, the effective
type, and the invariants. `IngressConfig::validate` rejects an unusable
bind address and every wire or mapping limit outside its published range —
the ingress crate is the single owner of those bounds, so direct
constructors fail exactly like the TOML path. `StorageConfig::validate`
owns insert-batch sizing, the command queue, group commit and the size
quota; the runtime's `MaintenanceConfig`/`WatchdogConfig` keep owning their
schedule and verdict invariants. The binary's `Config` stores those crate
types directly (no restated per-crate fields) and adds only what no crate
owns: the pipeline-assembly knob `ingest_queue_capacity`, the admin
metrics endpoint, and the cross-section gates in `validate.rs` —
cleartext/TLS exposure (`allow_insecure_remote`), remote metrics
(`allow_remote_metrics`), listen/metrics address checks, the
watchdog-vs-batch-age rule, the group-commit-below-batch warning and the
CLI-wide duration ceilings. `Config::from_env` / `from_file` validates
before metrics, listeners, storage, or workers start, delegating
per-section rules to the owning crates. The TOML overlay (`config/file.rs`)
is the operator contract in one table: `deny_unknown_fields`, unchanged
key names, each key applied straight into the owning crate's config. Mapping
runs under a process-wide `mapping_concurrency` budget: because tokio cannot
cancel `spawn_blocking`, the permit is held by the blocking job until it
really finishes, so a timed-out export frees its stream slot while its
abandoned mapping stays counted — retained mapping memory is bounded by
`mapping_concurrency × grpc_max_recv_msg_size`. TLS/auth files are
readability-checked. `check` also reports a rebuilding search index or a
configured/installed mode mismatch. Effective config prints as JSON at
startup with unchanged keys. `otel-sqlite check` runs the same validation
plus PEM parsing and a read-only schema inspection as a non-mutating
preflight (`check --json` for automation).

### 3.6 Performance model

All writes funnel through one SQLite writer thread. With
`synchronous = "normal"` in WAL mode a commit is a WAL append plus a lock —
no fsync per transaction — so the writer is CPU-bound, not I/O-bound. Every
microsecond of CPU inside a transaction is throughput loss; work moved onto
the batcher overlaps almost for free. The ingest boundary (tokio
multi-thread runtime, one task per request) is the only parallel stage.
Scope metadata follows this: one `LogScope` renders per `ScopeLogs` group
at mapping and is shared by reference, so the writer binds precomputed
bytes. The writer also coalesces consecutive insert commands already in the
command queue into one transaction up to `group_commit_max_records`, which
amortises the fixed commit cost when high resource cardinality produces many
small per-origin batches. Collection never waits, so a writer that keeps up
still commits one command at a time. Open bottlenecks and methodology:
[`docs/performance.md`](performance.md).

### 3.7 Search index modes

The log search index is derived data and never part of the canonical
contract. `[search] mode` selects it: `off` keeps `logs_fts` present but empty
with no triggers, `body` (default) indexes only `log_event.body`, and `full`
keeps the legacy six-column index. Startup reconciliation compares the
configured mode with `search_index_state` and installs a matching shape
without scanning `log_event`. A mode change or the async `006` upgrade marks
the index `rebuilding`; the writer then backfills in bounded, resumable
transactions (range delete then authoritative insert, cursor advanced in the
same commit) whenever the command queue is idle, plus one small step per
second under sustained load. `RebuildFts` restarts that background rebuild and
is a no-op when the index is disabled. Search results are partial while the
state row says `rebuilding`; `otel-sqlite check` and `verify` report that state
instead of treating the derived index as corruption.

### 3.8 Pipeline assembly and drain

`crates/otel-sqlite/src/pipeline.rs`, re-exported from the package's library
target (`otel_sqlite::{Pipeline, PipelineSpecs, ...}`) so harnesses reach it
too, is the single assembly site. Before it existed, three places assembled
the pipeline by hand — the binary, the embedded e2e harness (which silently
omitted the watchdog and the maintenance worker, so no e2e scenario could
exercise a watchdog halt or a purge cadence), and per-test wiring — and each
re-learned the same boot and teardown protocols.

`Pipeline::boot(PipelineSpecs)` takes the four crate configs plus the ingest
queue capacity and returns a handle owning the storage handle, the maintenance
worker, the watchdog, the retained ingest producer, the commit ledger and the
health adapter both evidence consumers sample. Boot is the readiness gate:
`Storage::open` still blocks on migrations, the startup checkpoint and file
hardening, and a boot that fails partway stops and joins whatever had already
started. `Pipeline::serve` is the production transport (blocks until an OS
signal or the watchdog halt); harnesses that own their transport take
`Pipeline::logs_service` plus `Pipeline::halt_signal` instead. Both stop
serving on the watchdog's one-shot halt.

The drain is one operation, `Pipeline::drain()`, preserving the shutdown
ordering that partial teardown would otherwise lose: flush barrier → stop
observation (halt evidence captured first) → stop maintenance scheduling →
drop the last producer → bounded join within the storage shutdown budget
captured at boot. Every phase runs even if an earlier one failed — aborting
would leave the maintenance worker holding the command queue open and the
writer undrained, so buffered records would be lost when the process exits —
and every failure lands in the returned `DrainReport`. The barrier refuses
with stranded-chunk evidence when the pipeline is already dead
(`FlushRefused`), instead of blocking; the bounded join reports a wedged
writer instead of hanging shutdown. The same barrier is available mid-run as
`Pipeline::flush`, which is what the e2e drain step uses as its order
barrier.

The embedded e2e server (`tests/otel-sqlite-e2e/src/server.rs`) boots this
same assembly — real watchdog, real maintenance worker — via a
`RuntimePolicy` knob, and `tests/otel-sqlite-e2e/tests/e2e.rs` asserts the
previously unreachable purge cadence plus watchdog non-halt evidence through
it. Storage-crate tests that drive `Storage` directly with raw `Flush`
messages are intentionally unchanged: they test sub-assemblies below this
seam.
