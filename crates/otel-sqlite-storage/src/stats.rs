//! Shared counters for the storage pipeline.
//!
//! Three concerns, one module:
//!
//! * [`WriterStats`] - atomic counters owned by the SQLite writer thread,
//! * [`BatcherStats`] - atomic counters owned by the insert batcher thread,
//! * [`StorageStatsSnapshot`] - the consistent point-in-time view surfaced
//!   through `Storage::stats()`.
//!
//! Counters use relaxed ordering: they are diagnostics, not synchronization.
//! Each thread updates its own struct; the snapshot only reads.
//!
//! Beyond raw counters each thread also publishes *progress evidence* for the
//! watchdog: a monotonic-milliseconds timestamp of its last meaningful step
//! (`last_progress_ms`) and a liveness flag cleared by a drop guard on every
//! exit path, including panics. The watchdog only ever reads these values;
//! nothing here blocks or wakes the observed threads.

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

use otel_sqlite_core::storage::CommitLedger;
use otel_sqlite_core::time::monotonic_millis;

/// Writer-side counters (`writer` thread).
///
/// Naming contract with the batcher side: the writer counts what it
/// *receives and persists* (`batches_received` increments only for insert
/// commands, before their transaction runs), while the batcher counts what it
/// *emitted* (`BatcherStats::batches_emitted`). The two are equal in steady
/// state; they diverge only when submissions fail (see `dropped_records`).
#[derive(Debug, Default)]
pub(crate) struct WriterStats {
    pub records_written: AtomicU64,
    pub transactions_committed: AtomicU64,
    /// Insert commands received from the command queue. One insert command is
    /// exactly one write batch and becomes exactly one transaction.
    pub batches_received: AtomicU64,
    pub maintenance_runs: AtomicU64,
    pub errors: AtomicU64,
    /// Data rows dropped by salvage passes after poison-class sqlite
    /// errors (constraint violations, bad inputs). Healthy siblings in the
    /// same batch are still persisted.
    pub quarantined_records: AtomicU64,
    /// Extra attempts made because of transient contention (BUSY/LOCKED).
    pub transient_retries: AtomicU64,
    pub queue_depth: AtomicUsize,
    /// Monotonic ms of the last successful command execution (commit,
    /// checkpoint or maintenance operation).
    pub last_progress_ms: AtomicU64,
    /// True while the writer is executing a maintenance-class command
    /// (checkpoint, prune, vacuum, FTS rebuild, quota eviction). During that
    /// window insert commits legitimately stop, so the watchdog must not
    /// apply its ordinary stall rules.
    pub maintenance_in_progress: AtomicBool,
    /// Monotonic ms at which the current maintenance operation started;
    /// `0` while none is in progress. Used to bound a genuinely hung
    /// maintenance call.
    pub maintenance_started_ms: AtomicU64,
    /// False while the writer thread is not running; cleared by a drop guard
    /// so early returns and panics are both covered.
    pub running: AtomicBool,
}

impl WriterStats {
    /// Creates counters whose progress clock starts "now", so a freshly
    /// started pipeline is never mistaken for a stalled one.
    pub(crate) fn new() -> Self {
        let stats = Self::default();
        stats.mark_progress();
        stats
    }

    pub(crate) fn mark_progress(&self) {
        self.last_progress_ms
            .store(monotonic_millis(), Ordering::Relaxed);
    }

    /// Marks the thread as running until the returned guard is dropped (or
    /// unwound). Bind it to a named variable — `let _ = ...` would drop it
    /// immediately.
    pub(crate) fn running_guard(&self) -> RunningGuard<'_> {
        RunningGuard::new(&self.running)
    }

    /// Publishes "maintenance in progress" until the returned guard is
    /// dropped (or unwound), timestamping the start for stall detection.
    pub(crate) fn maintenance_guard(&self) -> MaintenanceGuard<'_> {
        self.maintenance_started_ms
            .store(monotonic_millis(), Ordering::Relaxed);
        self.maintenance_in_progress.store(true, Ordering::Relaxed);
        MaintenanceGuard(self)
    }
}

/// RAII maintenance flag: set on creation, cleared (with the start timestamp)
/// on drop, covering normal returns, `?` and panics alike.
pub(crate) struct MaintenanceGuard<'a>(&'a WriterStats);

impl Drop for MaintenanceGuard<'_> {
    fn drop(&mut self) {
        self.0
            .maintenance_in_progress
            .store(false, Ordering::Relaxed);
        self.0.maintenance_started_ms.store(0, Ordering::Relaxed);
    }
}

/// Insert-batcher counters (`batcher` thread), surfaced through
/// `Storage::stats()` alongside the writer counters.
///
/// See [`WriterStats`] for the naming contract between `batches_emitted`
/// (this side) and `batches_received` (writer side).
#[derive(Debug, Default)]
pub struct BatcherStats {
    /// Mapped ingest chunks accepted from the ingress channel. Same counter as
    /// `StorageStatsSnapshot::chunks_ingested`; one name across atomic and
    /// snapshot on purpose.
    pub chunks_ingested: AtomicU64,
    /// Storage-sized write batches handed to the command queue successfully.
    pub batches_emitted: AtomicU64,
    pub timer_flushes: AtomicU64,
    pub buffered_records: AtomicUsize,
    /// Command-queue depth as observed by the producer after each submission.
    /// The writer's own `queue_depth` is only refreshed at the top of its
    /// loop, so it goes stale while a long command executes; the watchdog
    /// takes the max of the two to avoid under-reporting backlog.
    pub queued_commands: AtomicUsize,
    pub dropped_records: AtomicU64,
    /// Monotonic ms of the last chunk received or batch submitted.
    pub last_progress_ms: AtomicU64,
    /// False while the batcher thread is not running; cleared by a drop guard
    /// so early exits and panics are both covered.
    pub running: AtomicBool,
}

impl BatcherStats {
    pub(crate) fn new() -> Self {
        let stats = Self::default();
        stats.mark_progress();
        stats
    }

    pub(crate) fn mark_progress(&self) {
        self.last_progress_ms
            .store(monotonic_millis(), Ordering::Relaxed);
    }

    /// Marks the thread as running until the returned guard is dropped (or
    /// unwound). Bind it to a named variable — `let _ = ...` would drop it
    /// immediately.
    pub(crate) fn running_guard(&self) -> RunningGuard<'_> {
        RunningGuard::new(&self.running)
    }

    pub(crate) fn observe_buffered(&self, buffered: usize) {
        self.buffered_records.store(buffered, Ordering::Relaxed);
        ::metrics::gauge!("insert_batcher_buffered_records").set(buffered as f64);
    }
}

/// RAII liveness flag: sets `running = true` on creation and `false` on drop,
/// covering normal returns, error propagation (`?`) and panics alike.
pub(crate) struct RunningGuard<'a>(&'a AtomicBool);

impl<'a> RunningGuard<'a> {
    fn new(flag: &'a AtomicBool) -> Self {
        flag.store(true, Ordering::Relaxed);
        Self(flag)
    }
}

impl Drop for RunningGuard<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Relaxed);
    }
}

/// Cheap, cloneable probe over pipeline liveness and progress evidence.
///
/// Holds only `Arc`s to the threads' stat structs. `sample()` reads the
/// writer/batcher atomics without blocking and additionally takes the commit
/// ledger's short-lived mutex for ticket progress; it never touches SQLite or
/// waits on pipeline work, so it stays suitable for a watchdog thread even
/// while the observed components are blocked or wedged.
#[derive(Clone, Debug)]
pub struct StorageHealth {
    pub(crate) writer: std::sync::Arc<WriterStats>,
    pub(crate) batcher: std::sync::Arc<BatcherStats>,
    pub(crate) ledger: std::sync::Arc<CommitLedger>,
    pub(crate) queue_capacity: usize,
}

impl StorageHealth {
    /// Reads one consistent-enough point-in-time sample of health evidence.
    ///
    /// Fields are individually atomic reads (relaxed ordering) plus two
    /// short ledger lock acquisitions; they are diagnostics for a watchdog
    /// evaluator, not a transactional snapshot. No I/O and no blocking.
    pub fn sample(&self) -> StorageHealthSample {
        let now = monotonic_millis();
        let maintenance_started_ms = self.writer.maintenance_started_ms.load(Ordering::Relaxed);
        StorageHealthSample {
            writer_running: self.writer.running.load(Ordering::Relaxed),
            batcher_running: self.batcher.running.load(Ordering::Relaxed),
            writer_idle_ms: now
                .saturating_sub(self.writer.last_progress_ms.load(Ordering::Relaxed)),
            batcher_idle_ms: now
                .saturating_sub(self.batcher.last_progress_ms.load(Ordering::Relaxed)),
            queue_depth: self
                .writer
                .queue_depth
                .load(Ordering::Relaxed)
                .max(self.batcher.queued_commands.load(Ordering::Relaxed)),
            queue_capacity: self.queue_capacity,
            buffered_records: self.batcher.buffered_records.load(Ordering::Relaxed),
            outstanding_commit_tickets: self.ledger.outstanding_tickets(),
            watermark_idle_ms: now.saturating_sub(self.ledger.last_advance_ms()),
            maintenance_in_progress: self.writer.maintenance_in_progress.load(Ordering::Relaxed),
            maintenance_elapsed_ms: if maintenance_started_ms == 0 {
                0
            } else {
                now.saturating_sub(maintenance_started_ms)
            },
        }
    }
}

/// Point-in-time health evidence for the storage pipeline.
///
/// The two "idle" fields are ages: milliseconds since the component last made
/// meaningful progress. Stale ages are *not* failures by themselves — with an
/// empty pipeline they simply mean no work arrived (`NO WORK`, not
/// `WORK BUT NO PROGRESS`). Evaluators must combine them with the pending-work
/// counters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StorageHealthSample {
    /// Whether the SQLite writer thread is still running.
    pub writer_running: bool,
    /// Whether the insert batcher thread is still running.
    pub batcher_running: bool,
    /// Milliseconds since the writer last executed a command successfully.
    pub writer_idle_ms: u64,
    /// Milliseconds since the batcher last received input or submitted a batch.
    pub batcher_idle_ms: u64,
    /// Depth of the bounded write-command queue (writer's own view).
    pub queue_depth: usize,
    /// Capacity of the bounded write-command queue; `0` if unknown.
    pub queue_capacity: usize,
    /// Records currently buffered inside the insert batcher.
    pub buffered_records: usize,
    /// Issued durability tickets that have not settled (completed or voided).
    /// In-flight records account for some; a ticket that stays outstanding
    /// while nothing is in flight is a leak that stalls every durable ack.
    pub outstanding_commit_tickets: u64,
    /// Milliseconds since the contiguous commit watermark last advanced.
    /// Combined with `outstanding_commit_tickets` this detects a stuck
    /// durability watermark.
    pub watermark_idle_ms: u64,
    /// Whether a maintenance-class command is currently executing on the
    /// writer. Ordinary stall verdicts must be suspended while this is true.
    pub maintenance_in_progress: bool,
    /// Milliseconds since the current maintenance operation started; `0`
    /// when none is in progress. Bounds a genuinely hung maintenance call.
    pub maintenance_elapsed_ms: u64,
}

/// Point-in-time view of every pipeline counter, produced by
/// [`crate::Storage::stats`].
#[derive(Debug, Clone, Copy, Default)]
pub struct StorageStatsSnapshot {
    pub records_written: u64,
    pub transactions_committed: u64,
    pub batches_received: u64,
    pub maintenance_runs: u64,
    pub errors: u64,
    /// Rows dropped by salvage passes (see [`WriterStats::quarantined_records`]).
    pub quarantined_records: u64,
    /// Retry attempts spent on transient contention
    /// (see [`WriterStats::transient_retries`]).
    pub transient_retries: u64,
    pub queue_depth: usize,
    /// Mapped ingest chunks accepted by the insert batcher.
    pub chunks_ingested: u64,
    /// Storage-sized write batches produced by the insert batcher.
    pub write_batches_emitted: u64,
    /// Partial batches flushed because `max_batch_age` expired.
    pub timer_flushes: u64,
    /// Records lost when the command queue closed before submission could
    /// finish (writer failure); zero during graceful operation.
    pub dropped_records: u64,
}
