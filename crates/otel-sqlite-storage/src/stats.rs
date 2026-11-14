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

use std::mem::DropGuard;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

use otel_sqlite_core::PipelineSample;
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
    /// exactly one write batch; the writer may commit several consecutive
    /// commands in one transaction when grouping is enabled.
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
    /// `1` while the writer is executing a command, `0` while it waits for
    /// work. Counted as pending work so a command wedged in SQLite stays
    /// visible to the watchdog even when the queue behind it is empty.
    pub in_flight: AtomicUsize,
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

    /// Publishes the current command-queue backlog.
    pub(crate) fn observe_queue_depth(&self, depth: usize) {
        self.queue_depth.store(depth, Ordering::Relaxed);
        ::metrics::gauge!("storage_queue_depth").set(depth as f64);
    }

    /// Marks the thread as running until the returned guard is dropped (or
    /// unwound). Bind it to a named variable — `let _ = ...` would drop it
    /// immediately.
    pub(crate) fn running_guard(&self) -> RunningGuard<'_> {
        publish_running(&self.running)
    }

    /// Publishes the authoritative backlog while a command executes: commands
    /// still queued after the dequeue plus the one now in flight. The guard
    /// clears the in-flight marker on every exit path, including errors and
    /// panics.
    pub(crate) fn begin_command(&self, queued_after_dequeue: usize) -> InFlightGuard<'_> {
        self.queue_depth
            .store(queued_after_dequeue, Ordering::Relaxed);
        self.in_flight.store(1, Ordering::Relaxed);
        DropGuard::new(self, |stats: &WriterStats| {
            stats.in_flight.store(0, Ordering::Relaxed);
        })
    }

    /// Publishes "maintenance in progress" until the returned guard is
    /// dropped (or unwound), timestamping the start for stall detection.
    ///
    /// The timestamp is stored before the flag so a sample can never read a
    /// live window with a zero start; the guard clears the flag before zeroing
    /// the timestamp, and the watchdog only consults elapsed time while the
    /// flag is set, so the one torn read stays the benign half.
    pub(crate) fn maintenance_guard(&self) -> MaintenanceGuard<'_> {
        self.maintenance_started_ms
            .store(monotonic_millis(), Ordering::Relaxed);
        self.maintenance_in_progress.store(true, Ordering::Relaxed);
        DropGuard::new(self, |stats: &WriterStats| {
            stats
                .maintenance_in_progress
                .store(false, Ordering::Relaxed);
            stats.maintenance_started_ms.store(0, Ordering::Relaxed);
        })
    }
}

/// RAII in-flight marker for the writer's current command. See
/// [`WriterStats::begin_command`].
///
/// A name for [`DropGuard`], not a type of its own: the retraction is the `fn`
/// pointer in the alias, so every guard `begin_command` hands back withdraws
/// the claim the same way. A destructor that had to capture state instead of
/// receiving `&WriterStats` would make this alias unnameable, which is why the
/// guard carries the stats by reference.
pub(crate) type InFlightGuard<'a> = DropGuard<&'a WriterStats, fn(&WriterStats)>;

/// RAII maintenance flag: set on creation, cleared (with the start timestamp)
/// on drop, covering normal returns, `?` and panics alike.
///
/// A name for [`DropGuard`]; see [`InFlightGuard`] for why the alias exists.
pub(crate) type MaintenanceGuard<'a> = DropGuard<&'a WriterStats, fn(&WriterStats)>;

/// RAII liveness flag: `running = true` while the guard lives and `false` once
/// it is dropped, covering normal returns, error propagation (`?`) and panics
/// alike.
///
/// A name for [`DropGuard`]. The arm/clear pair itself lives in
/// [`publish_running`], so both stats structs share one implementation.
pub(crate) type RunningGuard<'a> = DropGuard<&'a AtomicBool, fn(&AtomicBool)>;

/// Arms a liveness flag and returns the guard that clears it on drop.
///
/// The one place the set-true/clear-on-drop pair is written, for
/// [`WriterStats`] and [`BatcherStats`] alike.
fn publish_running(flag: &AtomicBool) -> RunningGuard<'_> {
    flag.store(true, Ordering::Relaxed);
    DropGuard::new(flag, |running: &AtomicBool| {
        running.store(false, Ordering::Relaxed);
    })
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
    /// Chunks still waiting in the ingress→batcher channel.
    pub ingest_queue_depth: AtomicUsize,
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
        publish_running(&self.running)
    }

    pub(crate) fn observe_buffered(&self, buffered: usize) {
        self.buffered_records.store(buffered, Ordering::Relaxed);
        ::metrics::gauge!("insert_batcher_buffered_records").set(buffered as f64);
    }

    /// Publishes the ingress-channel backlog sampled by the batcher loop.
    pub(crate) fn observe_ingest_queue(&self, depth: usize) {
        self.ingest_queue_depth.store(depth, Ordering::Relaxed);
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
    pub fn sample(&self) -> PipelineSample {
        let now = monotonic_millis();
        let maintenance_started_ms = self.writer.maintenance_started_ms.load(Ordering::Relaxed);
        PipelineSample {
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
                .saturating_add(self.writer.in_flight.load(Ordering::Relaxed)),
            queue_capacity: self.queue_capacity,
            pending_records: self.batcher.buffered_records.load(Ordering::Relaxed),
            ingest_queue_depth: self.batcher.ingest_queue_depth.load(Ordering::Relaxed),
            outstanding_tickets: self.ledger.outstanding_tickets(),
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
