// Unit tests may assert invariants with unwrap(); production code must not.
#![cfg_attr(test, allow(clippy::unwrap_used))]

pub mod backup;
mod batcher;
mod command;
pub mod config;
mod error;
mod fault;
mod maintenance;
mod migration;
mod origin_buffers;
pub(crate) mod permissions;
mod sqlite;
mod stats;
mod writer;

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crossbeam_channel::{Receiver, Sender};
use otel_sqlite_core::storage::{CommitLedger, IngestMessage, InsertBatcherConfig, WriteCommand};

pub use config::{
    DEFAULT_COMMAND_QUEUE_CAPACITY, DEFAULT_SHUTDOWN_TIMEOUT, DEFAULT_STARTUP_TIMEOUT,
    StorageConfig,
};
pub use error::StorageError;
/// Applies only the migrations up to and including `target_version`, stamping
/// `schema_migrations` exactly as the production path would. Exposed for the
/// upgrade tests, which build databases stamped at every historical schema
/// version and then let [`Storage::open`] migrate them to the current schema.
#[doc(hidden)]
pub use migration::migrate_up_to;
// Convenience re-export: consumers wire the same ledger into ingress.
pub use otel_sqlite_core::storage::Watermark;
pub use stats::{BatcherStats, StorageHealth, StorageHealthSample, StorageStatsSnapshot};

#[derive(Debug)]
pub struct Storage {
    stats: Arc<stats::WriterStats>,
    batcher_stats: Arc<stats::BatcherStats>,
    /// Durability tickets: ingress issues/voids them, the writer completes
    /// them after each transaction commits. Shared with the OTLP handlers
    /// via [`Storage::commit_ledger`] so acks can wait on real persistence.
    ledger: Arc<CommitLedger>,
    batcher_handle: Option<JoinHandle<()>>,
    join_handle: Option<JoinHandle<Result<(), StorageError>>>,
    /// Capacity of the bounded command queue, reported through
    /// [`Storage::health`] so consumers can judge saturation pressure.
    command_queue_capacity: usize,
    /// Our own reference to the bounded command queue, used only to derive
    /// additional producer handles via [`Storage::producer`]. Released by
    /// `join()` so the queue can close once every external producer stopped.
    commands: Option<Sender<WriteCommand>>,
}

impl Storage {
    /// Starts the storage pipeline:
    ///
    /// ```text
    /// Receiver<IngestMessage> ──▶ insert batcher thread
    ///                                 │  bounded WriteCommand queue
    ///                                 ▼
    ///                             SQLite writer thread ──▶ SQLite
    /// ```
    ///
    /// The insert batcher is owned by the storage layer: it converts the
    /// arbitrary OTLP batch boundaries produced by ingress into storage-sized
    /// write batches (capacity + maximum age), which the single writer then
    /// persists one transaction at a time.
    ///
    /// **Storage-ready guarantee:** this call blocks until the writer thread
    /// finished bootstrapping (database opened, schema migrated). A broken
    /// backend therefore surfaces as `Err` at boot instead of on first
    /// traffic; on timeout or bootstrap failure no caller can observe a
    /// half-started pipeline.
    pub fn open(
        input: Receiver<IngestMessage>,
        config: StorageConfig,
    ) -> Result<Self, StorageError> {
        let stats = Arc::new(stats::WriterStats::new());
        let batcher_stats = Arc::new(stats::BatcherStats::new());
        let ledger = Arc::new(CommitLedger::new());
        let worker_stats = Arc::clone(&stats);
        let worker_batcher_stats = Arc::clone(&batcher_stats);
        let batcher_ledger = Arc::clone(&ledger);
        let writer_ledger = Arc::clone(&ledger);

        let (commands, receiver) =
            crossbeam_channel::bounded::<WriteCommand>(config.command_queue_capacity.max(1));
        // Kept solely for `producer()`; dropped again by `join()` so the
        // graceful shutdown path (queue closes -> writer drains) still works.
        let producer_source = commands.clone();
        let command_queue_capacity = commands
            .capacity()
            .expect("bounded command queue has capacity");
        let startup_timeout = config.startup_timeout;

        // The writer reports readiness once its database is open and the
        // schema is migrated; `open` waits for exactly that signal.
        let (ready_tx, ready_rx) = crossbeam_channel::bounded::<Result<(), String>>(1);

        let batcher_config: InsertBatcherConfig = config.insert_batcher;
        let batcher_handle = std::thread::Builder::new()
            .name("otel-sqlite-batcher".to_owned())
            .spawn(move || {
                batcher::run(
                    input,
                    commands,
                    &batcher_ledger,
                    batcher_config,
                    &worker_batcher_stats,
                );
            })?;

        let join_handle = std::thread::Builder::new()
            .name("otel-sqlite-writer".to_owned())
            .spawn(move || {
                writer::run(receiver, config, &writer_ledger, &worker_stats, ready_tx)
            })?;

        match ready_rx.recv_timeout(startup_timeout) {
            Ok(Ok(())) => {}
            Ok(Err(message)) => return Err(StorageError::StartupFailed { message }),
            Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                return Err(StorageError::StartupTimeout {
                    timeout_secs: startup_timeout.as_secs(),
                });
            }
            Err(crossbeam_channel::RecvTimeoutError::Disconnected) => {
                return Err(StorageError::WriterPanicked);
            }
        }

        // Publish liveness eagerly so health probes are correct the moment
        // `open` returns; the in-thread guards remain authoritative and clear
        // these flags on every exit path.
        stats.running.store(true, Ordering::Relaxed);
        batcher_stats.running.store(true, Ordering::Relaxed);

        Ok(Self {
            stats,
            batcher_stats,
            ledger,
            batcher_handle: Some(batcher_handle),
            join_handle: Some(join_handle),
            command_queue_capacity,
            commands: Some(producer_source),
        })
    }

    /// Returns an additional producer handle to the internal bounded
    /// `WriteCommand` queue.
    ///
    /// The returned sender feeds the *same* single SQLite writer as ingestion;
    /// it never creates a second connection or consumer. This is how the
    /// maintenance worker (`otel-sqlite-runtime`) becomes just another
    /// producer of the existing command queue.
    ///
    /// Every handle obtained here must be dropped (its owner stopped) before
    /// [`Storage::join`] is called; otherwise the queue cannot close and the
    /// writer would wait forever for the graceful drain.
    pub fn producer(&self) -> Sender<WriteCommand> {
        self.commands
            .as_ref()
            .expect("storage already joined")
            .clone()
    }

    pub fn stats(&self) -> StorageStatsSnapshot {
        let stats = &self.stats;
        let batcher = &self.batcher_stats;
        StorageStatsSnapshot {
            records_written: stats.records_written.load(Ordering::Relaxed),
            transactions_committed: stats.transactions_committed.load(Ordering::Relaxed),
            batches_received: stats.batches_received.load(Ordering::Relaxed),
            maintenance_runs: stats.maintenance_runs.load(Ordering::Relaxed),
            errors: stats.errors.load(Ordering::Relaxed),
            quarantined_records: stats.quarantined_records.load(Ordering::Relaxed),
            transient_retries: stats.transient_retries.load(Ordering::Relaxed),
            queue_depth: stats.queue_depth.load(Ordering::Relaxed),
            chunks_ingested: batcher.chunks_ingested.load(Ordering::Relaxed),
            write_batches_emitted: batcher.batches_emitted.load(Ordering::Relaxed),
            timer_flushes: batcher.timer_flushes.load(Ordering::Relaxed),
            dropped_records: batcher.dropped_records.load(Ordering::Relaxed),
        }
    }

    /// Returns a cheap, cloneable health probe over the pipeline threads.
    ///
    /// The probe only reads atomics owned by the writer and batcher threads —
    /// it never locks, blocks, or touches SQLite — so it is safe to poll from
    /// a watchdog even while the pipeline is wedged. This is the
    /// "components publish evidence, observers read it" direction; nothing
    /// here asks the components a question they must answer.
    pub fn health(&self) -> StorageHealth {
        StorageHealth {
            writer: Arc::clone(&self.stats),
            batcher: Arc::clone(&self.batcher_stats),
            ledger: Arc::clone(&self.ledger),
            queue_capacity: self.command_queue_capacity,
        }
    }

    /// Returns the durability-ticket ledger shared with this pipeline.
    ///
    /// Ingress stamps accepted chunks with tickets ([`CommitLedger::issue`])
    /// and awaits [`CommitLedger::committed`] before acknowledging in
    /// `durability = "commit"` mode. The writer publishes progress after each
    /// transaction and closes the ledger when the pipeline dies, failing all
    /// pending waiters immediately.
    pub fn commit_ledger(&self) -> Arc<CommitLedger> {
        Arc::clone(&self.ledger)
    }

    /// Deterministic shutdown: waits for the batcher to flush and close the
    /// command queue, then for the writer to drain pending commands, commit
    /// their transactions and exit.
    ///
    /// Waits indefinitely; see [`Storage::join_timeout`] for a bounded
    /// variant. Our own queue reference is released first; every handle
    /// handed out by [`Storage::producer`] must already be dropped by its
    /// owner (e.g. a stopped maintenance worker), otherwise the queue cannot
    /// close.
    pub fn join(&mut self) -> Result<(), StorageError> {
        self.join_inner(None)
    }

    /// Bounded shutdown: identical to [`Storage::join`], but gives up once
    /// `timeout` has elapsed without both threads finishing.
    ///
    /// The typical reason for a timeout is a wedged SQLite backend (stalled
    /// I/O, an oversized final checkpoint). On timeout the thread handles
    /// stay in `self`, so a later [`Storage::join`] can still reap them;
    /// production callers should instead treat the process as unsafe to
    /// reuse and exit so an external supervisor can restart it.
    pub fn join_timeout(&mut self, timeout: Duration) -> Result<(), StorageError> {
        self.join_inner(Some(timeout))
    }

    fn join_inner(&mut self, timeout: Option<Duration>) -> Result<(), StorageError> {
        let budget = timeout.map(|timeout| (Instant::now() + timeout, timeout));
        // Release our own queue reference first; every handle from
        // `producer()` must already be dropped by its owner, otherwise the
        // queue cannot close and the writer would wait forever.
        self.commands = None;
        wait_finished(self.batcher_handle.as_ref(), budget)?;
        if let Some(handle) = self.batcher_handle.take()
            && handle.join().is_err()
        {
            return Err(StorageError::BatcherPanicked);
        }
        wait_finished(self.join_handle.as_ref(), budget)?;
        if let Some(handle) = self.join_handle.take() {
            match handle.join() {
                Ok(result) => result?,
                Err(_) => return Err(StorageError::WriterPanicked),
            }
        }
        Ok(())
    }
}

/// Poll interval for deadline-bounded joins. Threads publish no completion
/// signal beyond their exit, so finishing is observed by polling
/// [`JoinHandle::is_finished`]; 5 ms bounds the added shutdown latency far
/// below any meaningful timeout.
const JOIN_POLL_INTERVAL: Duration = Duration::from_millis(5);

fn wait_finished<T>(
    handle: Option<&JoinHandle<T>>,
    budget: Option<(Instant, Duration)>,
) -> Result<(), StorageError> {
    let Some(handle) = handle else {
        return Ok(());
    };
    while !handle.is_finished() {
        if let Some((deadline, timeout)) = budget
            && Instant::now() >= deadline
        {
            return Err(StorageError::ShutdownTimeout {
                timeout_secs: timeout.as_secs(),
            });
        }
        std::thread::sleep(JOIN_POLL_INTERVAL);
    }
    Ok(())
}
