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
use otel_sqlite_core::saturating_deadline;
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
pub use permissions::{restrict_permissions, warn_if_world_readable};
pub use stats::{BatcherStats, StorageHealth, StorageHealthSample, StorageStatsSnapshot};

/// Closes the [`CommitLedger`] when dropped, so durable-ack waiters fail fast
/// instead of hanging on tickets that can never commit. Covers every exit path
/// including unwinding, because [`CommitLedger::close`] is idempotent;
/// [`disarm`](LedgerCloseOnDrop::disarm) hands the close to whichever pipeline
/// thread outlives this one.
pub(crate) struct LedgerCloseOnDrop<'a> {
    ledger: Option<&'a CommitLedger>,
}

impl<'a> LedgerCloseOnDrop<'a> {
    pub(crate) const fn new(ledger: &'a CommitLedger) -> Self {
        Self {
            ledger: Some(ledger),
        }
    }

    pub(crate) fn disarm(&mut self) {
        self.ledger = None;
    }
}

impl Drop for LedgerCloseOnDrop<'_> {
    fn drop(&mut self) {
        if let Some(ledger) = self.ledger {
            ledger.close();
        }
    }
}

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
    /// finished bootstrapping (database opened, schema migrated) and the
    /// insert batcher publishes its liveness guard. A broken backend
    /// therefore surfaces as `Err` at boot instead of on first traffic. On
    /// failure nothing is detached: the input receiver never left this call,
    /// so dropping it disconnects ingress, and the writer is reaped with a
    /// short budget before the error returns.
    pub fn open(
        input: Receiver<IngestMessage>,
        config: StorageConfig,
    ) -> Result<Self, StorageError> {
        // `InsertBatcher::new` asserts this; validating here turns a dead
        // batcher thread after a successful `open` into a startup error.
        if config.insert_batcher.max_batch_records == 0 {
            return Err(StorageError::StartupFailed {
                message: "insert_batcher.max_batch_records must be at least 1".to_owned(),
            });
        }

        let batcher_config: InsertBatcherConfig = config.insert_batcher;
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

        // Bootstrap the writer first. Only then is the batcher started, so a
        // failed startup never leaves a detached batcher holding the input
        // receiver: until the batcher spawns, `input` stays here and drops
        // with the error, disconnecting every ingress sender.
        let join_handle = std::thread::Builder::new()
            .name("otel-sqlite-writer".to_owned())
            .spawn(move || {
                writer::run(receiver, config, &writer_ledger, &worker_stats, ready_tx)
            })?;

        match ready_rx.recv_timeout(startup_timeout) {
            Ok(Ok(())) => {}
            Ok(Err(message)) => {
                drop(commands);
                drop(producer_source);
                reap_startup(join_handle);
                return Err(StorageError::StartupFailed { message });
            }
            Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                drop(commands);
                drop(producer_source);
                reap_startup(join_handle);
                return Err(StorageError::StartupTimeout {
                    timeout_secs: startup_timeout.as_secs(),
                });
            }
            Err(crossbeam_channel::RecvTimeoutError::Disconnected) => {
                drop(commands);
                drop(producer_source);
                reap_startup(join_handle);
                return Err(StorageError::WriterPanicked);
            }
        }

        // Wait for the batcher's liveness guard so `open` never returns with
        // a not-yet-running (or already dead) batcher.
        let (batcher_ready_tx, batcher_ready_rx) = crossbeam_channel::bounded::<()>(1);
        let batcher_handle = match std::thread::Builder::new()
            .name("otel-sqlite-batcher".to_owned())
            .spawn(move || {
                batcher::run(
                    input,
                    commands,
                    &batcher_ledger,
                    batcher_config,
                    &worker_batcher_stats,
                    batcher_ready_tx,
                );
            }) {
            Ok(handle) => handle,
            Err(error) => {
                drop(producer_source);
                reap_startup(join_handle);
                return Err(error.into());
            }
        };
        match batcher_ready_rx.recv_timeout(startup_timeout) {
            Ok(()) => {}
            Err(crossbeam_channel::RecvTimeoutError::Disconnected) => {
                drop(producer_source);
                reap_startup(join_handle);
                return Err(StorageError::BatcherPanicked);
            }
            Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                drop(producer_source);
                reap_startup(join_handle);
                return Err(StorageError::StartupTimeout {
                    timeout_secs: startup_timeout.as_secs(),
                });
            }
        }

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
    /// The probe reads atomics owned by the writer and batcher threads and
    /// takes the commit ledger's short-lived mutex for ticket progress — it
    /// never blocks on pipeline work or touches SQLite — so it is safe to
    /// poll from a watchdog even while the pipeline is wedged. This is the
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
        let budget = timeout.map(|timeout| (saturating_deadline(Instant::now(), timeout), timeout));
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

/// Reap budget on a failed startup: a thread already exiting finishes well
/// inside it; a thread wedged in a blocked SQLite call is detached, which is
/// unavoidable because there is no portable way to cancel it.
const STARTUP_REAP_BUDGET: Duration = Duration::from_secs(2);

/// Waits briefly for a thread exiting after a startup failure, then joins it.
fn reap_startup<T>(handle: JoinHandle<T>) {
    let deadline = saturating_deadline(Instant::now(), STARTUP_REAP_BUDGET);
    while !handle.is_finished() && Instant::now() < deadline {
        std::thread::sleep(JOIN_POLL_INTERVAL);
    }
    if handle.is_finished() {
        let _ = handle.join();
    }
}

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
