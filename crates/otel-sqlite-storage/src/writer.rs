use std::time::{Duration, Instant};

use crossbeam_channel::{Receiver, Sender};
use otel_sqlite_core::storage::{CheckpointMode, CommitLedger, MaintenanceOperation, WriteCommand};
use otel_sqlite_core::unix_nano_now;
use rusqlite::Connection;
use std::sync::atomic::Ordering;

use crate::LedgerCloseOnDrop;
use crate::StorageConfig;
use crate::command;
use crate::error::{FailureClass, StorageError};
use crate::fault;
use crate::maintenance;
use crate::migration;
use crate::sqlite;
use crate::stats::WriterStats;

/// Single SQLite writer.
///
/// The writer performs no record accumulation of its own: every insert
/// command it receives already carries a storage-sized [`WriteBatch`] built by
/// the insert batcher, and each one is persisted in exactly one transaction.
/// When the queue is empty the thread blocks inside `recv()` — there is no
/// polling and no timeout-driven grouping left at this layer. Closing the
/// channel (all senders dropped) drains the queued commands in order and then
/// shuts the writer down cleanly.
///
/// After every successful insert transaction the writer publishes the
/// command's durability ticket on `ledger`; that is the exact moment durable
/// acks in `durability = "commit"` mode may be released. On any exit path the
/// ledger is closed so pending waiters fail instead of hanging.
///
/// `ready_tx` receives exactly one result once bootstrap (open + migrations)
/// finished — `Ok` when storage is usable, otherwise an error message — so
/// [`crate::Storage::open`] can block until storage is genuinely ready and
/// fail fast at boot otherwise.
pub(crate) fn run(
    receiver: Receiver<WriteCommand>,
    config: StorageConfig,
    ledger: &CommitLedger,
    stats: &WriterStats,
    ready_tx: Sender<Result<(), String>>,
) -> Result<(), StorageError> {
    // Closes on every exit path *including unwinding*: a panic must fail
    // durable-ack waiters instead of leaving them hanging on a dead writer.
    // `CommitLedger::close` is idempotent, so the normal path is unaffected.
    let _close_ledger = LedgerCloseOnDrop::new(ledger);
    run_inner(receiver, config, ledger, stats, ready_tx)
}

/// Opens the database and applies migrations. Kept separate so the ready
/// signal in [`run`] wraps exactly this step.
fn bootstrap(config: &StorageConfig) -> Result<Connection, StorageError> {
    let mut conn = sqlite::open(&config.sqlite_path, config.synchronous)?;
    migration::migrate(&mut conn)?;
    Ok(conn)
}

fn run_inner(
    receiver: Receiver<WriteCommand>,
    config: StorageConfig,
    ledger: &CommitLedger,
    stats: &WriterStats,
    ready_tx: Sender<Result<(), String>>,
) -> Result<(), StorageError> {
    // Cleared on every exit path (including panics) so the watchdog can tell
    // a dead writer thread from a merely idle one.
    let _running = stats.running_guard();

    let mut conn = match bootstrap(&config) {
        Ok(conn) => {
            let _ = ready_tx.send(Ok(()));
            conn
        }
        Err(error) => {
            tracing::error!(%error, "storage bootstrap failed");
            let _ = ready_tx.send(Err(error.to_string()));
            return Err(error);
        }
    };
    maintenance::checkpoint(&conn, CheckpointMode::Truncate)?;
    // Sidecars materialize on first write, possibly after `sqlite::open`
    // hardened the paths — repeat here now that they are known to exist.
    crate::permissions::harden_database_files(&config.sqlite_path);
    tracing::info!(
        path = %config.sqlite_path.display(),
        synchronous = config.synchronous.as_str(),
        "storage writer ready (wal mode)"
    );
    stats.mark_progress();

    let fault_after = fault::armed_after(fault::WRITER_AFTER_N);
    let mut commands_processed = 0u64;

    loop {
        let depth = receiver.len();
        stats.queue_depth.store(depth, Ordering::Relaxed);
        ::metrics::gauge!("storage_queue_depth").set(depth as f64);

        match receiver.recv() {
            Ok(command) => {
                ::metrics::counter!("storage_commands_total", "kind" => command.kind())
                    .increment(1);
                // Depth *after* dequeue approximates the backlog that heavy
                // maintenance would stall behind it.
                let backlog = receiver.len();
                // Publish the authoritative backlog plus the command now
                // executing. The top-of-loop depth alone would hide a wedged
                // in-flight command whose queue is already empty.
                let _in_flight = stats.begin_command(backlog);
                // Size quota precedes the insert so an over-budget database
                // evicts its oldest rows before accepting new ones.
                if command.record_count() != 0
                    && let Some(quota) = config.max_db_bytes
                {
                    enforce_quota(&mut conn, &config.sqlite_path, quota, stats);
                }
                execute(&mut conn, command, ledger, stats, backlog)?;
                commands_processed += 1;
                if let Some(limit) = fault_after
                    && commands_processed >= limit
                {
                    tracing::error!(
                        limit,
                        "fault injection: sqlite writer exiting after {limit} commands"
                    );
                    return Err(StorageError::FaultInjected {
                        component: "writer",
                    });
                }
            }
            Err(crossbeam_channel::RecvError) => break,
        }
    }

    stats.queue_depth.store(0, Ordering::Relaxed);
    ::metrics::gauge!("storage_queue_depth").set(0.0);
    maintenance::checkpoint(&conn, CheckpointMode::Truncate)?;
    tracing::info!(
        records = stats.records_written.load(Ordering::Relaxed),
        transactions = stats.transactions_committed.load(Ordering::Relaxed),
        "storage writer stopped"
    );

    Ok(())
}

fn execute(
    conn: &mut Connection,
    command: WriteCommand,
    ledger: &CommitLedger,
    stats: &WriterStats,
    queue_backlog: usize,
) -> Result<(), StorageError> {
    match command {
        WriteCommand::InsertLogs(batch) => {
            if batch.records.is_empty() {
                // Defensive: nothing persisted, but the tickets must not
                // stall the watermark either.
                for commit_seq in &batch.commit_seqs {
                    ledger.void(*commit_seq);
                }
                return Ok(());
            }
            stats.batches_received.fetch_add(1, Ordering::Relaxed);
            let salvage = persist_with_policy(
                conn,
                stats,
                "logs",
                |tx| command::insert_logs(tx, &batch),
                |tx| command::insert_logs_tolerant(tx, &batch),
            )?;
            settle_log_batch(ledger, &batch, salvage);
        }
        WriteCommand::Flush => {}
        WriteCommand::Checkpoint(mode) => {
            let _maintenance = stats.maintenance_guard();
            maintenance::checkpoint(conn, mode)?;
            stats.maintenance_runs.fetch_add(1, Ordering::Relaxed);
            stats.mark_progress();
        }
        WriteCommand::Maintenance(operation) => {
            let _maintenance = stats.maintenance_guard();
            if run_maintenance(conn, operation, queue_backlog)? {
                stats.maintenance_runs.fetch_add(1, Ordering::Relaxed);
                stats.mark_progress();
            }
        }
    }
    Ok(())
}

/// Publishes a persisted batch to the commit ledger.
///
/// A ticket settles only when the batch contains its final segment. Records
/// written by an earlier segment are reported as progress, so a close midway
/// through a split chunk can still tell the handler how much is durable.
fn settle_log_batch(
    ledger: &CommitLedger,
    batch: &otel_sqlite_core::storage::LogWriteBatch,
    salvage: Option<command::SalvageOutcome>,
) {
    if batch.segments.is_empty() {
        // Legacy/bench batches carry no per-record ticket map. Treat every
        // referenced ticket as complete; a salvage drop fails all of them.
        match salvage {
            None => {
                for commit_seq in &batch.commit_seqs {
                    ledger.complete(*commit_seq);
                }
            }
            Some(outcome) => {
                for commit_seq in &batch.commit_seqs {
                    if outcome.dropped > 0 {
                        ledger.note_dropped(*commit_seq, outcome.dropped);
                    }
                    ledger.complete(*commit_seq);
                }
            }
        }
        return;
    }

    match salvage {
        None => {
            for segment in &batch.segments {
                ledger.note_committed(segment.commit_seq, segment.records as u64);
                if segment.complete {
                    ledger.complete(segment.commit_seq);
                }
            }
        }
        Some(outcome) => {
            // `by_ticket` is already summed for the whole batch, so note it
            // once per ticket rather than once per segment.
            for commit_seq in &batch.commit_seqs {
                let (kept, dropped) = outcome.by_ticket.get(commit_seq).copied().unwrap_or((0, 0));
                if kept > 0 {
                    ledger.note_committed(*commit_seq, kept);
                }
                if dropped > 0 {
                    ledger.note_dropped(*commit_seq, dropped);
                }
            }
            for segment in &batch.segments {
                if segment.complete {
                    ledger.complete(segment.commit_seq);
                }
            }
        }
    }
}

/// How often a [`FailureClass::Retryable`] transaction is re-attempted
/// before its error is treated as fatal. Backoff grows exponentially from
/// [`RETRY_BACKOFF_BASE`].
const MAX_TRANSIENT_RETRIES: u32 = 4;
const RETRY_BACKOFF_BASE: Duration = Duration::from_millis(250);

fn backoff_delay(attempt: u32) -> Duration {
    // attempt 0..MAX-1 -> 250ms, 500ms, 1s, 2s (capped at 4s for safety).
    RETRY_BACKOFF_BASE
        .saturating_mul(1u32 << attempt.min(4))
        .min(Duration::from_secs(4))
}

/// Persistence policy for one write batch (= one transaction).
///
/// * strict pass first; on success the batch is done.
/// * **Poison** errors (constraint violations, bad inputs) trigger exactly
///   one salvage pass in tolerant mode: healthy rows commit, offending rows
///   are dropped-and-counted as `quarantined_records`. The affected tickets
///   settle as failed so their handlers report `partial_success`; nothing is
///   lost silently and nothing stalls.
/// * **Retryable** errors (BUSY/LOCKED) are retried up to
///   `MAX_TRANSIENT_RETRIES` times with exponential backoff; exhaustion
///   escalates to fatal.
/// * **Fatal** errors propagate immediately: the writer exits, closes the
///   ledger and lets watchdog/supervisor handle restart.
#[allow(clippy::too_many_arguments)]
fn persist_with_policy(
    conn: &mut Connection,
    stats: &WriterStats,
    signal: &'static str,
    strict: impl Fn(&rusqlite::Transaction<'_>) -> Result<u64, StorageError>,
    salvage: impl Fn(&rusqlite::Transaction<'_>) -> Result<command::SalvageOutcome, StorageError>,
) -> Result<Option<command::SalvageOutcome>, StorageError> {
    let mut attempt = 0u32;
    loop {
        let outcome = run_transaction(conn, stats, &strict);
        match outcome {
            Ok(()) => return Ok(None),
            Err((error, class)) => match class {
                FailureClass::Poison => {
                    tracing::warn!(
                        signal,
                        %error,
                        "poisonous batch rejected by sqlite; attempting salvage pass"
                    );
                    return finish_salvage(conn, stats, signal, salvage, error).map(Some);
                }
                FailureClass::Retryable if attempt < MAX_TRANSIENT_RETRIES => {
                    let delay = backoff_delay(attempt);
                    attempt += 1;
                    stats.transient_retries.fetch_add(1, Ordering::Relaxed);
                    ::metrics::counter!("storage_transient_retries_total").increment(1);
                    tracing::warn!(
                        signal,
                        %error,
                        attempt,
                        ?delay,
                        "transient sqlite failure; retrying batch"
                    );
                    std::thread::sleep(delay);
                }
                FailureClass::Retryable | FailureClass::Fatal => return Err(error),
            },
        }
    }
}

/// Runs one strict transaction, mapping failures to their classification so
/// the policy loop above can decide without re-matching errors.
fn run_transaction(
    conn: &mut Connection,
    stats: &WriterStats,
    persist: impl Fn(&rusqlite::Transaction<'_>) -> Result<u64, StorageError>,
) -> Result<(), (StorageError, FailureClass)> {
    let started = Instant::now();
    let tx = match conn.transaction() {
        Ok(tx) => tx,
        Err(error) => {
            let error = StorageError::from(error);
            let class = error.class();
            return Err((error, class));
        }
    };
    match persist(&tx) {
        Ok(written) => {
            if let Err(error) = tx.commit() {
                let error = StorageError::from(error);
                let class = error.class();
                return Err((error, class));
            }
            stats.records_written.fetch_add(written, Ordering::Relaxed);
            stats.transactions_committed.fetch_add(1, Ordering::Relaxed);
            stats.mark_progress();
            ::metrics::counter!("storage_records_written_total").increment(written);
            ::metrics::histogram!("storage_transaction_batch_size").record(written as f64);
            ::metrics::histogram!("storage_transaction_duration")
                .record(started.elapsed().as_secs_f64());
            tracing::info_span!("storage.commit").in_scope(|| {
                tracing::info!(
                    records = written,
                    duration_us = u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX),
                    "transaction committed"
                );
            });
            Ok(())
        }
        Err(error) => {
            let _ = tx.rollback();
            stats.errors.fetch_add(1, Ordering::Relaxed);
            ::metrics::counter!("storage_errors_total").increment(1);
            let class = error.class();
            tracing::error!(%error, ?class, "transaction failed, rolled back");
            Err((error, class))
        }
    }
}

/// Second-chance pass for poisoned batches: tolerant mode keeps healthy rows
/// and drops-and-counts only the offending ones.
fn finish_salvage(
    conn: &mut Connection,
    stats: &WriterStats,
    signal: &'static str,
    salvage: impl Fn(&rusqlite::Transaction<'_>) -> Result<command::SalvageOutcome, StorageError>,
    cause: StorageError,
) -> Result<command::SalvageOutcome, StorageError> {
    let started = Instant::now();
    let tx = conn.transaction()?;
    match salvage(&tx) {
        Ok(outcome) => {
            tx.commit().map_err(StorageError::from)?;
            stats
                .records_written
                .fetch_add(outcome.kept, Ordering::Relaxed);
            stats
                .quarantined_records
                .fetch_add(outcome.dropped, Ordering::Relaxed);
            stats.transactions_committed.fetch_add(1, Ordering::Relaxed);
            stats.mark_progress();
            ::metrics::counter!("storage_quarantined_records_total").increment(outcome.dropped);
            ::metrics::histogram!("storage_transaction_duration")
                .record(started.elapsed().as_secs_f64());
            tracing::warn!(
                signal,
                kept = outcome.kept,
                dropped = outcome.dropped,
                cause = %cause,
                "salvage pass complete: healthy rows committed, poisonous rows quarantined"
            );
            Ok(outcome)
        }
        Err(salvage_error) => {
            let _ = tx.rollback();
            stats.errors.fetch_add(1, Ordering::Relaxed);
            ::metrics::counter!("storage_errors_total").increment(1);
            tracing::error!(
                signal,
                %salvage_error,
                cause = %cause,
                "salvage pass failed; treating as fatal"
            );
            Err(salvage_error)
        }
    }
}

/// Executes one maintenance command, returning `false` when it was
/// deliberately deferred (backlog pressure) or disabled. Callers only count
/// and timestamp a run that actually happened.
fn run_maintenance(
    conn: &mut Connection,
    operation: MaintenanceOperation,
    queue_backlog: usize,
) -> Result<bool, StorageError> {
    // Heavy operations (full-file VACUUM, full FTS rebuild, large prunes)
    // stall the single writer for seconds on GB databases. When ingestion is
    // waiting, defer them to the next scheduler interval instead of stalling
    // it: the scheduler re-enqueues on its own cadence.
    if queue_backlog > MAINTENANCE_PRESSURE_SKIP_DEPTH
        && let Some(label) = heavy_maintenance_label(&operation)
    {
        ::metrics::counter!(
            "storage_maintenance_deferred_total",
            "operation" => label
        )
        .increment(1);
        tracing::warn!(
            operation = label,
            queue_backlog,
            "deferring heavy maintenance; ingestion backlog present"
        );
        return Ok(false);
    }
    match operation {
        MaintenanceOperation::Analyze => maintenance::analyze(conn)?,
        MaintenanceOperation::Vacuum => maintenance::vacuum(conn)?,
        MaintenanceOperation::RebuildFts => maintenance::rebuild_fts(conn).map(|_| ())?,
        MaintenanceOperation::Prune(window) => {
            if window.is_none() {
                return Ok(false);
            }
            let report = maintenance::prune(conn, window, unix_nano_now())?;
            if report.total() > 0 {
                ::metrics::counter!("storage_records_pruned_total", "table" => "log_event")
                    .increment(report.log_events as u64);
                ::metrics::counter!("storage_dimensions_pruned_total")
                    .increment(report.dimensions_removed() as u64);
                tracing::info!(
                    log_events = report.log_events,
                    orphaned_resources = report.resources,
                    "retention prune applied"
                );
            }
        }
    }
    Ok(true)
}

/// Best-effort size-quota enforcement ahead of an insert batch.
///
/// Deliberately non-fatal: a quota-check failure (unreadable file, locked
/// database) must not reject ingestion — the insert itself surfaces real
/// backend trouble through the normal retry/salvage policy. Enforcement
/// evicts oldest-first so newest telemetry survives an over-budget database.
fn enforce_quota(
    conn: &mut Connection,
    db_path: &std::path::Path,
    quota_bytes: u64,
    stats: &WriterStats,
) {
    let size = crate::maintenance::database_bytes(db_path);
    if size <= quota_bytes {
        return;
    }
    // Only a real eviction is maintenance-class work. Taking the guard around
    // the size probe would make every insert look like maintenance to the
    // watchdog, masking saturation and ack-stall verdicts.
    let _maintenance = stats.maintenance_guard();
    match maintenance::enforce_size_quota(conn, db_path, quota_bytes) {
        Ok(report) => {
            ::metrics::counter!("storage_quota_enforcements_total").increment(1);
            stats.mark_progress();
            tracing::warn!(
                quota_bytes,
                bytes_before = report.bytes_before,
                bytes_after = report.bytes_after,
                log_events = report.log_events,
                "database over size quota; evicted oldest rows",
            );
        }
        Err(error) => {
            tracing::warn!(
                %error,
                "size-quota enforcement failed; proceeding with insert"
            );
        }
    }
}

/// Backlog depth above which heavy maintenance defers to the next scheduler
/// interval instead of stalling ingestion behind a seconds-long exclusive op.
const MAINTENANCE_PRESSURE_SKIP_DEPTH: usize = 1_000;
/// Label of an operation heavy enough to defer under ingestion pressure, or
/// `None` when it is cheap and should always run (`Analyze`, disabled prunes).
fn heavy_maintenance_label(operation: &MaintenanceOperation) -> Option<&'static str> {
    match operation {
        MaintenanceOperation::Vacuum => Some("vacuum"),
        MaintenanceOperation::RebuildFts => Some("rebuild_fts"),
        MaintenanceOperation::Prune(Some(_)) => Some("prune"),
        MaintenanceOperation::Prune(None) | MaintenanceOperation::Analyze => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The writer's only cleanup guarantee on an unwind is this guard: a
    /// panic must fail durable-ack waiters instead of leaving them hanging.
    #[test]
    fn ledger_guard_closes_the_ledger_on_drop() {
        let ledger = CommitLedger::new();
        {
            let _guard = LedgerCloseOnDrop::new(&ledger);
        }
        assert!(ledger.watermark().closed);
    }
}
