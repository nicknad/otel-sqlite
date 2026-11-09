//! The single SQLite writer.
//!
//! The writer performs no record accumulation of its own: every insert
//! command it receives already carries a storage-sized [`WriteBatch`] built by
//! the insert batcher. Consecutive insert commands already sitting in the
//! bounded command queue are coalesced into one transaction up to
//! `group_commit_max_records`; collection never waits to fill a group.
//!
//! When the queue is empty the thread blocks inside `recv()` — there is no
//! polling. While a search-index rebuild is pending it uses a short
//! `recv_timeout` instead, so the derived index makes progress without a
//! second writer.
//!
//! After every successful transaction the writer publishes the command's
//! durability tickets on `ledger`; that is the exact moment durable acks in
//! `durability = "commit"` mode may be released. On any exit path the ledger is
//! closed so pending waiters fail instead of hanging.
//!
//! `ready_tx` receives exactly one result once bootstrap finished — open,
//! migrations, search-index reconciliation, the startup checkpoint and
//! permission hardening — `Ok` when storage is usable, otherwise an error
//! message. [`crate::Storage::open`] therefore blocks until storage is
//! genuinely ready and fails fast at boot instead of surfacing a startup
//! failure on first traffic or shutdown.

use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use crossbeam_channel::{Receiver, RecvTimeoutError, Sender, TryRecvError};
use otel_sqlite_core::storage::{CheckpointMode, CommitLedger, MaintenanceOperation, WriteCommand};
use otel_sqlite_core::unix_nano_now;
use rusqlite::Connection;

use crate::LedgerCloseOnDrop;
use crate::StorageConfig;
use crate::command::InsertScratch;
use crate::error::StorageError;
use crate::fault;
use crate::maintenance;
use crate::migration;
use crate::search::{RebuildStep, SearchIndex};
use crate::sqlite;
use crate::stats::WriterStats;
use crate::transaction_group::{self, Pull, collect_group};

/// Search-index work done while the command queue is idle.
const SEARCH_REBUILD_IDLE_ROWS: usize = 2_000;
/// Search-index work done periodically under sustained ingestion.
const SEARCH_REBUILD_UNDER_LOAD_ROWS: usize = 500;
/// Slowest cadence for an under-load rebuild step.
const SEARCH_REBUILD_UNDER_LOAD_INTERVAL: Duration = Duration::from_secs(1);
/// Wake-up interval while a rebuild is pending and the queue is empty.
const SEARCH_REBUILD_TICK: Duration = Duration::from_millis(10);

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

/// Prepares storage for traffic: open, migrate, reconcile the derived search
/// index, fold the initial WAL and harden the files.
fn bootstrap(config: &StorageConfig) -> Result<(Connection, SearchIndex), StorageError> {
    let mut conn = sqlite::open(&config.sqlite_path, config.synchronous)?;
    migration::migrate(&mut conn)?;
    let search = SearchIndex::reconcile(&mut conn, config.fts_mode)?;
    maintenance::checkpoint(&conn, CheckpointMode::Truncate)?;
    // Sidecars materialize on first write, possibly after `sqlite::open`
    // hardened the paths — repeat here now that they are known to exist.
    crate::permissions::harden_database_files(&config.sqlite_path);
    Ok((conn, search))
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

    let (mut conn, mut search) = match bootstrap(&config) {
        Ok(ready) => {
            // Every startup failure path is behind us: the schema is current,
            // the WAL is folded, and the database files are owner-only.
            let _ = ready_tx.send(Ok(()));
            ready
        }
        Err(error) => {
            tracing::error!(%error, "storage bootstrap failed");
            let _ = ready_tx.send(Err(error.to_string()));
            return Err(error);
        }
    };
    tracing::info!(
        path = %config.sqlite_path.display(),
        synchronous = config.synchronous.as_str(),
        fts_mode = search.mode().as_str(),
        "storage writer ready (wal mode)"
    );
    stats.mark_progress();

    let fault_after = fault::armed_after(fault::WRITER_AFTER_N);
    // A zero budget makes every group hold exactly the command being executed,
    // so fault injection keeps counting one command at a time.
    let group_budget = if fault_after.is_some() {
        0
    } else {
        config.group_commit_max_records
    };
    let mut commands_processed = 0u64;
    let mut pending: Option<WriteCommand> = None;
    // Reused across transactions: statement lookups, resource fingerprints and
    // JSON/hex buffers are invariant work, not per-group work.
    let mut insert_scratch = InsertScratch::new();
    let mut last_rebuild_step = Instant::now();

    loop {
        let depth = receiver.len() + usize::from(pending.is_some());
        stats.observe_queue_depth(depth);

        let command = match pending.take() {
            Some(command) => command,
            None if search.is_rebuilding() => match receiver.recv_timeout(SEARCH_REBUILD_TICK) {
                Ok(command) => command,
                Err(RecvTimeoutError::Timeout) => {
                    rebuild_while_idle(&mut conn, &mut search, &receiver, stats)?;
                    continue;
                }
                Err(RecvTimeoutError::Disconnected) => break,
            },
            None => match receiver.recv() {
                Ok(command) => command,
                Err(_) => break,
            },
        };

        // Depth *after* dequeue approximates the backlog that heavy
        // maintenance would stall behind it. The in-flight guard spans group
        // collection, the transaction and settlement, so a wedged group stays
        // visible to the watchdog.
        let backlog = receiver.len() + usize::from(pending.is_some());
        let consumed = {
            let _in_flight = stats.begin_command(backlog);
            execute_next(
                &mut conn,
                command,
                &receiver,
                &mut pending,
                &mut search,
                ledger,
                stats,
                &config,
                group_budget,
                &mut insert_scratch,
            )?
        };
        commands_processed = commands_processed.saturating_add(consumed);

        if search.is_rebuilding()
            && last_rebuild_step.elapsed() >= SEARCH_REBUILD_UNDER_LOAD_INTERVAL
        {
            run_rebuild_step(
                &mut conn,
                &mut search,
                SEARCH_REBUILD_UNDER_LOAD_ROWS,
                stats,
            )?;
            last_rebuild_step = Instant::now();
        }

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

    stats.observe_queue_depth(0);
    maintenance::checkpoint(&conn, CheckpointMode::Truncate)?;
    tracing::info!(
        records = stats.records_written.load(Ordering::Relaxed),
        transactions = stats.transactions_committed.load(Ordering::Relaxed),
        "storage writer stopped"
    );

    Ok(())
}

/// Executes one dequeued command. Consecutive insert commands are coalesced
/// into one transaction up to `group_budget`; everything else is a hard group
/// boundary. Returns the number of queue commands consumed.
#[allow(clippy::too_many_arguments)]
fn execute_next(
    conn: &mut Connection,
    command: WriteCommand,
    receiver: &Receiver<WriteCommand>,
    pending: &mut Option<WriteCommand>,
    search: &mut SearchIndex,
    ledger: &CommitLedger,
    stats: &WriterStats,
    config: &StorageConfig,
    group_budget: usize,
    insert_scratch: &mut InsertScratch,
) -> Result<u64, StorageError> {
    match command {
        WriteCommand::InsertLogs(batch) => {
            let (group, boundary) =
                collect_group(batch, group_budget, || match receiver.try_recv() {
                    Ok(WriteCommand::InsertLogs(batch)) => Pull::Insert(batch),
                    Ok(other) => Pull::Boundary(other),
                    Err(TryRecvError::Empty) => Pull::Empty,
                    Err(TryRecvError::Disconnected) => Pull::Disconnected,
                });
            *pending = boundary;
            let consumed = group.len() as u64;
            ::metrics::counter!("storage_commands_total", "kind" => "insert_logs")
                .increment(consumed);
            stats
                .batches_received
                .fetch_add(group.len() as u64, Ordering::Relaxed);
            stats.observe_queue_depth(receiver.len() + usize::from(pending.is_some()));

            // Quota probes, transactions and statement work all belong only to
            // groups that actually carry rows.
            if group.records() > 0
                && let Some(quota) = config.max_db_bytes
            {
                enforce_quota(conn, &config.sqlite_path, quota, stats);
            }
            transaction_group::persist_group(conn, stats, ledger, &group, insert_scratch)?;
            Ok(consumed)
        }
        other => {
            ::metrics::counter!("storage_commands_total", "kind" => other.kind()).increment(1);
            execute_non_insert(conn, other, search, stats)?;
            Ok(1)
        }
    }
}

/// Handles barriers and maintenance commands. They never share an insert
/// transaction and never let collection cross them.
fn execute_non_insert(
    conn: &mut Connection,
    command: WriteCommand,
    search: &mut SearchIndex,
    stats: &WriterStats,
) -> Result<(), StorageError> {
    match command {
        WriteCommand::InsertLogs(_) => unreachable!("insert commands are handled by execute_next"),
        WriteCommand::Flush => {}
        WriteCommand::Maintenance(operation) => {
            let _maintenance = stats.maintenance_guard();
            run_maintenance(conn, operation, search, stats)?;
        }
    }
    Ok(())
}

/// Advances the search-index rebuild while the command queue is idle.
fn rebuild_while_idle(
    conn: &mut Connection,
    search: &mut SearchIndex,
    receiver: &Receiver<WriteCommand>,
    stats: &WriterStats,
) -> Result<(), StorageError> {
    while search.is_rebuilding() && receiver.is_empty() {
        run_rebuild_step(conn, search, SEARCH_REBUILD_IDLE_ROWS, stats)?;
    }
    Ok(())
}

fn run_rebuild_step(
    conn: &mut Connection,
    search: &mut SearchIndex,
    max_rows: usize,
    stats: &WriterStats,
) -> Result<(), StorageError> {
    let _maintenance = stats.maintenance_guard();
    match search.step(conn, max_rows)? {
        RebuildStep::Progress { rows } => {
            ::metrics::counter!("storage_search_index_rebuild_rows_total").increment(rows as u64);
        }
        RebuildStep::Complete => {
            tracing::info!("search index rebuild complete");
        }
    }
    stats.mark_progress();
    Ok(())
}

/// Executes one maintenance command. Operations disabled by configuration
/// (e.g. a rebuild while the search index is off) return without counting a
/// run; everything else is real maintenance work.
fn run_maintenance(
    conn: &mut Connection,
    operation: MaintenanceOperation,
    search: &mut SearchIndex,
    stats: &WriterStats,
) -> Result<(), StorageError> {
    match operation {
        MaintenanceOperation::Analyze => maintenance::analyze(conn)?,
        MaintenanceOperation::Checkpoint(mode) => maintenance::checkpoint(conn, mode)?,
        MaintenanceOperation::Vacuum => maintenance::vacuum(conn)?,
        MaintenanceOperation::RebuildFts => {
            // The command restarts a resumable background rebuild; it never
            // runs the whole scan synchronously and never re-enables a
            // disabled index.
            if !search.restart(conn)? {
                return Ok(());
            }
        }
        MaintenanceOperation::Prune(window) => {
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
    stats.maintenance_runs.fetch_add(1, Ordering::Relaxed);
    stats.mark_progress();
    Ok(())
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
