//! Writer-side insert transaction policy.
//!
//! [`InsertGroup`] coalesces consecutive `InsertLogs` commands the writer has
//! already dequeued. The writer collects without blocking and passes the group
//! here; this module owns the transaction boundary, retry/salvage policy and
//! commit-ticket settlement.
//!
//! Settlement is reachable only after [`commit_transaction`] committed. A strict
//! group transaction that hits a poison error rolls back as a unit and falls
//! back to the proven per-batch policy, so `SalvageOutcome::by_ticket` never
//! has to be merged across commands.

use std::time::{Duration, Instant};

use otel_sqlite_core::storage::{CommitLedger, LogWriteBatch, WriteCommand};
use rusqlite::Connection;
use std::sync::atomic::Ordering;

use crate::command;
use crate::command::InsertScratch;
use crate::error::{FailureClass, StorageError};
use crate::stats::WriterStats;

/// Consecutive insert commands the writer may commit atomically.
pub(crate) struct InsertGroup {
    budget: usize,
    batches: Vec<LogWriteBatch>,
    records: usize,
}

/// One non-blocking pull while a group is assembled.
pub(crate) enum Pull {
    Insert(LogWriteBatch),
    Boundary(WriteCommand),
    Empty,
    Disconnected,
}

impl InsertGroup {
    pub(crate) fn new(budget: usize) -> Self {
        Self {
            budget,
            batches: Vec::new(),
            records: 0,
        }
    }

    /// Starts a group from the command that triggered collection. The first
    /// batch is accepted unconditionally so one oversized command always makes
    /// progress.
    pub(crate) fn with_first(budget: usize, first: LogWriteBatch) -> Self {
        let mut group = Self::new(budget);
        group.push(first);
        group
    }

    /// Accepts the next batch unless it would exceed the budget; the rejected
    /// batch is returned to the writer's lookahead slot.
    #[allow(clippy::result_large_err)]
    pub(crate) fn try_push(&mut self, batch: LogWriteBatch) -> Result<(), LogWriteBatch> {
        if !self.batches.is_empty()
            && self.records.saturating_add(batch.records.len()) > self.budget
        {
            return Err(batch);
        }
        self.push(batch);
        Ok(())
    }

    fn push(&mut self, batch: LogWriteBatch) {
        debug_assert!(
            !batch.records.is_empty(),
            "the insert batcher never emits empty batches"
        );
        self.records = self.records.saturating_add(batch.records.len());
        self.batches.push(batch);
    }

    pub(crate) const fn at_budget(&self) -> bool {
        self.records >= self.budget
    }

    pub(crate) const fn records(&self) -> usize {
        self.records
    }

    pub(crate) fn len(&self) -> usize {
        self.batches.len()
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = &LogWriteBatch> {
        self.batches.iter()
    }
}

/// Collects the head insert plus every consecutive insert that fits the
/// budget. Never blocks and never reorders. The first non-insert command is
/// returned as the boundary the writer must execute after this group.
pub(crate) fn collect_group(
    first: LogWriteBatch,
    budget: usize,
    mut pull: impl FnMut() -> Pull,
) -> (InsertGroup, Option<WriteCommand>) {
    let mut group = InsertGroup::with_first(budget, first);
    let mut boundary = None;
    while !group.at_budget() {
        match pull() {
            Pull::Insert(batch) => {
                if let Err(batch) = group.try_push(batch) {
                    boundary = Some(WriteCommand::InsertLogs(batch));
                    break;
                }
            }
            Pull::Boundary(command) => {
                boundary = Some(command);
                break;
            }
            Pull::Empty | Pull::Disconnected => break,
        }
    }
    (group, boundary)
}

/// Persists one group under the writer's error policy and settles every batch
/// only after the commit. A poison failure falls back to the per-batch
/// strict-then-salvage path so healthy rows still commit.
pub(crate) fn persist_group(
    conn: &mut Connection,
    stats: &WriterStats,
    ledger: &CommitLedger,
    group: &InsertGroup,
    scratch: &mut InsertScratch,
) -> Result<(), StorageError> {
    let outcome = run_with_retries(stats, "logs group", || {
        commit_transaction(conn, stats, |tx| {
            let mut written = 0u64;
            for batch in group.iter() {
                written += command::insert_logs(tx, batch, scratch)?;
            }
            Ok(((), CommitReport::written(written)))
        })
    });
    match outcome {
        Ok(()) => {
            settle_group(ledger, group);
            Ok(())
        }
        Err((error, FailureClass::Poison)) => {
            tracing::warn!(
                batches = group.len(),
                %error,
                "poisonous group rejected by sqlite; falling back to per-batch salvage"
            );
            ::metrics::counter!("storage_group_poison_fallbacks_total").increment(1);
            for batch in group.iter() {
                persist_one(conn, stats, ledger, batch, scratch)?;
            }
            Ok(())
        }
        Err((error, _)) => Err(error),
    }
}

/// Persists one write batch with the strict-then-salvage policy and settles
/// its tickets after the commit. This is the pre-grouping insert path, reused
/// by the grouped poison fallback.
pub(crate) fn persist_one(
    conn: &mut Connection,
    stats: &WriterStats,
    ledger: &CommitLedger,
    batch: &LogWriteBatch,
    scratch: &mut InsertScratch,
) -> Result<(), StorageError> {
    let strict = run_with_retries(stats, "logs", || {
        commit_transaction(conn, stats, |tx| {
            let written = command::insert_logs(tx, batch, scratch)?;
            Ok(((), CommitReport::written(written)))
        })
    });
    match strict {
        Ok(()) => {
            settle_log_batch(ledger, batch, None);
            Ok(())
        }
        Err((cause, FailureClass::Poison)) => {
            tracing::warn!(
                signal = "logs",
                %cause,
                "poisonous batch rejected by sqlite; attempting salvage pass"
            );
            // Salvage is exceptional, so it gets its own scratch, created only
            // if the strict pass actually needs a second chance.
            let mut salvage_scratch = InsertScratch::new();
            let outcome = run_with_retries(stats, "logs salvage", || {
                commit_transaction(conn, stats, |tx| {
                    let outcome = command::insert_logs_tolerant(tx, batch, &mut salvage_scratch)?;
                    let report = CommitReport::salvage(&outcome);
                    Ok((outcome, report))
                })
            })
            .map_err(|(error, _)| error)?;
            tracing::warn!(
                signal = "logs",
                kept = outcome.kept,
                dropped = outcome.dropped,
                cause = %cause,
                "salvage pass complete: healthy rows committed, poisonous rows quarantined"
            );
            settle_log_batch(ledger, batch, Some(outcome));
            Ok(())
        }
        Err((error, _)) => Err(error),
    }
}

/// Settles every batch of a successfully committed group in queue order.
fn settle_group(ledger: &CommitLedger, group: &InsertGroup) {
    for batch in group.iter() {
        settle_log_batch(ledger, batch, None);
    }
}

/// How often a [`FailureClass::Retryable`] transaction is re-attempted before
/// its error is treated as fatal. Backoff grows exponentially from
/// [`RETRY_BACKOFF_BASE`].
const MAX_TRANSIENT_RETRIES: u32 = 4;
const RETRY_BACKOFF_BASE: Duration = Duration::from_millis(250);

fn backoff_delay(attempt: u32) -> Duration {
    // attempt 0..MAX-1 -> 250ms, 500ms, 1s, 2s (capped at 4s for safety).
    RETRY_BACKOFF_BASE
        .saturating_mul(1u32 << attempt.min(4))
        .min(Duration::from_secs(4))
}

/// Runs one classified transaction attempt, retrying retryable contention
/// (BUSY/LOCKED) with exponential backoff. Poison and fatal failures are
/// returned to the caller, which owns the salvage/abort decision.
fn run_with_retries<T>(
    stats: &WriterStats,
    signal: &'static str,
    mut run: impl FnMut() -> Result<T, (StorageError, FailureClass)>,
) -> Result<T, (StorageError, FailureClass)> {
    let mut attempt = 0u32;
    loop {
        match run() {
            Ok(value) => return Ok(value),
            Err((error, FailureClass::Retryable)) if attempt < MAX_TRANSIENT_RETRIES => {
                let delay = backoff_delay(attempt);
                attempt += 1;
                stats.transient_retries.fetch_add(1, Ordering::Relaxed);
                ::metrics::counter!("storage_transient_retries_total").increment(1);
                tracing::warn!(
                    signal,
                    %error,
                    attempt,
                    ?delay,
                    "transient sqlite failure; retrying transaction"
                );
                std::thread::sleep(delay);
            }
            Err(failure) => return Err(failure),
        }
    }
}

/// What one committed transaction persisted. Strict and salvage commits report
/// through the same helper so their counters and metrics cannot drift apart.
struct CommitReport {
    records_written: u64,
    quarantined: u64,
}

impl CommitReport {
    const fn written(records_written: u64) -> Self {
        Self {
            records_written,
            quarantined: 0,
        }
    }

    fn salvage(outcome: &command::SalvageOutcome) -> Self {
        Self {
            records_written: outcome.kept,
            quarantined: outcome.dropped,
        }
    }
}

/// Runs one transaction, mapping failures to their classification so the retry
/// policy can decide without re-matching errors. The body returns its result
/// plus the report the shared commit helper publishes.
fn commit_transaction<T>(
    conn: &mut Connection,
    stats: &WriterStats,
    mut persist: impl FnMut(&rusqlite::Transaction<'_>) -> Result<(T, CommitReport), StorageError>,
) -> Result<T, (StorageError, FailureClass)> {
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
        Ok((value, report)) => {
            if let Err(error) = tx.commit() {
                let error = StorageError::from(error);
                let class = error.class();
                record_failure(stats, &error);
                return Err((error, class));
            }
            report_commit(stats, report, started.elapsed());
            Ok(value)
        }
        Err(error) => {
            let _ = tx.rollback();
            let class = error.class();
            record_failure(stats, &error);
            Err((error, class))
        }
    }
}

/// Publishes the counters, metrics and logs shared by every successful
/// transaction, whether strict or salvaged.
fn report_commit(stats: &WriterStats, report: CommitReport, elapsed: Duration) {
    stats
        .records_written
        .fetch_add(report.records_written, Ordering::Relaxed);
    if report.quarantined > 0 {
        stats
            .quarantined_records
            .fetch_add(report.quarantined, Ordering::Relaxed);
        ::metrics::counter!("storage_quarantined_records_total").increment(report.quarantined);
    }
    stats.transactions_committed.fetch_add(1, Ordering::Relaxed);
    stats.mark_progress();
    ::metrics::counter!("storage_records_written_total").increment(report.records_written);
    ::metrics::histogram!("storage_transaction_batch_size").record(report.records_written as f64);
    ::metrics::histogram!("storage_transaction_duration").record(elapsed.as_secs_f64());
    let elapsed_us = u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX);
    tracing::debug!(
        records = report.records_written,
        duration_us = elapsed_us,
        "transaction committed"
    );
}

fn record_failure(stats: &WriterStats, error: &StorageError) {
    stats.errors.fetch_add(1, Ordering::Relaxed);
    ::metrics::counter!("storage_errors_total").increment(1);
    let class = error.class();
    tracing::error!(%error, ?class, "transaction failed, rolled back");
}

/// Publishes a persisted batch to the commit ledger.
///
/// A ticket settles only when the batch contains its final segment. Records
/// written by an earlier segment are reported as progress, so a close midway
/// through a split chunk can still tell the handler how much is durable.
fn settle_log_batch(
    ledger: &CommitLedger,
    batch: &LogWriteBatch,
    salvage: Option<command::SalvageOutcome>,
) {
    match salvage {
        None => {
            for segment in batch.segments() {
                ledger.note_committed(segment.commit_seq, segment.records as u64);
                if segment.complete {
                    ledger.complete(segment.commit_seq);
                }
            }
        }
        Some(outcome) => {
            // `by_ticket` is already summed for the whole batch, so note it
            // once per ticket rather than once per segment.
            for commit_seq in batch.commit_seqs() {
                let (kept, dropped) = outcome
                    .by_ticket
                    .get(&commit_seq)
                    .copied()
                    .unwrap_or((0, 0));
                if kept > 0 {
                    ledger.note_committed(commit_seq, kept);
                }
                if dropped > 0 {
                    ledger.note_dropped(commit_seq, dropped);
                }
            }
            for segment in batch.segments() {
                if segment.complete {
                    ledger.complete(segment.commit_seq);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use otel_sqlite_core::model::LogRecord;
    use otel_sqlite_core::storage::{
        BatchOrigin, RecordSegment, SyncMode, TicketState, WriteBatch,
    };
    use std::collections::VecDeque;

    fn batch_with_ticket(records: usize, ticket: u64) -> LogWriteBatch {
        LogWriteBatch::new(
            BatchOrigin::default(),
            WriteBatch::from_vec((0..records).map(|_| LogRecord::default()).collect()),
            vec![RecordSegment {
                commit_seq: ticket,
                records,
                complete: true,
            }],
        )
    }

    fn batch(records: usize) -> LogWriteBatch {
        batch_with_ticket(records, 1)
    }

    fn ticket_batch(bodies: &[&str], ticket: u64) -> LogWriteBatch {
        LogWriteBatch::new(
            BatchOrigin::default(),
            WriteBatch::from_vec(
                bodies
                    .iter()
                    .map(|body| LogRecord {
                        body: (*body).to_owned(),
                        ..LogRecord::default()
                    })
                    .collect(),
            ),
            vec![RecordSegment {
                commit_seq: ticket,
                records: bodies.len(),
                complete: true,
            }],
        )
    }

    #[test]
    fn group_accepts_up_to_the_budget_and_returns_overflow() {
        let mut group = InsertGroup::new(10);
        group.try_push(batch(4)).expect("first batch");
        group.try_push(batch(5)).expect("fits exactly");
        let overflow = group.try_push(batch(2)).expect_err("over budget");
        assert_eq!(overflow.records.len(), 2);
        assert_eq!(group.records(), 9);
        assert_eq!(group.len(), 2);
    }

    #[test]
    fn first_batch_larger_than_budget_is_accepted() {
        let mut group = InsertGroup::new(10);
        group
            .try_push(batch(50))
            .expect("progress must be possible");
        assert!(group.at_budget());
    }

    #[test]
    fn collect_stops_at_a_boundary_and_preserves_it() {
        let mut pulls = VecDeque::from([
            Pull::Insert(batch(2)),
            Pull::Boundary(WriteCommand::Flush),
            Pull::Insert(batch(99)),
        ]);
        let (group, boundary) =
            collect_group(batch(1), 100, || pulls.pop_front().unwrap_or(Pull::Empty));
        assert_eq!(group.len(), 2);
        assert_eq!(group.records(), 3);
        assert!(matches!(boundary, Some(WriteCommand::Flush)));
        assert_eq!(pulls.len(), 1, "nothing after the boundary was consumed");
    }

    #[test]
    fn collect_stops_on_empty_and_disconnect() {
        let (group, boundary) = collect_group(batch(1), 100, || Pull::Empty);
        assert_eq!(group.len(), 1);
        assert!(boundary.is_none());

        let (group, boundary) = collect_group(batch(1), 100, || Pull::Disconnected);
        assert_eq!(group.len(), 1);
        assert!(boundary.is_none());
    }

    #[test]
    fn collect_never_exceeds_the_budget() {
        let mut pulls = VecDeque::from([Pull::Insert(batch(3)), Pull::Insert(batch(3))]);
        let (group, boundary) =
            collect_group(batch(3), 5, || pulls.pop_front().unwrap_or(Pull::Empty));
        assert_eq!(group.records(), 3, "the second batch would exceed 5");
        assert!(matches!(boundary, Some(WriteCommand::InsertLogs(_))));
    }

    #[test]
    fn group_commit_settles_every_ticket_after_one_transaction() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("group.db");
        let mut conn = crate::sqlite::open(&path, SyncMode::default()).expect("open database");
        crate::migration::migrate(&mut conn).expect("migrate");

        let stats = WriterStats::new();
        let ledger = CommitLedger::new();
        let mut group = InsertGroup::new(100);
        for (records, ticket) in [(2usize, 1u64), (3, 2)] {
            group
                .try_push(batch_with_ticket(records, ticket))
                .expect("fits");
        }

        persist_group(
            &mut conn,
            &stats,
            &ledger,
            &group,
            &mut InsertScratch::new(),
        )
        .expect("commit group");

        assert_eq!(
            stats.transactions_committed.load(Ordering::Relaxed),
            1,
            "two commands must share one transaction"
        );
        assert_eq!(
            stats.records_written.load(Ordering::Relaxed),
            5,
            "every record in the group is written"
        );
        let rows: i64 = conn
            .query_row("SELECT COUNT(*) FROM log_event", [], |row| row.get(0))
            .expect("count rows");
        assert_eq!(rows, 5);
        assert_eq!(
            ledger.watermark().committed_through,
            2,
            "both tickets settle after the shared commit"
        );
    }

    #[test]
    fn poison_in_a_group_falls_back_and_keeps_ticket_attribution() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("group-poison.db");
        let mut conn = crate::sqlite::open(&path, SyncMode::default()).expect("open database");
        crate::migration::migrate(&mut conn).expect("migrate");
        conn.execute_batch(
            "CREATE TRIGGER poison_row BEFORE INSERT ON log_event
             WHEN NEW.body = 'poison'
             BEGIN SELECT RAISE(ABORT, 'poison row'); END;",
        )
        .expect("create poison trigger");

        let stats = WriterStats::new();
        let ledger = CommitLedger::new();
        let mut group = InsertGroup::new(100);
        group
            .try_push(ticket_batch(&["healthy one", "poison"], 1))
            .expect("first batch fits");
        group
            .try_push(ticket_batch(&["healthy two"], 2))
            .expect("second batch fits");

        persist_group(
            &mut conn,
            &stats,
            &ledger,
            &group,
            &mut InsertScratch::new(),
        )
        .expect("fallback persists every healthy row");

        assert_eq!(
            stats.records_written.load(Ordering::Relaxed),
            2,
            "healthy rows from both batches survive"
        );
        assert_eq!(stats.quarantined_records.load(Ordering::Relaxed), 1);
        assert_eq!(
            stats.transactions_committed.load(Ordering::Relaxed),
            2,
            "the fallback commits one transaction per batch"
        );
        assert!(
            matches!(ledger.state(1), TicketState::Failed { .. }),
            "the batch containing the poison row must fail its ticket"
        );
        assert_eq!(ledger.state(2), TicketState::Completed);
        assert_eq!(ledger.watermark().committed_through, 2);

        let bodies: Vec<String> = conn
            .prepare("SELECT body FROM log_event ORDER BY id")
            .expect("query rows")
            .query_map([], |row| row.get(0))
            .expect("map rows")
            .collect::<Result<_, _>>()
            .expect("collect rows");
        assert_eq!(bodies, vec!["healthy one", "healthy two"]);
    }
}
