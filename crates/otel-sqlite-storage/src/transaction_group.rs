//! Writer-side insert transaction policy.
//!
//! [`InsertGroup`] coalesces consecutive `InsertLogs` commands the writer has
//! already dequeued. The writer collects without blocking and passes the group
//! here; this module owns the transaction boundary, retry/salvage policy and
//! commit-ticket settlement.
//!
//! Settlement is reachable only after [`run_transaction`] committed. A strict
//! group transaction that hits a poison error rolls back as a unit and falls
//! back to the proven per-batch policy, so `SalvageOutcome::by_ticket` never
//! has to be merged across commands.

use std::time::{Duration, Instant};

use otel_sqlite_core::storage::{CommitLedger, LogWriteBatch, WriteCommand};
use rusqlite::Connection;
use std::sync::atomic::Ordering;

use crate::command;
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

    /// Accepts the first batch unconditionally so one oversized command always
    /// makes progress. A later batch that would exceed the budget is returned
    /// to the writer's lookahead slot.
    #[allow(clippy::result_large_err)]
    pub(crate) fn try_push(&mut self, batch: LogWriteBatch) -> Result<(), LogWriteBatch> {
        let added = batch.records.len();
        if !self.batches.is_empty() && self.records.saturating_add(added) > self.budget {
            return Err(batch);
        }
        self.records = self.records.saturating_add(added);
        self.batches.push(batch);
        Ok(())
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
    let mut group = InsertGroup::new(budget);
    group
        .try_push(first)
        .expect("an empty insert group accepts any batch");
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
/// only after the commit. Empty-only groups void their tickets without opening
/// a transaction.
pub(crate) fn persist_group(
    conn: &mut Connection,
    stats: &WriterStats,
    ledger: &CommitLedger,
    group: &InsertGroup,
) -> Result<(), StorageError> {
    if group.records() == 0 {
        settle_group(ledger, group);
        return Ok(());
    }

    let mut attempt = 0u32;
    loop {
        let outcome = run_transaction(conn, stats, |tx| {
            let mut written = 0u64;
            for batch in group.iter() {
                if !batch.records.is_empty() {
                    written += command::insert_logs(tx, batch)?;
                }
            }
            Ok(written)
        });
        match outcome {
            Ok(()) => {
                settle_group(ledger, group);
                return Ok(());
            }
            Err((error, FailureClass::Poison)) => {
                tracing::warn!(
                    batches = group.len(),
                    %error,
                    "poisonous group rejected by sqlite; falling back to per-batch salvage"
                );
                ::metrics::counter!("storage_group_poison_fallbacks_total").increment(1);
                for batch in group.iter() {
                    persist_one(conn, stats, ledger, batch)?;
                }
                return Ok(());
            }
            Err((error, FailureClass::Retryable)) if attempt < MAX_TRANSIENT_RETRIES => {
                let delay = backoff_delay(attempt);
                attempt += 1;
                stats.transient_retries.fetch_add(1, Ordering::Relaxed);
                ::metrics::counter!("storage_transient_retries_total").increment(1);
                tracing::warn!(
                    %error,
                    attempt,
                    ?delay,
                    "transient sqlite failure; retrying grouped transaction"
                );
                std::thread::sleep(delay);
            }
            Err((error, _)) => return Err(error),
        }
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
) -> Result<(), StorageError> {
    if batch.records.is_empty() {
        for commit_seq in &batch.commit_seqs {
            ledger.void(*commit_seq);
        }
        return Ok(());
    }
    let salvage = persist_with_policy(
        conn,
        stats,
        "logs",
        |tx| command::insert_logs(tx, batch),
        |tx| command::insert_logs_tolerant(tx, batch),
    )?;
    settle_log_batch(ledger, batch, salvage);
    Ok(())
}

/// Settles every batch of a successfully committed group in queue order.
fn settle_group(ledger: &CommitLedger, group: &InsertGroup) {
    for batch in group.iter() {
        if batch.records.is_empty() {
            for commit_seq in &batch.commit_seqs {
                ledger.void(*commit_seq);
            }
        } else {
            settle_log_batch(ledger, batch, None);
        }
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

/// Persistence policy for one transaction.
///
/// * strict pass first; on success the transaction is done.
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
            let elapsed_us = u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX);
            tracing::debug!(
                records = written,
                duration_us = elapsed_us,
                "transaction committed"
            );
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
    if batch.segments.is_empty() {
        // Legacy/bench batches carry no per-record ticket map. Treat every
        // referenced ticket as complete; a salvage drop fails all of them.
        let dropped = salvage.as_ref().map_or(0, |outcome| outcome.dropped);
        for commit_seq in &batch.commit_seqs {
            if dropped > 0 {
                ledger.note_dropped(*commit_seq, dropped);
            }
            ledger.complete(*commit_seq);
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

#[cfg(test)]
mod tests {
    use super::*;
    use otel_sqlite_core::model::LogRecord;
    use otel_sqlite_core::storage::{BatchOrigin, RecordSegment, SyncMode, WriteBatch};
    use std::collections::VecDeque;

    fn batch(records: usize) -> LogWriteBatch {
        LogWriteBatch {
            origin: BatchOrigin::default(),
            records: WriteBatch::from_vec((0..records).map(|_| LogRecord::default()).collect()),
            commit_seqs: Vec::new(),
            segments: Vec::new(),
        }
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
            let mut batch = batch(records);
            batch.commit_seqs = vec![ticket];
            batch.segments = vec![RecordSegment {
                commit_seq: ticket,
                records,
                complete: true,
            }];
            group.try_push(batch).expect("fits");
        }

        persist_group(&mut conn, &stats, &ledger, &group).expect("commit group");

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
    fn empty_group_settles_tickets_without_opening_a_transaction() {
        let stats = WriterStats::new();
        let ledger = CommitLedger::new();
        let mut group = InsertGroup::new(10);
        let mut empty = batch(0);
        empty.commit_seqs = vec![7];
        group.try_push(empty).expect("empty batch accepted");

        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("group-empty.db");
        let mut conn = crate::sqlite::open(&path, SyncMode::default()).expect("open database");
        crate::migration::migrate(&mut conn).expect("migrate");
        persist_group(&mut conn, &stats, &ledger, &group).expect("settle empty group");

        assert_eq!(stats.transactions_committed.load(Ordering::Relaxed), 0);
        assert!(
            matches!(
                ledger.state(7),
                otel_sqlite_core::storage::TicketState::Completed
            ),
            "an empty batch settles its ticket without a transaction"
        );
    }
}
