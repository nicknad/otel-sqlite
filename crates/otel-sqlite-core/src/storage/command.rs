use std::fmt;
use std::time::Duration;

use crate::model::LogRecord;

use super::batch::WriteBatch;

/// Origin metadata shared by all records of one mapped OTLP payload group
/// (`ResourceLogs`).
///
/// Records keep their batch-level origin so the insert batcher can combine
/// arbitrary incoming groups without ever attributing records to the wrong
/// resource: batches with different origins are never merged into the same
/// [`WriteBatch`].
#[derive(Debug, Clone, PartialEq, Default)]
pub struct BatchOrigin {
    pub resource: Option<crate::model::Resource>,
    pub schema_url: String,
}

/// One mapped group of log records plus its origin, as handed from ingress to
/// the storage insert batcher.
///
/// `commit_seq` is the durability ticket issued by the ingress
/// [`CommitLedger`](super::commit::CommitLedger): it identifies the chunk on
/// the commit watermark so OTLP handlers in `durability = "commit"` mode can
/// wait for this exact data to be persisted before acknowledging. Batching
/// keeps each ticket's records in ordered [`RecordSegment`]s; a ticket is
/// settled only by the segment carrying its last record, even when the chunk
/// spans several write batches.
#[derive(Debug)]
pub struct LogChunk {
    pub origin: BatchOrigin,
    pub records: Vec<LogRecord>,
    pub commit_seq: u64,
}

impl LogChunk {
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }
}

/// Input side of the ingestion pipeline: one item on the FIFO stream between
/// ingress and the storage-owned insert batcher.
///
/// Most items are payloads — mapped records (`LogChunk`) that the batcher
/// absorbs into sized [`WriteBatch`]es; their boundaries are *not* SQLite
/// transaction boundaries. The remaining item is the `Flush` barrier, an
/// instruction the batcher executes in stream order. Both kinds must share one
/// stream because a barrier derives its ordering guarantee from interleaving
/// with the payloads it brackets. Maintenance and checkpoint commands reach
/// the writer directly through its producer handle, not this channel.
#[derive(Debug)]
pub enum IngestMessage {
    /// Mapped log records plus their origin; a payload for the batcher.
    Logs(LogChunk),
    /// Order barrier: the batcher first flushes its partial buffers, then
    /// forwards this, so everything ingested before it reaches SQLite before
    /// anything ingested afterwards is observed.
    Flush,
}

impl IngestMessage {
    /// Stamps a payload message with its durability ticket. Barrier messages
    /// carry no records and ignore the ticket.
    pub fn set_commit_seq(&mut self, commit_seq: u64) {
        match self {
            Self::Logs(chunk) => chunk.commit_seq = commit_seq,
            Self::Flush => {}
        }
    }

    /// The durability ticket carried by a payload message, if any.
    pub const fn commit_seq(&self) -> Option<u64> {
        match self {
            Self::Logs(chunk) => Some(chunk.commit_seq),
            Self::Flush => None,
        }
    }

    /// Number of records the message carries (zero for the `Flush` barrier).
    pub fn record_count(&self) -> usize {
        match self {
            Self::Logs(chunk) => chunk.records.len(),
            Self::Flush => 0,
        }
    }
}

/// One run of records that belong to the same durability ticket.
///
/// A chunk may be split across several storage batches. `complete` is true
/// only on the segment that carries the chunk's last record; the writer must
/// settle the ticket then, never on an earlier segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordSegment {
    pub commit_seq: u64,
    pub records: usize,
    pub complete: bool,
}

/// A storage-ready batch of log records with its origin.
///
/// `segments` partitions `records` in order and says which durability ticket
/// each run belongs to; only a segment with `complete = true` may settle its
/// ticket. A ticket whose chunk is split across batches therefore stays
/// outstanding until its last record is written, so a durable ack can never be
/// released for records still buffered. The segments are the single source of
/// truth: the ticket set is derived from them, never stored separately.
#[derive(Debug, Clone, PartialEq)]
pub struct LogWriteBatch {
    pub origin: BatchOrigin,
    pub records: WriteBatch<LogRecord>,
    segments: Vec<RecordSegment>,
}

impl LogWriteBatch {
    /// Builds a batch and checks that the segments partition its records
    /// exactly. A mismatch would misattribute durability tickets, so it is a
    /// programming error rather than a recoverable state.
    pub fn new(
        origin: BatchOrigin,
        records: WriteBatch<LogRecord>,
        segments: Vec<RecordSegment>,
    ) -> Self {
        let covered: usize = segments.iter().map(|segment| segment.records).sum();
        debug_assert_eq!(
            covered,
            records.len(),
            "record segments must cover every record exactly once"
        );
        Self {
            origin,
            records,
            segments,
        }
    }

    /// Ordered ticket partition of [`LogWriteBatch::records`].
    pub fn segments(&self) -> &[RecordSegment] {
        &self.segments
    }

    /// Every distinct ticket referenced by this batch, ascending. Derived
    /// from the segments so the two can never disagree.
    pub fn commit_seqs(&self) -> Vec<u64> {
        let mut tickets: Vec<u64> = self
            .segments
            .iter()
            .map(|segment| segment.commit_seq)
            .collect();
        tickets.sort_unstable();
        tickets.dedup();
        tickets
    }
}

/// Commands consumed by the single SQLite writer. Insert variants carry fully
/// formed storage batches produced by the insert batcher; the writer may
/// commit consecutive insert commands together in one transaction.
#[derive(Debug, Clone, PartialEq)]
pub enum WriteCommand {
    InsertLogs(LogWriteBatch),
    Flush,
    Maintenance(MaintenanceOperation),
}

impl WriteCommand {
    /// Number of records the command would persist (insert commands only).
    pub fn record_count(&self) -> usize {
        match self {
            Self::InsertLogs(batch) => batch.records.len(),
            _ => 0,
        }
    }

    pub const fn kind(&self) -> &'static str {
        match self {
            Self::InsertLogs(_) => "insert_logs",
            Self::Flush => "flush",
            Self::Maintenance(operation) => operation.as_str(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[repr(u8)]
pub enum CheckpointMode {
    #[default]
    Passive = 0,
    Full = 1,
    Restart = 2,
    Truncate = 3,
}

impl CheckpointMode {
    /// Parses the operator-facing spelling (`passive`, `full`, `restart`,
    /// `truncate`, case-insensitive). Lives beside the type so every config
    /// loader uses one vocabulary.
    pub fn parse(value: &str) -> Option<Self> {
        if value.eq_ignore_ascii_case("passive") {
            Some(Self::Passive)
        } else if value.eq_ignore_ascii_case("full") {
            Some(Self::Full)
        } else if value.eq_ignore_ascii_case("restart") {
            Some(Self::Restart)
        } else if value.eq_ignore_ascii_case("truncate") {
            Some(Self::Truncate)
        } else {
            None
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Passive => "PASSIVE",
            Self::Full => "FULL",
            Self::Restart => "RESTART",
            Self::Truncate => "TRUNCATE",
        }
    }
}

impl fmt::Display for CheckpointMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MaintenanceOperation {
    Analyze,
    Checkpoint(CheckpointMode),
    Vacuum,
    RebuildFts,
    /// Age-based prune of log events.
    ///
    /// The SQLite writer interprets the window at execution time (cutoff =
    /// now − window), so a long-queued prune never deletes data newer than
    /// the configured window. Disabled pruning is represented by not
    /// scheduling the operation at all.
    Prune(Duration),
}

impl MaintenanceOperation {
    /// Canonical name used by scheduling, writer logs and metrics.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Analyze => "optimize",
            Self::Checkpoint(_) => "checkpoint",
            Self::Vacuum => "vacuum",
            Self::RebuildFts => "rebuild_fts",
            Self::Prune(_) => "purge",
        }
    }

    /// Operations that can stall the single writer for an extended time.
    /// Cheap operations (`ANALYZE`, a passive checkpoint) may run even under
    /// backlog.
    pub const fn is_heavy(self) -> bool {
        matches!(
            self,
            Self::Vacuum
                | Self::RebuildFts
                | Self::Prune(_)
                | Self::Checkpoint(
                    CheckpointMode::Full | CheckpointMode::Restart | CheckpointMode::Truncate
                )
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Resource;

    fn sample_log_write(records: usize) -> LogWriteBatch {
        LogWriteBatch::new(
            BatchOrigin {
                resource: Some(Resource::new(vec![("service.name", "test").into()])),
                schema_url: "https://example.test".to_owned(),
            },
            WriteBatch::from_vec((0..records).map(|_| LogRecord::default()).collect()),
            vec![RecordSegment {
                commit_seq: 7,
                records,
                complete: true,
            }],
        )
    }

    #[test]
    fn command_kinds_use_the_maintenance_label() {
        let logs = WriteCommand::InsertLogs(sample_log_write(1));
        assert_eq!(logs.kind(), "insert_logs");

        assert_eq!(WriteCommand::Flush.kind(), "flush");
        assert_eq!(
            WriteCommand::Maintenance(MaintenanceOperation::Checkpoint(
                CheckpointMode::Truncate
            ))
            .kind(),
            "checkpoint"
        );
        assert_eq!(
            WriteCommand::Maintenance(MaintenanceOperation::Vacuum).kind(),
            "vacuum"
        );
        assert_eq!(
            WriteCommand::Maintenance(MaintenanceOperation::Analyze).kind(),
            "optimize"
        );
        assert_eq!(
            WriteCommand::Maintenance(MaintenanceOperation::Prune(Duration::from_secs(1))).kind(),
            "purge"
        );
    }

    #[test]
    fn record_counts_only_apply_to_inserts() {
        assert_eq!(WriteCommand::Flush.record_count(), 0);
        assert_eq!(
            WriteCommand::Maintenance(MaintenanceOperation::Checkpoint(
                CheckpointMode::Passive
            ))
            .record_count(),
            0
        );
        assert_eq!(
            WriteCommand::InsertLogs(sample_log_write(3)).record_count(),
            3
        );
    }

    #[test]
    fn batch_tickets_are_derived_from_segments() {
        let batch = LogWriteBatch::new(
            BatchOrigin::default(),
            WriteBatch::from_vec(vec![LogRecord::default(), LogRecord::default()]),
            vec![
                RecordSegment {
                    commit_seq: 5,
                    records: 1,
                    complete: true,
                },
                RecordSegment {
                    commit_seq: 3,
                    records: 1,
                    complete: true,
                },
            ],
        );
        assert_eq!(batch.commit_seqs(), vec![3, 5]);
    }

    #[test]
    fn checkpoint_mode_display_and_default() {
        assert_eq!(CheckpointMode::Full.to_string(), "FULL");
        assert_eq!(CheckpointMode::default(), CheckpointMode::Passive);
    }

    #[test]
    fn checkpoint_mode_parses_the_operator_spelling() {
        for mode in [
            CheckpointMode::Passive,
            CheckpointMode::Full,
            CheckpointMode::Restart,
            CheckpointMode::Truncate,
        ] {
            assert_eq!(CheckpointMode::parse(&mode.as_str().to_lowercase()), Some(mode));
        }
        assert_eq!(CheckpointMode::parse("FULL"), Some(CheckpointMode::Full));
        assert_eq!(CheckpointMode::parse("unknown"), None);
    }

    #[test]
    fn only_long_running_operations_are_heavy() {
        assert!(!MaintenanceOperation::Analyze.is_heavy());
        assert!(!MaintenanceOperation::Checkpoint(CheckpointMode::Passive).is_heavy());
        assert!(MaintenanceOperation::Checkpoint(CheckpointMode::Full).is_heavy());
        assert!(MaintenanceOperation::Vacuum.is_heavy());
        assert!(MaintenanceOperation::RebuildFts.is_heavy());
        assert!(MaintenanceOperation::Prune(Duration::from_secs(1)).is_heavy());
    }

    #[test]
    fn commit_seq_stamps_and_reads_inserts_only() {
        let mut logs = IngestMessage::Logs(LogChunk {
            origin: BatchOrigin::default(),
            records: vec![LogRecord::default()],
            commit_seq: 0,
        });
        assert_eq!(logs.commit_seq(), Some(0));
        logs.set_commit_seq(42);
        assert_eq!(logs.commit_seq(), Some(42));

        // Barriers carry no records, so they have no ticket.
        let mut flush = IngestMessage::Flush;
        flush.set_commit_seq(11);
        assert_eq!(flush.commit_seq(), None);
    }

    #[test]
    fn record_count_matches_payload_records_only() {
        let logs = IngestMessage::Logs(LogChunk {
            origin: BatchOrigin::default(),
            records: vec![LogRecord::default(), LogRecord::default()],
            commit_seq: 0,
        });
        assert_eq!(logs.record_count(), 2);
        assert_eq!(IngestMessage::Flush.record_count(), 0);
    }
}
