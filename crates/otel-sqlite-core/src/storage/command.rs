use std::fmt;
use std::time::Duration;

use crate::model::LogRecord;

use super::batch::WriteBatch;

/// Origin metadata shared by all records of one mapped OTLP payload group
/// (`ResourceLogs` / `ResourceMetrics`).
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
/// merges tickets with `max`, so every emitted write batch claims the highest
/// ticket among its records.
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
/// transaction boundaries. The remaining items are barriers
/// (`Flush`, `Checkpoint`, `Maintenance`): instructions the batcher executes
/// in stream order. Both kinds must share one stream because barriers derive
/// their ordering guarantee from interleaving with the payloads they bracket.
#[derive(Debug)]
pub enum IngestMessage {
    /// Mapped log records plus their origin; a payload for the batcher.
    Logs(LogChunk),
    /// Order barrier: the batcher first flushes its partial buffers, then
    /// forwards this, so everything ingested before it reaches SQLite before
    /// anything ingested afterwards is observed.
    Flush,
    Checkpoint(CheckpointMode),
    Maintenance(MaintenanceOperation),
}

impl IngestMessage {
    /// Stamps a payload message with its durability ticket. Barrier messages
    /// carry no records and ignore the ticket.
    pub fn set_commit_seq(&mut self, commit_seq: u64) {
        match self {
            Self::Logs(chunk) => chunk.commit_seq = commit_seq,
            Self::Flush | Self::Checkpoint(_) | Self::Maintenance(_) => {}
        }
    }

    /// The durability ticket carried by a payload message, if any.
    pub const fn commit_seq(&self) -> Option<u64> {
        match self {
            Self::Logs(chunk) => Some(chunk.commit_seq),
            Self::Flush | Self::Checkpoint(_) | Self::Maintenance(_) => None,
        }
    }
}

/// A storage-ready batch of log records with its origin.
///
/// `commit_seqs` lists every durability ticket folded into this batch (sorted
/// ascending). Merging chunks of one origin folds their tickets together, so
/// the writer must settle *all* of them once the transaction commits —
/// claiming only the maximum would strand the intermediate tickets on the
/// commit watermark forever.
#[derive(Debug, Clone, PartialEq)]
pub struct LogWriteBatch {
    pub origin: BatchOrigin,
    pub records: WriteBatch<LogRecord>,
    pub commit_seqs: Vec<u64>,
}

/// Commands consumed by the single SQLite writer. Insert variants carry fully
/// formed storage batches produced by the insert batcher; the writer persists
/// each of them in exactly one transaction without further accumulation.
#[derive(Debug, Clone, PartialEq)]
pub enum WriteCommand {
    InsertLogs(LogWriteBatch),
    Flush,
    Checkpoint(CheckpointMode),
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
            Self::Checkpoint(_) => "checkpoint",
            Self::Maintenance(_) => "maintenance",
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

#[derive(Debug, Clone, PartialEq)]
pub enum MaintenanceOperation {
    Analyze,
    Vacuum,
    RebuildFts,
    /// Age-based prune of log events; `None` disables the prune for this
    /// command.
    ///
    /// The SQLite writer interprets the window at execution time (cutoff =
    /// now − window), so a long-queued prune never deletes data newer than
    /// the configured window.
    Prune(Option<Duration>),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Resource;

    fn sample_log_write(records: usize) -> LogWriteBatch {
        LogWriteBatch {
            origin: BatchOrigin {
                resource: Some(Resource::new(vec![("service.name", "test").into()])),
                schema_url: "https://example.test".to_owned(),
            },
            records: WriteBatch::from_vec((0..records).map(|_| LogRecord::default()).collect()),
            commit_seqs: vec![7],
        }
    }

    #[test]
    fn command_kinds() {
        let logs = WriteCommand::InsertLogs(sample_log_write(1));
        assert_eq!(logs.kind(), "insert_logs");

        assert_eq!(WriteCommand::Flush.kind(), "flush");
        assert_eq!(
            WriteCommand::Checkpoint(CheckpointMode::Truncate).kind(),
            "checkpoint"
        );
        assert_eq!(
            WriteCommand::Maintenance(MaintenanceOperation::Vacuum).kind(),
            "maintenance"
        );
    }

    #[test]
    fn record_counts_only_apply_to_inserts() {
        assert_eq!(WriteCommand::Flush.record_count(), 0);
        assert_eq!(
            WriteCommand::Checkpoint(CheckpointMode::Passive).record_count(),
            0
        );
        assert_eq!(
            WriteCommand::InsertLogs(sample_log_write(3)).record_count(),
            3
        );
    }

    #[test]
    fn checkpoint_mode_display_and_default() {
        assert_eq!(CheckpointMode::Full.to_string(), "FULL");
        assert_eq!(CheckpointMode::default(), CheckpointMode::Passive);
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
}
