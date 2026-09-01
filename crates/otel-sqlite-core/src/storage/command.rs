use std::fmt;
use std::time::Duration;

use crate::model::{LogRecord, MetricRecord};

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

impl BatchOrigin {
    pub const fn new() -> Self {
        Self {
            resource: None,
            schema_url: String::new(),
        }
    }
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
    pub fn len(&self) -> usize {
        self.records.len()
    }

    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }
}

/// One mapped group of metric records plus its origin. See [`LogChunk`] for
/// the meaning of `commit_seq`.
#[derive(Debug)]
pub struct MetricChunk {
    pub origin: BatchOrigin,
    pub records: Vec<MetricRecord>,
    pub commit_seq: u64,
}

impl MetricChunk {
    pub fn len(&self) -> usize {
        self.records.len()
    }

    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }
}

/// Input side of the ingestion pipeline: one item on the FIFO stream between
/// ingress and the storage-owned insert batcher.
///
/// Most items are payloads — mapped records ([`LogChunk`]/[`MetricChunk`])
/// that the batcher absorbs into sized [`WriteBatch`]es; their boundaries are
/// *not* SQLite transaction boundaries. The remaining items are barriers
/// (`Flush`, `Checkpoint`, `Maintenance`): instructions the batcher executes
/// in stream order. Both kinds must share one stream because barriers derive
/// their ordering guarantee from interleaving with the payloads they bracket.
#[derive(Debug)]
pub enum IngestMessage {
    /// Mapped log records plus their origin; a payload for the batcher.
    Logs(LogChunk),
    /// Mapped metric records plus their origin; a payload for the batcher.
    Metrics(MetricChunk),
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
            Self::Metrics(chunk) => chunk.commit_seq = commit_seq,
            Self::Flush | Self::Checkpoint(_) | Self::Maintenance(_) => {}
        }
    }

    /// The durability ticket carried by a payload message, if any.
    pub const fn commit_seq(&self) -> Option<u64> {
        match self {
            Self::Logs(chunk) => Some(chunk.commit_seq),
            Self::Metrics(chunk) => Some(chunk.commit_seq),
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

/// A storage-ready batch of metric records with its origin. See
/// [`LogWriteBatch`] for the meaning of `commit_seqs`.
#[derive(Debug, Clone, PartialEq)]
pub struct MetricWriteBatch {
    pub origin: BatchOrigin,
    pub records: WriteBatch<MetricRecord>,
    pub commit_seqs: Vec<u64>,
}

/// Commands consumed by the single SQLite writer. Insert variants carry fully
/// formed storage batches produced by the insert batcher; the writer persists
/// each of them in exactly one transaction without further accumulation.
#[derive(Debug, Clone, PartialEq)]
pub enum WriteCommand {
    InsertLogs(LogWriteBatch),
    InsertMetrics(MetricWriteBatch),
    Flush,
    Checkpoint(CheckpointMode),
    Maintenance(MaintenanceOperation),
}

impl WriteCommand {
    /// Number of records the command would persist (insert commands only).
    pub fn record_count(&self) -> usize {
        match self {
            Self::InsertLogs(batch) => batch.records.len(),
            Self::InsertMetrics(batch) => batch.records.len(),
            _ => 0,
        }
    }

    /// Every durability ticket the command's records carry (insert commands
    /// only). The writer publishes each of them as committed once the
    /// transaction succeeds.
    pub fn commit_seqs(&self) -> &[u64] {
        match self {
            Self::InsertLogs(batch) => &batch.commit_seqs,
            Self::InsertMetrics(batch) => &batch.commit_seqs,
            Self::Flush | Self::Checkpoint(_) | Self::Maintenance(_) => &[],
        }
    }

    /// Rejects insert commands carrying no records; control commands are
    /// always valid.
    ///
    /// # Errors
    /// Returns [`CommandError::EmptyBatch`] for an empty `InsertLogs` or
    /// `InsertMetrics` batch.
    pub fn validate(&self) -> CommandResult<()> {
        match self {
            Self::InsertLogs(batch) if batch.records.is_empty() => Err(CommandError::EmptyBatch),
            Self::InsertMetrics(batch) if batch.records.is_empty() => Err(CommandError::EmptyBatch),
            _ => Ok(()),
        }
    }

    pub const fn kind(&self) -> &'static str {
        match self {
            Self::InsertLogs(_) => "insert_logs",
            Self::InsertMetrics(_) => "insert_metrics",
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
    pub const fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(Self::Passive),
            1 => Some(Self::Full),
            2 => Some(Self::Restart),
            3 => Some(Self::Truncate),
            _ => None,
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

#[derive(Debug, Clone, PartialEq)]
pub enum MaintenanceOperation {
    Analyze,
    Vacuum,
    RebuildFts,
    Prune(RetentionPolicy),
}

/// Per-signal retention windows for a prune command. Each signal is pruned
/// independently; `None` leaves that signal untouched, so a policy may retain
/// metrics for a year while pruning week-old logs.
///
/// The SQLite writer interprets the windows at execution time (cutoff =
/// now − window), so a long-queued prune never deletes data newer than the
/// configured window.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RetentionPolicy {
    /// Log-event retention window; `None` disables log pruning.
    pub logs: Option<Duration>,
    /// Metric data-point retention window; `None` disables metric pruning.
    pub metrics: Option<Duration>,
}

impl RetentionPolicy {
    /// One uniform window applied to every signal.
    pub const fn new(older_than: Duration) -> Self {
        Self {
            logs: Some(older_than),
            metrics: Some(older_than),
        }
    }

    /// Prune only log events.
    pub const fn logs_only(older_than: Duration) -> Self {
        Self {
            logs: Some(older_than),
            metrics: None,
        }
    }

    /// Prune only metric data points.
    pub const fn metrics_only(older_than: Duration) -> Self {
        Self {
            logs: None,
            metrics: Some(older_than),
        }
    }

    /// Whether this policy prunes anything at all.
    pub const fn is_enabled(&self) -> bool {
        self.logs.is_some() || self.metrics.is_some()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandError {
    EmptyBatch,
}

impl fmt::Display for CommandError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyBatch => f.write_str("cannot insert an empty batch"),
        }
    }
}

impl std::error::Error for CommandError {}

pub type CommandResult<T> = Result<T, CommandError>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{MetricRecord, Resource};

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

        let metrics = WriteCommand::InsertMetrics(MetricWriteBatch {
            origin: BatchOrigin::new(),
            records: WriteBatch::from_vec(vec![MetricRecord::default()]),
            commit_seqs: vec![9],
        });
        assert_eq!(metrics.kind(), "insert_metrics");

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
    fn validate_rejects_empty_inserts() {
        let empty = LogWriteBatch {
            origin: BatchOrigin::default(),
            records: WriteBatch::default(),
            commit_seqs: Vec::new(),
        };
        assert_eq!(
            WriteCommand::InsertLogs(empty).validate(),
            Err(CommandError::EmptyBatch)
        );

        assert!(
            WriteCommand::InsertLogs(sample_log_write(2))
                .validate()
                .is_ok()
        );
        assert!(WriteCommand::Flush.validate().is_ok());
        assert!(
            WriteCommand::Maintenance(MaintenanceOperation::Analyze)
                .validate()
                .is_ok()
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
    fn checkpoint_modes_round_trip() {
        assert_eq!(CheckpointMode::from_u8(0), Some(CheckpointMode::Passive));
        assert_eq!(CheckpointMode::from_u8(3), Some(CheckpointMode::Truncate));
        assert_eq!(CheckpointMode::from_u8(4), None);
        assert_eq!(CheckpointMode::Full.to_string(), "FULL");
        assert_eq!(CheckpointMode::default(), CheckpointMode::Passive);
    }

    #[test]
    fn retention_policy_windows_are_per_signal() {
        let uniform = RetentionPolicy::new(Duration::from_secs(60));
        assert_eq!(uniform.logs, Some(Duration::from_secs(60)));
        assert_eq!(uniform.metrics, Some(Duration::from_secs(60)));
        assert!(uniform.is_enabled());

        let logs = RetentionPolicy::logs_only(Duration::from_secs(30));
        assert_eq!(logs.logs, Some(Duration::from_secs(30)));
        assert_eq!(logs.metrics, None);
        assert!(logs.is_enabled());

        let metrics = RetentionPolicy::metrics_only(Duration::from_secs(90));
        assert_eq!(metrics.logs, None);
        assert_eq!(metrics.metrics, Some(Duration::from_secs(90)));
        assert!(metrics.is_enabled());

        assert!(!RetentionPolicy::default().is_enabled());
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

        let metrics = IngestMessage::Metrics(MetricChunk {
            origin: BatchOrigin::default(),
            records: vec![MetricRecord::default()],
            commit_seq: 5,
        });
        assert_eq!(metrics.commit_seq(), Some(5));

        // Barriers carry no records, so they have no ticket.
        let mut flush = IngestMessage::Flush;
        flush.set_commit_seq(11);
        assert_eq!(flush.commit_seq(), None);

        assert_eq!(
            WriteCommand::InsertLogs(sample_log_write(2)).commit_seqs(),
            &[7]
        );
        assert!(WriteCommand::Flush.commit_seqs().is_empty());
    }
}
