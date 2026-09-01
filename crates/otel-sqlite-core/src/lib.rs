// Unit tests assert invariants with `.unwrap()`; production code must not.
#![cfg_attr(test, allow(clippy::unwrap_used))]

pub mod model;
pub mod storage;
pub mod time;

pub use model::{LogBatch, LogRecord, MetricBatch, MetricRecord};
pub use storage::{
    BatchOrigin, BatchOutput, CommitLedger, DurabilityMode, IngestMessage, InsertBatcher,
    InsertBatcherConfig, LogChunk, LogWriteBatch, MetricChunk, MetricWriteBatch, SyncMode,
    Watermark, WriteBatch, WriteCommand,
};
pub use time::unix_nano_now;
