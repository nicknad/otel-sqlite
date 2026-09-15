// Unit tests assert invariants with `.unwrap()`; production code must not.
#![cfg_attr(test, allow(clippy::unwrap_used))]

pub mod model;
pub mod storage;
pub mod time;

pub use model::{LogBatch, LogRecord};
pub use storage::{
    BatchOrigin, BatchOutput, CommitLedger, DurabilityMode, IngestMessage, InsertBatcher,
    InsertBatcherConfig, LogChunk, LogWriteBatch, SyncMode, Watermark, WriteBatch, WriteCommand,
};
pub use time::{monotonic_millis, saturating_deadline, unix_nano_now};
