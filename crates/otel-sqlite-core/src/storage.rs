//! Storage-owned batching and persistence vocabulary.
//!
//! ## Naming legend
//!
//! The pipeline crosses two boundaries, and every type below belongs to
//! exactly one side of one boundary. The nouns are chosen to make the stage
//! obvious:
//!
//! ```text
//! OTLP request (transport-shaped, arbitrary size)
//!     │  mapping
//!     ▼
//! ──── ingress boundary ────────────────────────────────────────────────
//!     │  IngestMessage::Logs(LogChunk)
//!     ▼
//! InsertBatcher  (max_batch_records / max_batch_age)
//!     │  emits WriteBatch<T> wrapped in LogWriteBatch
//!     ▼
//! ──── write boundary (one command == one SQLite transaction) ─────────
//!     │  WriteCommand::InsertLogs(..)
//!     ▼
//! SQLite writer thread
//! ```
//!
//! * **`LogChunk`** - a mapped group of records as the transport handed it
//!   over. Never sized by the storage layer; its boundaries are OTLP's, not
//!   ours.
//! * **[`WriteBatch`](batch::WriteBatch)** - records grouped *by the storage
//!   layer*, capped at `max_batch_records`. This is what becomes exactly one
//!   transaction.
//! * **`LogWriteBatch`** - a `WriteBatch` plus the [`BatchOrigin`] needed for
//!   resource attribution at persist time.
//! * **[`IngestMessage`](command::IngestMessage)** vs
//!   **[`WriteCommand`](command::WriteCommand)** - items crossing the ingress
//!   boundary vs commands crossing the write boundary. An ingest message is
//!   *not* a transaction unit; a write command is.
//!
//! The model type [`LogBatch`](crate::model::LogBatch) is a mapping-internal
//! helper (records + origin in one struct) and does not appear on either
//! queue.

pub mod batch;
pub mod batcher;
pub mod command;
pub mod commit;
pub mod durability;

pub use batch::WriteBatch;
pub use batcher::{
    BatchOutput, DEFAULT_MAX_INSERT_BATCH_AGE, DEFAULT_MAX_INSERT_BATCH_RECORDS, InsertBatcher,
    InsertBatcherConfig,
};
pub use command::{
    BatchOrigin, CheckpointMode, CommandError, CommandResult, IngestMessage, LogChunk,
    LogWriteBatch, MaintenanceOperation, WriteCommand,
};
pub use commit::{CommitLedger, CommitLedgerClosed, Watermark};
pub use durability::{DurabilityMode, SyncMode};
