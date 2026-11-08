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
//! ──── write boundary (writer may group commands) ──────────────────────
//!     │  WriteCommand::InsertLogs(..)
//!     ▼
//! SQLite writer thread
//! ```
//!
//! * **`LogChunk`** - a mapped group of records as the transport handed it
//!   over. Never sized by the storage layer; its boundaries are OTLP's, not
//!   ours.
//! * **[`WriteBatch`](batch::WriteBatch)** - records grouped *by the storage
//!   layer*, capped at `max_batch_records`. The writer commits it in one
//!   transaction, possibly together with consecutive batches.
//! * **`LogWriteBatch`** - a `WriteBatch` plus the [`BatchOrigin`] needed for
//!   resource attribution at persist time.
//! * **[`IngestMessage`](command::IngestMessage)** vs
//!   **[`WriteCommand`](command::WriteCommand)** - items crossing the ingress
//!   boundary vs commands crossing the write boundary. An ingest message is
//!   *not* a transaction unit; the writer may commit several consecutive
//!   insert commands in one transaction.

pub mod batch;
pub mod batcher;
pub mod command;
pub mod commit;
pub mod durability;

pub use batch::WriteBatch;
pub use batcher::{
    DEFAULT_MAX_INSERT_BATCH_AGE, DEFAULT_MAX_INSERT_BATCH_RECORDS, InsertBatcher,
    InsertBatcherConfig,
};
pub use command::{
    BatchOrigin, CheckpointMode, IngestMessage, LogChunk, LogWriteBatch, MaintenanceOperation,
    RecordSegment, WriteCommand,
};
pub use commit::{
    CommitLedger, CommitLedgerClosed, CommitOutcome, TicketProgress, TicketState, Watermark,
};
pub use durability::{DurabilityMode, SyncMode};
