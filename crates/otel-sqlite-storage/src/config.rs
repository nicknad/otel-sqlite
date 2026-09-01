use std::path::PathBuf;
use std::time::Duration;

use otel_sqlite_core::storage::{InsertBatcherConfig, SyncMode};

/// Capacity of the bounded queue carrying `WriteCommand`s from the insert
/// batcher to the SQLite writer.
pub const DEFAULT_COMMAND_QUEUE_CAPACITY: usize = 50_000;

/// How long [`crate::Storage::open`] waits for the writer thread to finish
/// bootstrapping (open + migrate) before giving up.
pub const DEFAULT_STARTUP_TIMEOUT: Duration = Duration::from_secs(30);

/// How long [`crate::Storage::join_timeout`] waits for the batcher and writer
/// to drain and exit before giving up. A wedged SQLite (stalled I/O, giant
/// checkpoint) must not hang process shutdown forever.
pub const DEFAULT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Clone)]
pub struct StorageConfig {
    pub sqlite_path: PathBuf,
    /// Insert-batching policy owned by the storage layer. OTLP batch
    /// boundaries never reach SQLite as transaction boundaries: the batcher
    /// groups mapped records into storage-sized write batches.
    pub insert_batcher: InsertBatcherConfig,
    /// Bounded command queue depth between the insert batcher and the single
    /// SQLite writer. A full queue applies backpressure to ingestion instead
    /// of dropping completed batches.
    pub command_queue_capacity: usize,
    pub retention: Option<Duration>,
    /// SQLite `synchronous` level for the writer connection. `Normal` (the
    /// default) syncs WAL at checkpoints; `Full` fsyncs every commit.
    pub synchronous: SyncMode,
    /// Budget for the writer's startup bootstrap (open + migrations).
    /// `Storage::open` fails once it is exceeded, so a broken storage
    /// backend surfaces at boot instead of on first traffic.
    pub startup_timeout: Duration,
    /// Budget for graceful shutdown: batcher flush + writer drain + final
    /// checkpoint. [`crate::Storage::join_timeout`] reports
    /// [`crate::StorageError::ShutdownTimeout`] once it is exceeded.
    pub shutdown_timeout: Duration,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            sqlite_path: std::path::PathBuf::from("otel-logs.db"),
            insert_batcher: InsertBatcherConfig::default(),
            command_queue_capacity: DEFAULT_COMMAND_QUEUE_CAPACITY,
            retention: None,
            synchronous: SyncMode::default(),
            startup_timeout: DEFAULT_STARTUP_TIMEOUT,
            shutdown_timeout: DEFAULT_SHUTDOWN_TIMEOUT,
        }
    }
}
