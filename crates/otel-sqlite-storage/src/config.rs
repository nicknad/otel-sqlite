use std::path::PathBuf;
use std::time::Duration;

use otel_sqlite_core::storage::{InsertBatcherConfig, SyncMode};
use thiserror::Error;

use crate::search::FtsMode;

/// Default database filename used for the live database and backup restores.
pub const DEFAULT_DB_FILENAME: &str = "otel-logs.db";

/// Capacity of the bounded queue carrying `WriteCommand`s from the insert
/// batcher to the SQLite writer.
pub const DEFAULT_COMMAND_QUEUE_CAPACITY: usize = 50_000;

/// Default record ceiling for one coalesced insert transaction.
///
/// The measured profile is roughly `45 µs + 5.7 µs/record`; 1 000 records keeps
/// the worst case near 6 ms while amortising the fixed cost across up to two
/// default-sized batches or hundreds of tiny per-origin batches.
pub const DEFAULT_GROUP_COMMIT_RECORDS: usize = 1_000;

pub const MAX_GROUP_COMMIT_RECORDS: usize = 20_000;

/// Default temporal window a coalesced insert transaction may stay open for
/// straggler commands. `ZERO` (off) keeps the historical behaviour: only
/// commands already queued are grouped, collection never waits.
pub const DEFAULT_GROUP_COMMIT_LINGER: Duration = Duration::ZERO;

/// Largest accepted `group_commit_linger`. One second is already 200x the
/// largest measured-beneficial window (5 ms) and stays orders of magnitude
/// below every watchdog threshold, so a mid-group wait can never be mistaken
/// for a stalled pipeline.
pub const MAX_GROUP_COMMIT_LINGER: Duration = Duration::from_secs(1);

/// Largest accepted `batcher_max_batch_records`. Batch capacity beyond this
/// point multiplies flush latency and writer transaction size for no measured
/// throughput gain (see core's `DEFAULT_MAX_INSERT_BATCH_RECORDS`).
pub const MAX_BATCH_RECORDS: usize = 100_000;

/// Largest accepted `command_queue_capacity`. The queue is the pipeline's
/// backpressure buffer; deeper queues hide saturation longer and cost memory
/// per slot.
pub const MAX_COMMAND_QUEUE_CAPACITY: usize = 1_000_000;

/// Minimum accepted `max_db_bytes` quota: below 1 MiB the writer would evict
/// on nearly every batch (a fresh migrated database already exceeds it).
pub const MIN_DB_QUOTA_BYTES: u64 = 1024 * 1024;

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
    /// Maximum records the writer may assemble into one insert transaction.
    /// `0` restores the one-transaction-per-command behavior. Consecutive
    /// insert commands already in the queue are coalesced; collection never
    /// waits to fill a group.
    pub group_commit_max_records: usize,
    /// Fixed temporal window an under-budget insert group may stay open for
    /// straggler commands. `ZERO` (the default) disables it: collection never
    /// waits. When set, the window opens when the group forms and is never
    /// reset by later arrivals; the added latency lands only on
    /// `durability = "commit"` acknowledgements, which wait for the
    /// transaction, and boundaries (flush, maintenance, shutdown) plus the
    /// record budget still close a group immediately.
    pub group_commit_linger: Duration,
    /// Installed search-index policy. The index is derived data, so the
    /// configured mode is reconciled at startup and never blocks readiness.
    pub fts_mode: FtsMode,
    /// Optional on-disk size quota in bytes (`None` = unbounded, the default).
    /// When set, the writer checks the database file size before each insert
    /// batch and evicts the oldest rows (size-based retention: newest survive)
    /// until back under quota. Bounds disk growth from high-cardinality or
    /// high-volume senders when time-based retention is `None` or too wide.
    pub max_db_bytes: Option<u64>,
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
            sqlite_path: std::path::PathBuf::from(DEFAULT_DB_FILENAME),
            insert_batcher: InsertBatcherConfig::default(),
            command_queue_capacity: DEFAULT_COMMAND_QUEUE_CAPACITY,
            group_commit_max_records: DEFAULT_GROUP_COMMIT_RECORDS,
            group_commit_linger: DEFAULT_GROUP_COMMIT_LINGER,
            fts_mode: FtsMode::default(),
            max_db_bytes: None,
            synchronous: SyncMode::default(),
            startup_timeout: DEFAULT_STARTUP_TIMEOUT,
            shutdown_timeout: DEFAULT_SHUTDOWN_TIMEOUT,
        }
    }
}

/// A [`StorageConfig`] that cannot back a healthy pipeline.
#[derive(Debug, Clone, PartialEq, Error)]
pub enum StorageConfigError {
    /// A knob sits outside the range this crate publishes for it.
    #[error("config key `{key}` must be in the range {minimum}..={maximum}; got {value}")]
    OutOfRange {
        key: &'static str,
        minimum: u64,
        maximum: u64,
        value: u64,
    },
    /// The size quota is smaller than a freshly migrated database.
    #[error(
        "config key `max_db_bytes` must be at least {MIN_DB_QUOTA_BYTES} bytes (1 MiB); got {value}"
    )]
    QuotaTooSmall { value: u64 },
    /// A zero batch age would flush every non-empty buffer immediately.
    #[error("config key `batcher_max_batch_age` must be greater than zero")]
    ZeroBatchAge,
    /// The linger window exceeds the largest accepted value.
    #[error(
        "config key `group_commit_linger` must be no more than {MAX_GROUP_COMMIT_LINGER:?}; got {value:?}"
    )]
    LingerTooLarge { value: Duration },
}

impl StorageConfig {
    /// Rejects values that would stall or thrash the single-writer pipeline:
    /// an empty insert batch, a zero batch age, an empty or oversized command
    /// queue, a group-commit ceiling out of range, a linger window past its
    /// ceiling, or a quota the writer would evict against on nearly every
    /// batch.
    ///
    /// The binary applies the same rules while loading its config file;
    /// [`crate::Storage::open`] re-checks the rules a direct constructor
    /// could otherwise smuggle past an embedder, keeping this type the
    /// single owner of every storage invariant.
    pub fn validate(&self) -> Result<(), StorageConfigError> {
        Self::ensure_range(
            "batcher_max_batch_records",
            self.insert_batcher.max_batch_records,
            1,
            MAX_BATCH_RECORDS,
        )?;
        if self.insert_batcher.max_batch_age.is_zero() {
            return Err(StorageConfigError::ZeroBatchAge);
        }
        Self::ensure_range(
            "command_queue_capacity",
            self.command_queue_capacity,
            1,
            MAX_COMMAND_QUEUE_CAPACITY,
        )?;
        Self::ensure_range(
            "group_commit_max_records",
            self.group_commit_max_records,
            0,
            MAX_GROUP_COMMIT_RECORDS,
        )?;
        if self.group_commit_linger > MAX_GROUP_COMMIT_LINGER {
            return Err(StorageConfigError::LingerTooLarge {
                value: self.group_commit_linger,
            });
        }
        if let Some(quota) = self.max_db_bytes
            && quota < MIN_DB_QUOTA_BYTES
        {
            return Err(StorageConfigError::QuotaTooSmall { value: quota });
        }
        Ok(())
    }

    /// Range check shared by every storage knob.
    fn ensure_range(
        key: &'static str,
        value: usize,
        minimum: usize,
        maximum: usize,
    ) -> Result<(), StorageConfigError> {
        if value < minimum || value > maximum {
            return Err(StorageConfigError::OutOfRange {
                key,
                minimum: minimum as u64,
                maximum: maximum as u64,
                value: value as u64,
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_is_valid() {
        StorageConfig::default()
            .validate()
            .expect("defaults must be usable");
    }

    #[test]
    fn direct_construction_rejects_out_of_range_knobs() {
        macro_rules! reject {
            ($config:expr) => {
                assert!(
                    $config.validate().is_err(),
                    "direct construction must reject nonsense values"
                );
            };
        }
        reject!(StorageConfig {
            insert_batcher: InsertBatcherConfig {
                max_batch_records: 0,
                max_batch_age: Duration::from_millis(10),
            },
            ..StorageConfig::default()
        });
        reject!(StorageConfig {
            insert_batcher: InsertBatcherConfig {
                max_batch_records: MAX_BATCH_RECORDS + 1,
                max_batch_age: Duration::from_millis(10),
            },
            ..StorageConfig::default()
        });
        reject!(StorageConfig {
            insert_batcher: InsertBatcherConfig {
                max_batch_records: 500,
                max_batch_age: Duration::ZERO,
            },
            ..StorageConfig::default()
        });
        reject!(StorageConfig {
            command_queue_capacity: 0,
            ..StorageConfig::default()
        });
        reject!(StorageConfig {
            command_queue_capacity: MAX_COMMAND_QUEUE_CAPACITY + 1,
            ..StorageConfig::default()
        });
        reject!(StorageConfig {
            group_commit_max_records: MAX_GROUP_COMMIT_RECORDS + 1,
            ..StorageConfig::default()
        });
        reject!(StorageConfig {
            group_commit_linger: Duration::from_secs(2),
            ..StorageConfig::default()
        });
        reject!(StorageConfig {
            max_db_bytes: Some(1024),
            ..StorageConfig::default()
        });
    }

    #[test]
    fn group_commit_zero_disables_and_validates() {
        StorageConfig {
            group_commit_max_records: 0,
            ..StorageConfig::default()
        }
        .validate()
        .expect("0 disables grouping and must be valid");
    }

    #[test]
    fn linger_window_validates_between_zero_and_its_ceiling() {
        let error = StorageConfig {
            group_commit_linger: Duration::from_secs(2),
            ..StorageConfig::default()
        }
        .validate()
        .expect_err("a window past its ceiling must fail");
        assert!(error.to_string().contains("group_commit_linger"), "{error}");

        StorageConfig {
            group_commit_linger: MAX_GROUP_COMMIT_LINGER,
            ..StorageConfig::default()
        }
        .validate()
        .expect("the ceiling itself is valid");
    }

    #[test]
    fn quota_floor_is_accepted() {
        StorageConfig {
            max_db_bytes: Some(MIN_DB_QUOTA_BYTES),
            ..StorageConfig::default()
        }
        .validate()
        .expect("the 1 MiB quota floor is valid");
    }

    #[test]
    fn validate_errors_name_the_key_and_the_range() {
        let error = StorageConfig {
            insert_batcher: InsertBatcherConfig {
                max_batch_records: 0,
                max_batch_age: Duration::from_millis(10),
            },
            ..StorageConfig::default()
        }
        .validate()
        .expect_err("a zero batch must fail");
        let message = error.to_string();
        assert!(message.contains("batcher_max_batch_records"), "{message}");
        assert!(
            message.contains("1..=") && message.contains("got 0"),
            "{message}"
        );
    }
}
