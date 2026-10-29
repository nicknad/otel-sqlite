//! Regression: a drained, idle pipeline must never be halted.
//!
//! The watchdog's "WORK BUT NO PROGRESS" rule depends on queue-depth evidence.
//! A stale producer-side snapshot used to keep reporting the last observed
//! backlog after the queue drained, so an idle pipeline looked permanently
//! stalled and was halted with `halt_on_unhealthy = true`.

// Integration tests may assert with unwrap(); production code must not.
#![allow(clippy::unwrap_used)]

use std::time::Duration;

use otel_sqlite_core::PipelineSample;
use otel_sqlite_core::model::{LogRecord, Severity};
use otel_sqlite_core::storage::{
    BatchOrigin, IngestMessage, InsertBatcherConfig, LogChunk, SyncMode,
};
use otel_sqlite_runtime::{HealthSampleSource, Watchdog, WatchdogConfig};
use otel_sqlite_storage::{Storage, StorageConfig};
use otel_sqlite_test_support::wait_until;

struct StorageSource(otel_sqlite_storage::StorageHealth);

impl HealthSampleSource for StorageSource {
    fn sample(&self) -> PipelineSample {
        self.0.sample()
    }
}

fn storage_config(db_path: std::path::PathBuf) -> StorageConfig {
    StorageConfig {
        sqlite_path: db_path,
        // One record per command and a small queue keep the producer backlog
        // observable at the moment the last batch is submitted.
        insert_batcher: InsertBatcherConfig::new(1, Duration::from_secs(600)),
        command_queue_capacity: 8,
        max_db_bytes: None,
        synchronous: SyncMode::Normal,
        startup_timeout: otel_sqlite_storage::DEFAULT_STARTUP_TIMEOUT,
        shutdown_timeout: otel_sqlite_storage::DEFAULT_SHUTDOWN_TIMEOUT,
    }
}

fn logs_chunk(first_seq: i64, count: usize) -> IngestMessage {
    let records = (0..count)
        .map(|offset| {
            let seq = first_seq + offset as i64;
            LogRecord {
                time_unix_nano: seq,
                observed_time_unix_nano: seq,
                severity_number: Severity::Info,
                body: format!("record-{seq}"),
                ..LogRecord::default()
            }
        })
        .collect();
    IngestMessage::Logs(LogChunk {
        origin: BatchOrigin::default(),
        records,
        commit_seq: 0,
    })
}

#[test]
fn idle_pipeline_after_a_burst_is_not_halted() {
    let dir = tempfile::tempdir().unwrap();
    let (sender, receiver) = crossbeam_channel::bounded(64);
    let mut storage = Storage::open(receiver, storage_config(dir.path().join("idle.db"))).unwrap();

    for chunk in 0..200 {
        sender.send(logs_chunk(chunk * 10, 10)).unwrap();
    }
    assert!(
        wait_until(Duration::from_secs(30), || {
            storage.stats().records_written == 2_000
        }),
        "every record must be persisted before the idle check"
    );

    // Fully drained: no queued/executing commands, nothing buffered.
    assert!(
        wait_until(Duration::from_secs(5), || {
            storage.health().sample().pending_work() == 0
        }),
        "drained pipeline still reports pending work: {:?}",
        storage.health().sample()
    );

    let (halt_tx, mut halt_rx) = tokio::sync::watch::channel(false);
    let config = WatchdogConfig {
        tick_interval: Duration::from_millis(20),
        degraded_after: Duration::from_millis(50),
        unhealthy_after: Duration::from_millis(150),
        ack_stall_after: Duration::from_secs(60),
        maintenance_stall_after: Duration::from_secs(60),
        queue_pressure_ratio: 0.75,
        halt_on_unhealthy: true,
    };
    let watchdog = Watchdog::new(StorageSource(storage.health()), config, halt_tx)
        .expect("valid watchdog config")
        .spawn();

    // Several ticks of idleness: the stale-depth bug halted within ~150 ms.
    std::thread::sleep(Duration::from_millis(600));
    assert!(!watchdog.requested_halt(), "idle pipeline was halted");
    assert!(
        !*halt_rx.borrow_and_update(),
        "idle pipeline must not signal a halt"
    );

    watchdog.stop().expect("watchdog stops cleanly");
    drop(sender);
    storage.join().expect("writer drains and exits");
}
