//! Integration test for the complete maintenance path:
//!
//! ```text
//! MaintenanceWorker ──▶ CommandQueue ──▶ SQLite Writer ──▶ SQLite
//! ```
//!
//! The maintenance worker receives the *producer handle of the same bounded
//! command queue* ingestion uses (`Storage::producer()`), schedules retention
//! prunes with a test-only short interval, and the single SQLite writer
//! executes them. Verified against a real database: expired records are
//! deleted, fresh records survive, and everything shuts down cleanly.

// Integration tests may assert with unwrap(); production code must not.
#![allow(clippy::unwrap_used)]

use std::time::Duration;

use crossbeam_channel::Sender;
use otel_sqlite_core::model::{LogRecord, Severity};
use otel_sqlite_core::storage::{
    BatchOrigin, IngestMessage, InsertBatcherConfig, LogChunk, SyncMode,
};
use otel_sqlite_core::unix_nano_now;
use otel_sqlite_runtime::{MaintenanceConfig, MaintenanceWorker};
use otel_sqlite_storage::{Storage, StorageConfig};
use otel_sqlite_test_support::{open_readonly, wait_until};
use rusqlite::Connection;

fn storage_config(db_path: std::path::PathBuf) -> StorageConfig {
    StorageConfig {
        sqlite_path: db_path,
        insert_batcher: InsertBatcherConfig::new(256, Duration::from_secs(600)),
        command_queue_capacity: 16,
        max_db_bytes: None,
        synchronous: SyncMode::Normal,
        startup_timeout: otel_sqlite_storage::DEFAULT_STARTUP_TIMEOUT,
        shutdown_timeout: otel_sqlite_storage::DEFAULT_SHUTDOWN_TIMEOUT,
    }
}

/// Test-only maintenance configuration: purge every 50 ms, nothing else.
fn maintenance_config() -> MaintenanceConfig {
    MaintenanceConfig {
        retention: Some(Duration::from_secs(3_600)), // 1 hour
        metric_retention: None,
        purge_interval: Some(Duration::from_millis(50)),
        checkpoint_interval: None,
        checkpoint_mode: otel_sqlite_core::storage::CheckpointMode::Passive,
        optimize_interval: None,
        vacuum_interval: None,
        rebuild_fts_interval: None,
        retry_delay: Duration::from_millis(100),
    }
}

/// Two records older than the retention window, one current record.
fn ingest_chunk(now_unix_nano: i64) -> IngestMessage {
    let records = vec![
        log_record("expired-a", now_unix_nano - 6 * 3_600_000_000_000),
        log_record("expired-b", now_unix_nano - 2 * 3_600_000_000_000),
        log_record("current", now_unix_nano),
    ];
    IngestMessage::Logs(LogChunk {
        origin: BatchOrigin::default(),
        records,
        commit_seq: 0,
    })
}

fn log_record(body: &str, time_unix_nano: i64) -> LogRecord {
    LogRecord {
        time_unix_nano,
        observed_time_unix_nano: time_unix_nano,
        severity_number: Severity::Info,
        body: body.to_owned(),
        ..LogRecord::default()
    }
}

fn bodies(conn: &Connection) -> Vec<String> {
    let mut statement = conn
        .prepare("SELECT body FROM log_event ORDER BY id")
        .expect("query prepared");
    statement
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}

#[test]
fn maintenance_worker_purges_expired_records_through_the_real_pipeline() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("maintenance.db");
    let now_unix_nano = unix_nano_now();

    // Start writer + command queue (via Storage). The maintenance worker is
    // started only after the baseline is verified so its prunes cannot race
    // the initial observation.
    let (ingest_tx, receiver) = crossbeam_channel::unbounded();
    let mut storage = Storage::open(receiver, storage_config(db_path.clone())).unwrap();
    let producer: Sender<_> = storage.producer();

    // Ingest two expired and one current record; the Flush barrier makes them
    // observable before any scheduled prune can possibly run.
    ingest_tx.send(ingest_chunk(now_unix_nano)).unwrap();
    ingest_tx.send(IngestMessage::Flush).unwrap();
    assert!(
        wait_until(Duration::from_secs(5), || storage.stats().records_written
            == 3),
        "records must reach SQLite first"
    );
    assert_eq!(bodies(&open_readonly(&db_path)).len(), 3);

    let worker = MaintenanceWorker::new(producer.clone(), maintenance_config()).spawn();

    // The scheduler enqueues a prune; the writer executes it and deletes only
    // the records outside the retention window.
    assert!(
        wait_until(Duration::from_secs(10), || {
            storage.stats().maintenance_runs >= 1
                && bodies(&open_readonly(&db_path)) == vec!["current".to_owned()]
        }),
        "expired records must be purged through the pipeline"
    );

    // Shutdown order: stop scheduling -> drop producer -> drain writer.
    worker.stop().expect("maintenance worker stops cleanly");
    drop(ingest_tx);
    drop(producer);
    storage.join().expect("writer drains and exits");

    // Final state survives shutdown unchanged.
    assert_eq!(bodies(&open_readonly(&db_path)), vec!["current".to_owned()]);
    let stats = storage.stats();
    assert!(stats.maintenance_runs >= 1);
    assert_eq!(stats.errors, 0);
    assert_eq!(stats.dropped_records, 0);
}

#[test]
fn maintenance_worker_survives_writer_shutdown_ordering() {
    // Reverse teardown order check: stopping the worker *after* the ingest
    // channel closed but *before* storage.join() must still drain cleanly —
    // the documented production sequence.
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("ordering.db");

    let (ingest_tx, receiver) = crossbeam_channel::unbounded();
    let mut storage = Storage::open(receiver, storage_config(db_path.clone())).unwrap();
    let producer = storage.producer();
    let worker = MaintenanceWorker::new(producer.clone(), MaintenanceConfig::default()).spawn();

    ingest_tx
        .send(IngestMessage::Logs(LogChunk {
            origin: BatchOrigin::default(),
            records: vec![log_record("kept", unix_nano_now())],
            commit_seq: 0,
        }))
        .unwrap();
    ingest_tx.send(IngestMessage::Flush).unwrap();
    assert!(wait_until(Duration::from_secs(5), || storage
        .stats()
        .records_written
        == 1));

    drop(ingest_tx); // stop ingress first
    worker.stop().expect("maintenance stops cleanly"); // then maintenance
    drop(producer);
    storage.join().expect("then the writer drains");

    assert_eq!(bodies(&open_readonly(&db_path)), vec!["kept".to_owned()]);
}
