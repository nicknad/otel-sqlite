//! End-to-end pipeline tests over the public `Storage` API:
//!
//! ```text
//! multiple mapped OTLP chunks
//!         |
//!         v
//! InsertBatcher (storage-owned, max_batch_records / max_batch_age)
//!         |
//!         v
//! bounded WriteCommand queue
//!         |
//!         v
//! single SQLite writer thread
//!         |
//!         v
//! SQLite
//! ```
//!
//! Verified here against a real database: completeness (nothing lost),
//! uniqueness (nothing duplicated), storage-batch splitting at the configured
//! capacity, timer-driven flushes of partial batches, ordering guarantees,
//! deterministic shutdown draining, and backpressure without silent drops.

// Integration tests may assert with unwrap(); production code must not.
#![allow(clippy::unwrap_used)]

use std::time::Duration;

use crossbeam_channel::TrySendError;
use otel_sqlite_core::model::{
    Gauge, LogRecord, MetricData, MetricRecord, NumberDataPoint, NumberValue, Resource, Severity,
};
use otel_sqlite_core::storage::{
    BatchOrigin, IngestMessage, InsertBatcherConfig, LogChunk, SyncMode,
};
use otel_sqlite_storage::{Storage, StorageConfig};
use otel_sqlite_test_support::{open_readonly, wait_until};
use rusqlite::Connection;

fn config(
    db_path: std::path::PathBuf,
    max_batch_records: usize,
    max_batch_age_ms: u64,
) -> StorageConfig {
    StorageConfig {
        sqlite_path: db_path,
        insert_batcher: InsertBatcherConfig::new(
            max_batch_records,
            Duration::from_millis(max_batch_age_ms),
        ),
        command_queue_capacity: 16,
        max_db_bytes: None,
        synchronous: SyncMode::Normal,
        startup_timeout: otel_sqlite_storage::DEFAULT_STARTUP_TIMEOUT,
        shutdown_timeout: otel_sqlite_storage::DEFAULT_SHUTDOWN_TIMEOUT,
    }
}

/// Chunk of log records whose sequence numbers are encoded in both the body
/// and the timestamp so completeness, uniqueness and ordering are observable
/// in SQLite.
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
        origin: BatchOrigin {
            resource: None,
            schema_url: "https://example.test/schemas".to_owned(),
        },
        records,
        commit_seq: 0,
    })
}

/// A poison data point (CHECK start<=timestamp violated) must not kill the
/// pipeline: the strict pass fails, the salvage pass commits healthy points,
/// the offending ones are counted as quarantined, and the writer keeps
/// serving subsequent traffic.
#[test]
fn poison_metric_point_is_quarantined_and_healthy_rows_survive() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("poison.db");

    let (sender, receiver) = crossbeam_channel::bounded(4);
    let mut storage = Storage::open(receiver, config(db_path.clone(), 64, 60_000)).unwrap();
    let ledger = storage.commit_ledger();

    let good = NumberDataPoint {
        start_time_unix_nano: 100,
        time_unix_nano: 200,
        value: Some(NumberValue::Int(7)),
        ..NumberDataPoint::default()
    };
    // CHECK (start_timestamp_ns <= timestamp_ns) violated on purpose.
    let poison = NumberDataPoint {
        start_time_unix_nano: 900,
        time_unix_nano: 200,
        value: Some(NumberValue::Int(8)),
        ..NumberDataPoint::default()
    };

    let record = MetricRecord {
        name: "poison.test".to_owned(),
        scope_name: "scope".to_owned(),
        data: MetricData::Gauge(Gauge {
            data_points: vec![poison, good],
        }),
        ..MetricRecord::default()
    };

    let ticket = ledger.issue();
    sender
        .send(IngestMessage::Metrics(
            otel_sqlite_core::storage::MetricChunk {
                origin: BatchOrigin::default(),
                records: vec![record],
                commit_seq: ticket,
            },
        ))
        .unwrap();
    sender.send(IngestMessage::Flush).unwrap();

    assert!(
        wait_until(Duration::from_secs(5), || storage.stats().records_written
            == 1),
        "the healthy point must be persisted by the salvage pass"
    );
    let stats = storage.stats();
    assert_eq!(stats.quarantined_records, 1, "the poison point is counted");
    assert_eq!(stats.errors, 1, "the failed strict attempt is accounted");
    assert_eq!(ledger.watermark().committed_through, ticket);

    // The writer survived: subsequent traffic still flows.
    let follow_up_chunk = logs_chunk(0, 3);
    sender.send(follow_up_chunk).unwrap();
    sender.send(IngestMessage::Flush).unwrap();
    assert!(
        wait_until(Duration::from_secs(5), || storage.stats().records_written
            == 4),
        "writer must keep serving after a salvage"
    );

    drop(sender);
    storage.join().unwrap();

    let conn = open_readonly(&db_path);
    assert_eq!(row_count(&conn), 3, "only log rows + healthy metric remain");
    let metrics_left: i64 = conn
        .query_row("SELECT COUNT(*) FROM metric_data_point", [], |r| r.get(0))
        .unwrap();
    assert_eq!(
        metrics_left, 1,
        "exactly the healthy point survives; the poison point was dropped"
    );
}

/// Unwraps the payload message built by [`logs_chunk`], so durability tickets
/// can be stamped onto the chunk the way ingress does before enqueueing.
fn into_log_chunk(message: IngestMessage) -> LogChunk {
    let IngestMessage::Logs(chunk) = message else {
        unreachable!("logs_chunk builds a payload message")
    };
    chunk
}

/// Chunk whose origin carries a concrete `service.name` resource attribute.
fn logs_chunk_with_service(service: &str, first_seq: i64, count: usize) -> IngestMessage {
    let mut message = logs_chunk(first_seq, count);
    if let IngestMessage::Logs(chunk) = &mut message {
        chunk.origin.resource = Some(Resource::new(vec![
            ("service.name", service.to_owned()).into(),
        ]));
    }
    message
}

fn row_count(conn: &Connection) -> i64 {
    conn.query_row("SELECT COUNT(*) FROM log_event", [], |row| row.get(0))
        .expect("row count")
}

// ---------------------------------------------------------------------------
// Integration: batching correctness through SQLite
// ---------------------------------------------------------------------------

#[test]
fn pipeline_splits_combines_and_persists_every_record_exactly_once() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("pipeline.db");

    // Capacity 100 forces splits and combinations across the chunk sizes.
    let (sender, receiver) = crossbeam_channel::bounded(8);
    let mut storage = Storage::open(receiver, config(db_path.clone(), 100, 600_000)).unwrap();

    let chunk_sizes = [70usize, 150, 250, 90];
    let total: usize = chunk_sizes.iter().sum(); // 560 records

    let mut next_seq = 0i64;
    for size in chunk_sizes {
        sender.send(logs_chunk(next_seq, size)).unwrap();
        next_seq += size as i64;
    }

    // Barrier: everything above must be persisted before we observe.
    sender.send(IngestMessage::Flush).unwrap();
    assert!(
        wait_until(Duration::from_secs(5), || {
            storage.stats().records_written == total as u64
        }),
        "records must reach SQLite, got {}",
        storage.stats().records_written
    );

    // 70 buffers; 150 completes a full batch (100) and buffers 20; 250
    // completes two more; the first 30 of the 90 complete another, leaving
    // 60 buffered. Five full batches (500) plus the Flush barrier handing
    // over the 60-record partial: six write batches in total.
    let stats = storage.stats();
    assert_eq!(stats.records_written, total as u64);
    assert_eq!(stats.write_batches_emitted, 6);
    // Every emitted write batch becomes exactly one intended transaction.
    assert_eq!(stats.transactions_committed, 6);
    assert_eq!(stats.dropped_records, 0);

    // Graceful shutdown with an empty buffer changes nothing.
    drop(sender);
    storage.join().unwrap();

    let stats = storage.stats();
    assert_eq!(stats.records_written, total as u64);
    assert_eq!(stats.write_batches_emitted, 6);
    assert_eq!(stats.transactions_committed, 6);

    let conn = open_readonly(&db_path);
    assert_eq!(row_count(&conn), total as i64);

    // No duplicates: bodies carry unique sequence numbers.
    let distinct_bodies: i64 = conn
        .query_row("SELECT COUNT(DISTINCT body) FROM log_event", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(distinct_bodies, total as i64, "no duplicated records");

    // Ordering: insertion (rowid) order matches sequence order exactly.
    let mut statement = conn
        .prepare("SELECT timestamp_ns FROM log_event ORDER BY id")
        .unwrap();
    let sequences: Vec<i64> = statement
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(sequences, (0..total as i64).collect::<Vec<_>>());
}

#[test]
fn low_volume_partial_batch_is_flushed_by_max_batch_age() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("timer.db");

    let (sender, receiver) = crossbeam_channel::unbounded();
    let mut storage = Storage::open(receiver, config(db_path.clone(), 10_000, 40)).unwrap();

    // Far below capacity: only the age deadline can release these records.
    sender.send(logs_chunk(0, 7)).unwrap();

    let flushed = wait_until(Duration::from_secs(2), || {
        let stats = storage.stats();
        stats.timer_flushes >= 1 && stats.records_written == 7
    });
    assert!(
        flushed,
        "partial batch must flush after max_batch_age without further input"
    );

    // The records are durable while the producer is still connected.
    {
        let conn = open_readonly(&db_path);
        assert_eq!(row_count(&conn), 7);
    }

    drop(sender);
    storage.join().unwrap();

    let stats = storage.stats();
    assert_eq!(stats.records_written, 7);
    assert_eq!(stats.transactions_committed, 1);
}

#[test]
fn writer_awaits_when_queue_is_empty_and_commits_one_transaction_per_write_batch() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("writer.db");

    let (sender, receiver) = crossbeam_channel::unbounded();
    let mut storage = Storage::open(receiver, config(db_path.clone(), 1_000, 60_000)).unwrap();

    // Idle pipeline: the writer must block on recv (no work, no commits).
    std::thread::sleep(Duration::from_millis(120));
    let stats = storage.stats();
    assert_eq!(stats.transactions_committed, 0);
    assert_eq!(stats.queue_depth, 0);

    // One chunk below capacity stays buffered until the Flush barrier hands
    // it over: exactly one write batch -> exactly one transaction.
    sender.send(logs_chunk(0, 3)).unwrap();
    sender.send(IngestMessage::Flush).unwrap();
    assert!(
        wait_until(Duration::from_secs(2), || storage.stats().records_written
            == 3),
        "flushed records must be committed"
    );

    let stats = storage.stats();
    assert_eq!(stats.chunks_ingested, 1);
    assert_eq!(stats.write_batches_emitted, 1);
    assert_eq!(stats.batches_received, 1);
    assert_eq!(stats.transactions_committed, 1);

    drop(sender);
    storage.join().unwrap();
}

#[test]
fn shutdown_drains_buffered_and_queued_records() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("shutdown.db");

    let (sender, receiver) = crossbeam_channel::bounded(8);
    let mut storage = Storage::open(receiver, config(db_path.clone(), 1_000, 600_000)).unwrap();

    // 400 buffers; 700 completes a full batch of 1000 and leaves 100 more;
    // those 100 stay queued/buffered across shutdown (age far in the future).
    sender.send(logs_chunk(0, 400)).unwrap();
    sender.send(logs_chunk(400, 700)).unwrap();
    // No Flush, no timer expiry: only graceful shutdown can drain the rest.

    // Wait until the first full batch is committed, proving the queue carried
    // completed commands while another partial existed.
    assert!(wait_until(Duration::from_secs(2), || {
        storage.stats().records_written == 1_000
    }));

    drop(sender);
    storage.join().unwrap();

    let stats = storage.stats();
    assert_eq!(stats.records_written, 1_100);
    assert_eq!(stats.dropped_records, 0);
    assert_eq!(stats.errors, 0);

    let conn = open_readonly(&db_path);
    assert_eq!(row_count(&conn), 1_100);
    let distinct: i64 = conn
        .query_row("SELECT COUNT(DISTINCT body) FROM log_event", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(distinct, 1_100);
}

#[test]
fn backpressure_through_bounded_queues_never_drops_accepted_records() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("backpressure.db");

    // Deliberately tiny queues: a slow SQLite writer cannot be compensated by
    // dropping work because both boundaries apply backpressure instead.
    let (sender, receiver) = crossbeam_channel::bounded(4);
    let storage_config = StorageConfig {
        command_queue_capacity: 2,
        ..config(db_path.clone(), 50, 60_000)
    };
    let mut storage = Storage::open(receiver, storage_config).unwrap();

    let chunk_size = 25usize;
    let mut accepted_chunks = 0usize;
    for _ in 0..80 {
        match sender.try_send(logs_chunk(
            (accepted_chunks * chunk_size) as i64,
            chunk_size,
        )) {
            Ok(()) => accepted_chunks += 1,
            Err(TrySendError::Full(_)) => {
                // OTLP-level backpressure: this chunk is rejected wholesale
                // (surfaced to the client as UNAVAILABLE in production) and
                // never half-applied. Stop offering and let the pipeline drain.
                break;
            }
            Err(TrySendError::Disconnected(_)) => panic!("storage closed early"),
        }
    }

    sender.send(IngestMessage::Flush).unwrap();
    let accepted = accepted_chunks * chunk_size;
    assert!(
        wait_until(Duration::from_secs(10), || storage.stats().records_written
            == accepted as u64),
        "every accepted record must persist: expected {accepted}, got {}",
        storage.stats().records_written
    );

    drop(sender);
    storage.join().unwrap();

    let stats = storage.stats();
    assert_eq!(stats.records_written, accepted as u64);
    assert_eq!(stats.dropped_records, 0);
    assert_eq!(stats.errors, 0);

    let conn = open_readonly(&db_path);
    assert_eq!(row_count(&conn), accepted as i64);
}

#[test]
fn mixed_origins_are_all_persisted_without_mixing_resources() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("mixed.db");

    let (sender, receiver) = crossbeam_channel::bounded(8);
    let mut storage = Storage::open(receiver, config(db_path.clone(), 32, 60_000)).unwrap();

    // Alternating origins must never merge into one resource attribution.
    for round in 0..6i64 {
        let service = if round % 2 == 0 { "svc-a" } else { "svc-b" };
        sender
            .send(logs_chunk_with_service(service, round * 10, 10))
            .unwrap();
    }
    sender.send(IngestMessage::Flush).unwrap();

    assert!(wait_until(Duration::from_secs(5), || {
        storage.stats().records_written == 60
    }));

    drop(sender);
    storage.join().unwrap();

    let conn = open_readonly(&db_path);
    assert_eq!(row_count(&conn), 60);

    // Every record's resource fingerprint must resolve to its own origin's
    // service name: even sequence decades belong to svc-a, odd ones to svc-b.
    let mismatches: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM logs \
             WHERE ((CAST(timestamp_ns AS INTEGER) / 10) % 2 = 0 AND service_name <> 'svc-a') \
                OR ((CAST(timestamp_ns AS INTEGER) / 10) % 2 = 1 AND service_name <> 'svc-b')",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        mismatches, 0,
        "records must stay attributed to their origin"
    );

    // Two distinct resources exist and nothing else.
    let services: i64 = conn
        .query_row(
            "SELECT COUNT(DISTINCT service_name) FROM log_resource",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(services, 2);
}

/// Durable-ack contract: tickets stamped onto accepted chunks are only
/// released by the writer's actual transactions, and the watermark covers
/// every accepted ticket once the pipeline drains.
#[test]
fn commit_watermark_tracks_accepted_tickets_through_the_real_pipeline() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("watermark.db");

    let (sender, receiver) = crossbeam_channel::bounded(8);
    let mut storage = Storage::open(receiver, config(db_path.clone(), 32, 60_000)).unwrap();
    let ledger = storage.commit_ledger();

    // Nothing accepted yet: the watermark sits at zero and stays there.
    assert_eq!(ledger.watermark().committed_through, 0);
    assert!(!ledger.watermark().closed);

    // Stamp chunks with real ledger tickets the way ingress does.
    let mut last_ticket = 0u64;
    for round in 0..4i64 {
        let mut chunk = into_log_chunk(logs_chunk(round * 10, 10));
        chunk.commit_seq = ledger.issue();
        last_ticket = chunk.commit_seq;
        sender.send(IngestMessage::Logs(chunk)).unwrap();

        // The writer may or may not have caught up, but the watermark can
        // never claim a ticket that was not issued-and-sent yet.
        assert!(ledger.watermark().committed_through <= last_ticket);
    }

    sender.send(IngestMessage::Flush).unwrap();
    assert!(
        wait_until(Duration::from_secs(5), || {
            storage.stats().records_written == 40
        }),
        "all records must be persisted before the watermark check"
    );

    // Every accepted ticket is now committed: a durable ack waiting on any
    // of them must resolve.
    assert_eq!(ledger.watermark().committed_through, last_ticket);
    for ticket in 1..=last_ticket {
        assert!(wait_until(Duration::from_secs(5), || ledger
            .watermark()
            .committed_through
            >= ticket),);
    }

    drop(sender);
    storage.join().unwrap();

    // After the writer exits, the ledger is closed: late waiters fail fast
    // instead of hanging.
    assert!(ledger.watermark().closed);

    let conn = open_readonly(&db_path);
    assert_eq!(row_count(&conn), 40);
}

/// A rejected enqueue voids its ticket so it never stalls later commits:
/// exactly the hole-handling the contiguous watermark needs.
#[test]
fn voided_ticket_does_not_stall_subsequent_commits() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("voided.db");

    let (sender, receiver) = crossbeam_channel::bounded(1);
    let mut storage = Storage::open(receiver, config(db_path.clone(), 32, 60_000)).unwrap();
    let ledger = storage.commit_ledger();

    let mut chunk_a = into_log_chunk(logs_chunk(0, 5));
    chunk_a.commit_seq = ledger.issue(); // ticket 1, will be sent

    // Ticket 2 is issued for a chunk that is never sent (simulating a rejected
    // enqueue or a batch dropped before commit) and must be voided instead of
    // completed, or it would stall the watermark forever.
    let rejected_ticket = ledger.issue();
    ledger.void(rejected_ticket);

    let mut chunk_b = into_log_chunk(logs_chunk(10, 5));
    chunk_b.commit_seq = ledger.issue(); // ticket 3

    sender.send(IngestMessage::Logs(chunk_a)).unwrap();
    sender.send(IngestMessage::Logs(chunk_b)).unwrap();
    sender.send(IngestMessage::Flush).unwrap();

    assert!(wait_until(Duration::from_secs(5), || {
        ledger.watermark().committed_through >= 3
    }));

    drop(sender);
    storage.join().unwrap();

    let conn = open_readonly(&db_path);
    assert_eq!(row_count(&conn), 10);
}
