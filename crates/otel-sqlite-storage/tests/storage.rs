// Integration tests may assert with unwrap(); production code must not.
#![allow(clippy::unwrap_used)]

use std::time::{Duration, Instant};

use crossbeam_channel::unbounded;
use otel_sqlite_core::model::{
    Attribute, AttributeValue, Exemplar, HistogramDataPoint, LogBatch, LogRecord, MetricBatch,
    MetricData, MetricRecord, NumberDataPoint, NumberValue, Resource, Severity, Sum, Temporality,
};
use otel_sqlite_core::storage::{
    BatchOrigin, IngestMessage, LogChunk, MaintenanceOperation, MetricChunk, RetentionPolicy,
};
use otel_sqlite_storage::{Storage, StorageConfig, StorageError};
use rusqlite::Connection;

fn logs_message(batch: LogBatch) -> IngestMessage {
    IngestMessage::Logs(LogChunk {
        origin: BatchOrigin {
            resource: batch.resource,
            schema_url: batch.schema_url,
        },
        records: batch.records,
        commit_seq: 0,
    })
}

fn sample_batch() -> LogBatch {
    let mut batch = LogBatch::with_capacity(2);
    batch.resource = Some(Resource::new(vec![
        ("service.name", "checkout").into(),
        ("retries", 3_i64).into(),
    ]));
    "https://example.test/schemas".clone_into(&mut batch.schema_url);

    let mut record = LogRecord {
        time_unix_nano: 1_000,
        observed_time_unix_nano: 1_500,
        severity_number: Severity::Error,
        severity_text: "ERROR".to_owned(),
        body: "payment failed".to_owned(),
        trace_id: [7; 16],
        span_id: [9; 8],
        event_name: "order.failed".to_owned(),
        scope_name: "scope-a".to_owned(),
        scope_version: "1.0.0".to_owned(),
        ..LogRecord::default()
    };
    record.attributes.push(Attribute {
        key: "attempt".to_owned(),
        value: AttributeValue::Int(2),
    });
    batch.push(record);

    batch.push(LogRecord {
        time_unix_nano: 2_000,
        severity_number: Severity::Info,
        body: "ok".to_owned(),
        ..LogRecord::default()
    });

    batch
}

fn sample_metrics() -> MetricBatch {
    let mut batch = MetricBatch::with_capacity(3);
    batch.resource = Some(Resource::new(vec![("service.name", "checkout").into()]));

    let mut gauge_point = NumberDataPoint {
        start_time_unix_nano: 100,
        time_unix_nano: 200,
        value: Some(NumberValue::Int(42)),
        ..NumberDataPoint::default()
    };
    gauge_point.attributes.push(Attribute {
        key: "host".to_owned(),
        value: AttributeValue::from("node-1"),
    });
    push_metric(
        &mut batch,
        "cpu.usage",
        "",
        "1",
        MetricData::Gauge(otel_sqlite_core::model::Gauge {
            data_points: vec![gauge_point],
        }),
    );

    let histogram = otel_sqlite_core::model::Histogram {
        data_points: vec![HistogramDataPoint {
            start_time_unix_nano: 100,
            time_unix_nano: 300,
            count: 4,
            sum: Some(10.5),
            bucket_counts: vec![1, 2, 1],
            explicit_bounds: vec![5.0, 10.0],
            min: Some(1.0),
            max: Some(9.0),
            ..HistogramDataPoint::default()
        }],
        aggregation_temporality: Temporality::Cumulative,
    };
    push_metric(
        &mut batch,
        "rpc.duration",
        "duration",
        "ms",
        MetricData::Histogram(histogram),
    );

    push_metric(
        &mut batch,
        "bytes.sent",
        "",
        "By",
        MetricData::Sum(Sum {
            data_points: vec![NumberDataPoint {
                start_time_unix_nano: 100,
                time_unix_nano: 250,
                value: Some(NumberValue::Double(2.5)),
                ..NumberDataPoint::default()
            }],
            aggregation_temporality: Temporality::Delta,
            is_monotonic: true,
        }),
    );

    batch
}

fn push_metric(
    batch: &mut MetricBatch,
    name: &str,
    description: &str,
    unit: &str,
    data: MetricData,
) {
    batch.push(otel_sqlite_core::model::MetricRecord {
        name: name.to_owned(),
        description: description.to_owned(),
        unit: unit.to_owned(),
        data,
        scope_name: "otel-sqlite-test".to_owned(),
        ..otel_sqlite_core::model::MetricRecord::default()
    });
}

#[test]
fn writes_logs_deduplicates_resources() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let db_path = dir.path().join("test.db");

    let (sender, receiver) = unbounded();
    let config = StorageConfig {
        sqlite_path: db_path.clone(),
        insert_batcher: otel_sqlite_core::storage::InsertBatcherConfig::new(
            1_000,
            Duration::from_millis(20),
        ),
        ..StorageConfig::default()
    };
    let mut storage = Storage::open(receiver, config)?;

    sender.send(logs_message(sample_batch()))?;
    sender.send(logs_message(sample_batch()))?;
    sender.send(IngestMessage::Flush)?;
    drop(sender);
    storage.join()?;

    let stats = storage.stats();
    assert_eq!(stats.records_written, 4);
    // Both ingest chunks share one origin, so the insert batcher combines
    // them into a single storage-sized write batch / transaction.
    assert_eq!(stats.batches_received, 1);
    assert_eq!(stats.chunks_ingested, 2);
    assert_eq!(stats.errors, 0);

    let conn = Connection::open(&db_path)?;
    let log_count: i64 = conn.query_row("SELECT COUNT(*) FROM logs", [], |r| r.get(0))?;
    let resource_count: i64 =
        conn.query_row("SELECT COUNT(*) FROM log_resource", [], |r| r.get(0))?;
    let service: String = conn.query_row(
        "SELECT service_name FROM logs WHERE body = 'payment failed'",
        [],
        |r| r.get(0),
    )?;
    let trace_rows: i64 = conn.query_row(
        "SELECT COUNT(*) FROM logs WHERE trace_id IS NOT NULL",
        [],
        |r| r.get(0),
    )?;

    assert_eq!(log_count, 4);
    assert_eq!(
        resource_count, 1,
        "identical resources must fingerprint to one row"
    );
    assert_eq!(service, "checkout");
    assert_eq!(trace_rows, 2);

    let migration: String = conn.query_row(
        "SELECT version FROM schema_migrations ORDER BY version DESC LIMIT 1",
        [],
        |r| r.get(0),
    )?;
    assert_eq!(migration, "003");

    Ok(())
}

/// The search index must track the canonical table without manual rebuilds:
/// fresh inserts are searchable immediately (insert trigger), and a
/// retention prune removes their index entries in the same transaction
/// (delete trigger), so no stale hits survive.
#[test]
fn log_search_index_tracks_inserts_and_prunes_without_rebuild()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let db_path = dir.path().join("test.db");

    let count_matches = |conn: &Connection| -> i64 {
        conn.query_row(
            "SELECT COUNT(*) FROM logs_fts WHERE logs_fts MATCH 'payment'",
            [],
            |r| r.get(0),
        )
        .unwrap()
    };

    // 1. A freshly inserted log is searchable without any rebuild.
    {
        let (sender, receiver) = unbounded();
        let mut storage = Storage::open(
            receiver,
            StorageConfig {
                sqlite_path: db_path.clone(),
                insert_batcher: otel_sqlite_core::storage::InsertBatcherConfig::new(
                    1_000,
                    Duration::from_millis(20),
                ),
                ..StorageConfig::default()
            },
        )?;

        sender.send(logs_message(sample_batch()))?;
        sender.send(IngestMessage::Flush)?;
        drop(sender);
        storage.join()?;

        let conn = Connection::open(&db_path)?;
        assert_eq!(
            count_matches(&conn),
            1,
            "inserts must be indexed incrementally by trigger"
        );
    }

    // 2. Reopening the database (migration no-op) and pruning the records
    //    must remove their index rows too.
    {
        let (sender, receiver) = unbounded();
        let mut storage = Storage::open(
            receiver,
            StorageConfig {
                sqlite_path: db_path.clone(),
                ..StorageConfig::default()
            },
        )?;

        // The sample records carry epoch timestamps, so any retention
        // window covers them.
        sender.send(IngestMessage::Maintenance(MaintenanceOperation::Prune(
            otel_sqlite_core::storage::RetentionPolicy::new(Duration::from_secs(1)),
        )))?;
        sender.send(IngestMessage::Flush)?;
        drop(sender);
        storage.join()?;

        let conn = Connection::open(&db_path)?;
        let remaining: i64 = conn.query_row("SELECT COUNT(*) FROM log_event", [], |r| r.get(0))?;
        assert_eq!(remaining, 0, "prune must remove the sample events");
        assert_eq!(
            count_matches(&conn),
            0,
            "pruned events must not leave stale search hits"
        );

        // The index survives intact for future inserts: rebuilding is still
        // available as maintenance but must not be required.
        let fts_rows: i64 = conn.query_row("SELECT COUNT(*) FROM logs_fts", [], |r| r.get(0))?;
        assert_eq!(fts_rows, 0);
    }

    Ok(())
}

#[test]
fn fts_rebuild_enables_text_search() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let db_path = dir.path().join("test.db");

    let (sender, receiver) = unbounded();
    let config = StorageConfig {
        sqlite_path: db_path.clone(),
        insert_batcher: otel_sqlite_core::storage::InsertBatcherConfig::new(
            1_000,
            Duration::from_millis(20),
        ),
        ..StorageConfig::default()
    };
    let mut storage = Storage::open(receiver, config)?;

    sender.send(logs_message(sample_batch()))?;
    sender.send(IngestMessage::Flush)?;
    sender.send(IngestMessage::Maintenance(MaintenanceOperation::RebuildFts))?;
    drop(sender);
    storage.join()?;

    let conn = Connection::open(&db_path)?;
    let matches: i64 = conn.query_row(
        "SELECT COUNT(*) FROM logs_fts WHERE logs_fts MATCH 'payment'",
        [],
        |r| r.get(0),
    )?;
    assert_eq!(matches, 1);

    Ok(())
}

/// A rebuild interrupted at its worst point — index and triggers dropped,
/// nothing recreated yet — must be recoverable: the next `RebuildFts`
/// recreates both atomically, makes the existing rows searchable again and
/// keeps incremental indexing working for everything inserted afterwards.
#[test]
fn rebuild_fts_heals_a_crashed_rebuild() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let db_path = dir.path().join("test.db");
    let config_for = || StorageConfig {
        sqlite_path: db_path.clone(),
        insert_batcher: otel_sqlite_core::storage::InsertBatcherConfig::new(
            1_000,
            Duration::from_millis(20),
        ),
        ..StorageConfig::default()
    };

    // 1. Populate through the normal pipeline.
    {
        let (sender, receiver) = unbounded();
        let mut storage = Storage::open(receiver, config_for())?;
        sender.send(logs_message(sample_batch()))?;
        sender.send(IngestMessage::Flush)?;
        drop(sender);
        storage.join()?;
    }

    // 2. Simulate a crash in the middle of a rebuild: exactly the state
    //    right after rebuild's drop phase — no index, no triggers.
    {
        let conn = Connection::open(&db_path)?;
        conn.execute_batch(
            "DROP TRIGGER IF EXISTS logs_fts_ai;
             DROP TRIGGER IF EXISTS logs_fts_ad;
             DROP TABLE IF EXISTS logs_fts;",
        )?;
        let triggers: i64 = conn.query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'trigger' AND name LIKE 'logs_fts%'",
            [],
            |r| r.get(0),
        )?;
        assert_eq!(triggers, 0, "setup: crash simulation must remove triggers");
    }

    // 3. Heal via the maintenance command, then keep writing normally.
    {
        let (sender, receiver) = unbounded();
        let mut storage = Storage::open(receiver, config_for())?;
        sender.send(IngestMessage::Maintenance(MaintenanceOperation::RebuildFts))?;
        sender.send(logs_message(sample_batch()))?;
        sender.send(IngestMessage::Flush)?;
        drop(sender);
        storage.join()?;

        let conn = Connection::open(&db_path)?;
        let matches: i64 = conn.query_row(
            "SELECT COUNT(*) FROM logs_fts WHERE logs_fts MATCH 'payment'",
            [],
            |r| r.get(0),
        )?;
        assert_eq!(
            matches, 2,
            "rebuild must re-index existing rows and the trigger must index new ones"
        );
        let triggers: i64 = conn.query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'trigger' AND name LIKE 'logs_fts%'",
            [],
            |r| r.get(0),
        )?;
        assert_eq!(triggers, 2, "rebuild must restore both sync triggers");
    }

    Ok(())
}

#[test]
fn writes_normalized_metric_hierarchy() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let db_path = dir.path().join("test.db");

    let (sender, receiver) = unbounded();
    let config = StorageConfig {
        sqlite_path: db_path.clone(),
        insert_batcher: otel_sqlite_core::storage::InsertBatcherConfig::new(
            1_000,
            Duration::from_millis(20),
        ),
        ..StorageConfig::default()
    };
    let mut storage = Storage::open(receiver, config)?;

    sender.send(IngestMessage::Metrics(
        otel_sqlite_core::storage::MetricChunk {
            origin: BatchOrigin::default(),
            records: sample_metrics().records,
            commit_seq: 0,
        },
    ))?;
    sender.send(IngestMessage::Flush)?;
    drop(sender);
    storage.join()?;

    let conn = Connection::open(&db_path)?;

    let resources: i64 = conn.query_row("SELECT COUNT(*) FROM log_resource", [], |r| r.get(0))?;
    let scopes: i64 = conn.query_row("SELECT COUNT(*) FROM scope", [], |r| r.get(0))?;
    let metrics: i64 = conn.query_row("SELECT COUNT(*) FROM metric", [], |r| r.get(0))?;
    let series: i64 = conn.query_row("SELECT COUNT(*) FROM metric_series", [], |r| r.get(0))?;
    let points: i64 = conn.query_row("SELECT COUNT(*) FROM metric_data_point", [], |r| r.get(0))?;

    assert_eq!(resources, 1);
    assert_eq!(scopes, 1);
    assert_eq!(metrics, 3);
    assert_eq!(series, 3, "gauge series + histogram series + sum series");
    assert_eq!(points, 3);

    let monotonic_sum: i64 = conn.query_row(
        "SELECT m.is_monotonic FROM metrics v JOIN metric m ON m.id = v.metric_id WHERE v.metric_name = 'bytes.sent'",
        [],
        |r| r.get(0),
    )?;
    assert_eq!(monotonic_sum, 1);

    let double_only: i64 = conn.query_row(
        "SELECT COUNT(*) FROM metric_data_point WHERE double_value IS NOT NULL AND int_value IS NOT NULL",
        [],
        |r| r.get(0),
    )?;
    assert_eq!(double_only, 0);

    let int_gauge: Option<i64> = conn.query_row(
        "SELECT int_value FROM metrics WHERE metric_name = 'cpu.usage'",
        [],
        |r| r.get(0),
    )?;
    assert_eq!(int_gauge, Some(42));

    let buckets: i64 = conn.query_row(
        "SELECT COUNT(*)
         FROM metric_buckets b
         JOIN metric_series ms ON ms.id = b.series_id
         JOIN metric m ON m.id = ms.metric_id
         WHERE m.name = 'rpc.duration'",
        [],
        |r| r.get(0),
    )?;
    assert_eq!(buckets, 3, "one row per bound match + the +Inf bucket");

    Ok(())
}

#[test]
fn health_probe_reports_liveness_and_progress() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let (sender, receiver) = unbounded();
    let mut storage = Storage::open(
        receiver,
        StorageConfig {
            sqlite_path: dir.path().join("health.db"),
            ..StorageConfig::default()
        },
    )?;

    // Both threads report running from the start; the probe never blocks.
    let sample = storage.health().sample();
    assert!(sample.writer_running, "writer must report running");
    assert!(sample.batcher_running, "batcher must report running");
    assert_eq!(sample.buffered_records, 0);

    sender.send(logs_message(sample_batch()))?;
    sender.send(IngestMessage::Flush)?;
    let started = std::time::Instant::now();
    while storage.stats().records_written == 0 {
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "writer must persist the flushed batch"
        );
        std::thread::sleep(Duration::from_millis(2));
    }

    // A successful commit is meaningful progress: the age must be fresh.
    let sample = storage.health().sample();
    assert!(
        sample.writer_idle_ms < 5_000,
        "commit must refresh writer progress, got {}ms",
        sample.writer_idle_ms
    );
    assert!(sample.batcher_idle_ms < 5_000);
    assert_eq!(storage.stats().records_written, 2);

    drop(sender);
    storage.join()?;

    // The drop guard clears liveness on every exit path: after a clean
    // shutdown both threads must report not-running.
    let sample = storage.health().sample();
    assert!(!sample.writer_running);
    assert!(!sample.batcher_running);
    Ok(())
}

/// Storage readiness is part of `Storage::open`: an unusable database path
/// must surface as an immediate startup error, never as a half-started
/// pipeline that only fails on first traffic.
#[test]
fn open_fails_fast_when_the_database_is_unusable() {
    let dir = tempfile::tempdir().unwrap();
    // A directory where the database file should be: SQLite cannot open it.
    let blocked = dir.path().join("blocked.db");
    std::fs::create_dir(&blocked).unwrap();

    let (_sender, receiver) = unbounded();
    let started = Instant::now();
    let result = Storage::open(
        receiver,
        StorageConfig {
            sqlite_path: blocked,
            ..StorageConfig::default()
        },
    );

    let error = result.expect_err("open must fail on an unusable database path");
    assert!(
        matches!(
            error,
            StorageError::StartupFailed { .. } | StorageError::Sqlite(_) | StorageError::Io(_)
        ),
        "unexpected error variant: {error:?}"
    );
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "startup failure must surface immediately, took {:?}",
        started.elapsed()
    );
}

/// A graceful drain completes well inside the shutdown budget.
#[test]
fn join_timeout_completes_a_graceful_drain() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let (sender, receiver) = unbounded();
    let mut storage = Storage::open(
        receiver,
        StorageConfig {
            sqlite_path: dir.path().join("test.db"),
            insert_batcher: otel_sqlite_core::storage::InsertBatcherConfig::new(
                1_000,
                Duration::from_millis(20),
            ),
            ..StorageConfig::default()
        },
    )?;

    sender.send(logs_message(sample_batch()))?;
    sender.send(IngestMessage::Flush)?;
    drop(sender);

    storage.join_timeout(Duration::from_secs(10))?;
    assert_eq!(storage.stats().records_written, 2);
    Ok(())
}

/// Shutdown must be bounded: while a producer keeps the pipeline fed, the
/// batcher never finishes and `join_timeout` reports the budget overrun
/// instead of hanging. Releasing the producer afterwards lets the same
/// `Storage` drain and join successfully.
#[test]
fn join_timeout_reports_the_budget_overrun_while_input_stays_open() {
    let dir = tempfile::tempdir().unwrap();
    let (sender, receiver) = unbounded();
    let mut storage = Storage::open(
        receiver,
        StorageConfig {
            sqlite_path: dir.path().join("test.db"),
            insert_batcher: otel_sqlite_core::storage::InsertBatcherConfig::new(
                1_000,
                Duration::from_millis(20),
            ),
            ..StorageConfig::default()
        },
    )
    .unwrap();

    sender.send(logs_message(sample_batch())).unwrap();
    let started = Instant::now();
    let error = storage
        .join_timeout(Duration::from_millis(200))
        .expect_err("an open producer must keep shutdown from completing");
    let elapsed = started.elapsed();

    assert!(
        matches!(error, StorageError::ShutdownTimeout { .. }),
        "unexpected error variant: {error:?}"
    );
    // The timeout bounds the wait; it neither fires early nor waits forever.
    assert!(
        elapsed >= Duration::from_millis(150) && elapsed < Duration::from_secs(5),
        "timeout must fire near the budget, took {elapsed:?}"
    );

    drop(sender);
    storage
        .join_timeout(Duration::from_secs(10))
        .expect("releasing the producer lets shutdown complete");
}

fn metrics_message(batch: MetricBatch) -> IngestMessage {
    IngestMessage::Metrics(MetricChunk {
        origin: BatchOrigin {
            resource: batch.resource,
            schema_url: batch.schema_url,
        },
        records: batch.records,
        commit_seq: 0,
    })
}

/// One gauge point on its own resource/scope/metric/series chain.
fn single_point_batch(service_name: &str, metric_name: &str, time_unix_nano: i64) -> MetricBatch {
    let mut batch = MetricBatch::with_capacity(1);
    batch.resource = Some(Resource::new(vec![("service.name", service_name).into()]));
    push_metric(
        &mut batch,
        metric_name,
        "",
        "1",
        MetricData::Gauge(otel_sqlite_core::model::Gauge {
            data_points: vec![NumberDataPoint {
                time_unix_nano,
                value: Some(NumberValue::Int(1)),
                ..NumberDataPoint::default()
            }],
        }),
    );
    batch
}

/// A uniform retention prune must age out log events
/// *and* metric data points together, garbage-collect the dimensions their
/// deletion orphans (series → metric → scope → resource), and leave the
/// search index with no ghosts of the pruned events — while every fresh row
/// and the dimension chain that still references it survives untouched.
#[test]
fn retention_prunes_metrics_collects_orphans_and_leaves_no_fts_ghosts()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let db_path = dir.path().join("test.db");

    let now = otel_sqlite_core::unix_nano_now();
    // Epoch-era timestamps: older than any retention window.
    let ancient = 1_000;

    let mut old_logs = LogBatch::with_capacity(1);
    old_logs.resource = Some(Resource::new(vec![("service.name", "svc-old").into()]));
    old_logs.push(LogRecord {
        time_unix_nano: ancient,
        observed_time_unix_nano: ancient,
        severity_number: Severity::Info,
        body: "ancient expired message".to_owned(),
        ..LogRecord::default()
    });

    let mut fresh_logs = LogBatch::with_capacity(1);
    fresh_logs.resource = Some(Resource::new(vec![("service.name", "svc-live").into()]));
    fresh_logs.push(LogRecord {
        time_unix_nano: now,
        observed_time_unix_nano: now,
        severity_number: Severity::Info,
        body: "fresh keeper message".to_owned(),
        ..LogRecord::default()
    });

    let (sender, receiver) = unbounded();
    let config = StorageConfig {
        sqlite_path: db_path.clone(),
        insert_batcher: otel_sqlite_core::storage::InsertBatcherConfig::new(
            1_000,
            Duration::from_millis(20),
        ),
        ..StorageConfig::default()
    };
    let mut storage = Storage::open(receiver, config)?;

    sender.send(logs_message(old_logs))?;
    sender.send(logs_message(fresh_logs))?;
    sender.send(metrics_message(single_point_batch(
        "svc-old",
        "stale.metric",
        ancient,
    )))?;
    sender.send(metrics_message(single_point_batch(
        "svc-live",
        "live.metric",
        now,
    )))?;
    sender.send(IngestMessage::Flush)?;
    assert!(
        wait_for(
            || storage.stats().records_written >= 4,
            Duration::from_secs(5)
        ),
        "all four records must reach SQLite before the prune runs"
    );

    // Uniform window: prunes both signals; everything from ~1970 expires.
    sender.send(IngestMessage::Maintenance(MaintenanceOperation::Prune(
        RetentionPolicy::new(Duration::from_secs(1)),
    )))?;
    sender.send(IngestMessage::Flush)?;
    drop(sender);
    storage.join()?;

    assert_eq!(storage.stats().errors, 0);

    let conn = Connection::open(&db_path)?;
    let scalar = |sql: &str| -> i64 {
        conn.query_row(sql, [], |row| row.get(0))
            .unwrap_or_else(|error| panic!("{sql} failed: {error}"))
    };

    // Canonical data: only fresh rows survive.
    assert_eq!(
        scalar("SELECT COUNT(*) FROM logs WHERE body = 'ancient expired message'"),
        0,
        "expired log events must be deleted"
    );
    assert_eq!(
        scalar("SELECT COUNT(*) FROM logs WHERE body = 'fresh keeper message'"),
        1,
        "fresh log events must survive"
    );
    assert_eq!(
        scalar("SELECT COUNT(*) FROM metric_data_point"),
        1,
        "only the fresh metric data point must survive"
    );

    // Dimension GC: the stale chain is fully orphaned by the point deletion.
    assert_eq!(
        scalar("SELECT COUNT(*) FROM metric_series"),
        1,
        "the orphaned series must be collected"
    );
    assert_eq!(
        scalar("SELECT COUNT(*) FROM metric"),
        1,
        "the orphaned metric definition must be collected"
    );
    assert_eq!(
        scalar("SELECT COUNT(*) FROM scope"),
        1,
        "the orphaned scope must be collected"
    );
    assert_eq!(
        scalar("SELECT COUNT(*) FROM log_resource"),
        1,
        "the resource of the pruned chain (logs + metrics gone) must be collected"
    );
    let surviving_service: String =
        conn.query_row("SELECT service_name FROM log_resource", [], |r| r.get(0))?;
    assert_eq!(surviving_service, "svc-live");

    // Search returns no ghosts of the pruned event, and the fresh one stays
    // searchable without any rebuild.
    let fts_count = |query: &str| -> i64 {
        conn.query_row(
            "SELECT COUNT(*) FROM logs_fts WHERE logs_fts MATCH ?1",
            [query],
            |r| r.get(0),
        )
        .unwrap_or_else(|error| panic!("FTS match `{query}` failed: {error}"))
    };
    assert_eq!(fts_count("expired"), 0, "pruned events must leave no hits");
    assert_eq!(
        fts_count("keeper"),
        1,
        "fresh events must remain searchable"
    );

    Ok(())
}

/// P0-3 conformance: supported input fields must reach the database
/// byte-identically to their canonical JSON rendering — structured bodies,
/// nested attribute values, scope attributes/schema URLs, exemplars and metric
/// metadata are never flattened, discarded or silently converted.
#[test]
fn mapping_fidelity_persists_canonical_structured_values() -> Result<(), Box<dyn std::error::Error>>
{
    let dir = tempfile::tempdir()?;
    let db_path = dir.path().join("fidelity.db");

    let (sender, receiver) = unbounded();
    let config = StorageConfig {
        sqlite_path: db_path.clone(),
        insert_batcher: otel_sqlite_core::storage::InsertBatcherConfig::new(
            1_000,
            Duration::from_millis(20),
        ),
        ..StorageConfig::default()
    };
    let mut storage = Storage::open(receiver, config)?;

    // --- logs: structured body, nested attribute, scope attrs/schema URL ---
    let mut log_batch = LogBatch::with_capacity(1);
    log_batch.resource = Some(Resource::new(vec![("service.name", "checkout").into()]));
    log_batch.schema_url = "https://example.test/schemas/logs".to_owned();
    log_batch.push(LogRecord {
        time_unix_nano: 1_000,
        severity_number: Severity::Info,
        body: String::new(),
        body_json: Some(r#"{"b":1,"a":[true,{"n":1.5}]}"#.to_owned()),
        attributes: vec![Attribute {
            key: "nested".to_owned(),
            value: AttributeValue::Array(vec![
                AttributeValue::Bool(true),
                AttributeValue::Kvlist(vec![Attribute {
                    key: "k".to_owned(),
                    value: AttributeValue::Int(7),
                }]),
            ]),
        }],
        scope_name: "scope-a".to_owned(),
        scope_version: "1.0.0".to_owned(),
        scope_attributes: vec![
            Attribute {
                key: "b".to_owned(),
                value: AttributeValue::Int(1),
            },
            Attribute {
                key: "a".to_owned(),
                value: AttributeValue::String("v".to_owned()),
            },
        ],
        scope_schema_url: "https://example.test/schemas/logs/scope".to_owned(),
        ..LogRecord::default()
    });
    sender.send(logs_message(log_batch))?;

    // --- metrics: exemplars, metadata, scope attrs/schema URL ---
    let mut metric_batch = MetricBatch::with_capacity(1);
    metric_batch.resource = Some(Resource::new(vec![("service.name", "checkout").into()]));
    metric_batch.schema_url = "https://example.test/schemas/metrics".to_owned();
    metric_batch.push(MetricRecord {
        name: "requests.total".to_owned(),
        description: "count".to_owned(),
        unit: "1".to_owned(),
        metadata: vec![Attribute {
            key: "unit.origin".to_owned(),
            value: AttributeValue::String("prometheus".to_owned()),
        }],
        data: MetricData::Gauge(otel_sqlite_core::model::Gauge {
            data_points: vec![NumberDataPoint {
                time_unix_nano: 200,
                value: Some(NumberValue::Int(42)),
                exemplars: vec![Exemplar {
                    filtered_attributes: vec![Attribute {
                        key: "z".to_owned(),
                        value: AttributeValue::Bool(true),
                    }],
                    time_unix_nano: 150,
                    value: Some(NumberValue::Double(0.5)),
                    trace_id: Some([8; 16]),
                    span_id: Some([9; 8]),
                }],
                ..NumberDataPoint::default()
            }],
        }),
        scope_name: "scope-m".to_owned(),
        scope_version: "0.9.9".to_owned(),
        scope_attributes: vec![Attribute {
            key: "scope.tag".to_owned(),
            value: AttributeValue::String("prod".to_owned()),
        }],
        scope_schema_url: "https://example.test/schemas/metrics/scope".to_owned(),
        ..MetricRecord::default()
    });
    sender.send(metrics_message(metric_batch))?;

    sender.send(IngestMessage::Flush)?;
    drop(sender);
    storage.join()?;

    let conn = Connection::open(&db_path)?;

    // Structured body is persisted as canonical JSON in the body column.
    let body: String = conn.query_row(
        "SELECT body FROM logs WHERE scope_name = 'scope-a'",
        [],
        |r| r.get(0),
    )?;
    assert_eq!(body, r#"{"b":1,"a":[true,{"n":1.5}]}"#);

    // Nested attribute values are persisted with canonical nested JSON.
    let attributes_json: String = conn.query_row(
        "SELECT attributes_json FROM logs WHERE scope_name = 'scope-a'",
        [],
        |r| r.get(0),
    )?;
    assert_eq!(attributes_json, r#"{"nested":[true,{"k":7}]}"#);

    // Scope attributes (sorted keys) and scope schema URL reach log_event.
    let scope_attributes_json: String = conn.query_row(
        "SELECT scope_attributes_json FROM logs WHERE scope_name = 'scope-a'",
        [],
        |r| r.get(0),
    )?;
    assert_eq!(scope_attributes_json, r#"{"a":"v","b":1}"#);
    let scope_schema_url: String = conn.query_row(
        "SELECT scope_schema_url FROM logs WHERE scope_name = 'scope-a'",
        [],
        |r| r.get(0),
    )?;
    assert_eq!(scope_schema_url, "https://example.test/schemas/logs/scope");

    // Exemplars reach exemplars_json with sorted field order and hex ids.
    let exemplars_json: String =
        conn.query_row("SELECT exemplars_json FROM metric_data_point", [], |r| {
            r.get(0)
        })?;
    assert_eq!(
        exemplars_json,
        concat!(
            r#"[{"filtered_attributes":{"z":true},"#,
            r#""span_id":"0909090909090909","#,
            r#""time_unix_nano":150,"#,
            r#""trace_id":"08080808080808080808080808080808","#,
            r#""value":0.5}]"#
        )
    );

    // Metric metadata and scope metadata reach the shared dimension tables.
    let metadata_json: String =
        conn.query_row("SELECT metadata_json FROM metric", [], |r| r.get(0))?;
    assert_eq!(metadata_json, r#"{"unit.origin":"prometheus"}"#);

    let scope_attributes_json: String =
        conn.query_row("SELECT attributes_json FROM scope", [], |r| r.get(0))?;
    assert_eq!(scope_attributes_json, r#"{"scope.tag":"prod"}"#);

    let scope_schema_url: String =
        conn.query_row("SELECT schema_url FROM scope", [], |r| r.get(0))?;
    assert_eq!(
        scope_schema_url,
        "https://example.test/schemas/metrics/scope"
    );

    Ok(())
}

/// Poll until `predicate` holds or `budget` elapses. The pipeline commits
/// asynchronously, so tests observe effects, not internal ordering.
fn wait_for(predicate: impl Fn() -> bool, budget: Duration) -> bool {
    let deadline = Instant::now() + budget;
    while Instant::now() < deadline {
        if predicate() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    predicate()
}
