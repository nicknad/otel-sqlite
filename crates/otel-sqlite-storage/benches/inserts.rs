//! Benchmarks for the SQLite storage boundary, driven over the public
//! `Storage` API:
//!
//! * `pipeline_overhead`: fixed cost of opening + shutting down a pipeline
//!   (schema migration, WAL configuration, shutdown checkpoint) — the additive
//!   constant behind every other number here,
//! * `write_batch`: one `WriteCommand::InsertLogs` == one SQLite transaction,
//!   measured across transaction sizes (batch-size sweep for the writer),
//! * `ingest_path`: full ingress-side path `LogChunk -> InsertBatcher ->
//!   command queue -> writer -> commit`, flushed immediately,
//! * `maintenance`: retention prune, WAL checkpoint truncate, ANALYZE and
//!   VACUUM against a populated database.
//!
//! Each iteration runs against a fresh temporary database (created in untimed
//! setup), so table growth cannot distort comparisons between sizes.
//!
//! Run with `cargo bench -p otel-sqlite-storage`.

use std::path::PathBuf;
use std::time::{Duration, Instant};

use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use crossbeam_channel::Sender;
use otel_sqlite_core::model::{AttributeValue, LogRecord, Severity};
use otel_sqlite_core::storage::{
    BatchOrigin, CheckpointMode, IngestMessage, InsertBatcherConfig, LogChunk, LogWriteBatch,
    MaintenanceOperation, RetentionPolicy, SyncMode, WriteBatch, WriteCommand,
};
use otel_sqlite_core::unix_nano_now;
use otel_sqlite_storage::{Storage, StorageConfig};
use tempfile::TempDir;

const COMMAND_QUEUE_CAPACITY: usize = 50_000;

/// A live pipeline: ingest side (through the real batcher) plus the direct
/// write boundary (`Storage::producer()`).
struct Pipeline {
    _dir: TempDir,
    ingest: Sender<IngestMessage>,
    producer: Sender<WriteCommand>,
    storage: Storage,
}

fn open_pipeline() -> Pipeline {
    let dir = TempDir::new().expect("temp dir");
    let (ingest, receiver) = crossbeam_channel::unbounded();
    let config = StorageConfig {
        sqlite_path: db_path(&dir),
        insert_batcher: InsertBatcherConfig::new(1_000, Duration::from_secs(600)),
        command_queue_capacity: COMMAND_QUEUE_CAPACITY,
        max_db_bytes: None,
        synchronous: SyncMode::Normal,
        startup_timeout: otel_sqlite_storage::DEFAULT_STARTUP_TIMEOUT,
        shutdown_timeout: otel_sqlite_storage::DEFAULT_SHUTDOWN_TIMEOUT,
    };
    let storage = Storage::open(receiver, config).expect("storage opens");
    let producer = storage.producer();
    let pipeline = Pipeline {
        _dir: dir,
        ingest,
        producer,
        storage,
    };
    // Warm-up outside any timed region: absorbs lazy writer startup (schema
    // migration, FTS init) and first-use statement preparation.
    let target = pipeline.storage.stats().records_written + 1;
    pipeline
        .producer
        .send(logs_write_command(1))
        .expect("writer alive");
    wait_written(&pipeline.storage, target);
    pipeline
}

fn db_path(dir: &TempDir) -> PathBuf {
    dir.path().join("bench.db")
}

/// Graceful shutdown: stop accepting work, let the batcher drain, join the
/// writer (which ends with a WAL TRUNCATE checkpoint).
fn close_pipeline(pipeline: Pipeline) {
    let Pipeline {
        _dir,
        ingest,
        producer,
        mut storage,
    } = pipeline;
    drop(ingest);
    drop(producer);
    storage.join().expect("writer drains and exits");
}

/// Busy-waits until the writer has committed `target` records total. Spinning
/// instead of polling keeps sub-millisecond transactions measurable.
fn wait_written(storage: &Storage, target: u64) {
    let deadline = Instant::now() + Duration::from_secs(60);
    while storage.stats().records_written < target {
        assert!(
            Instant::now() < deadline,
            "writer did not reach {target} records"
        );
        std::hint::spin_loop();
    }
}

fn wait_maintenance_run(storage: &Storage, previous_runs: u64) {
    let deadline = Instant::now() + Duration::from_secs(60);
    while storage.stats().maintenance_runs <= previous_runs {
        assert!(
            Instant::now() < deadline,
            "maintenance operation did not complete"
        );
        std::hint::spin_loop();
    }
}

fn log_record(seq: usize, time_unix_nano: i64) -> LogRecord {
    let id = (seq & 0xff) as u8;
    LogRecord {
        time_unix_nano,
        observed_time_unix_nano: time_unix_nano,
        severity_number: Severity::Info,
        severity_text: "INFO".to_owned(),
        trace_id: [id; 16],
        span_id: [id; 8],
        body: format!("seq={seq} sample log body padded for measurement realism .............."),
        attributes: vec![
            ("service.name", "checkout".to_owned()).into(),
            ("http.status_code", AttributeValue::Int(200)).into(),
            ("duration.ms", AttributeValue::Double(12.5)).into(),
        ],
        event_name: "benchmark".to_owned(),
        ..LogRecord::default()
    }
}

fn records(start_seq: usize, count: usize, time_unix_nano: i64) -> Vec<LogRecord> {
    (start_seq..start_seq + count)
        .map(|seq| log_record(seq, time_unix_nano))
        .collect()
}

/// Direct write-boundary command: one such command becomes one transaction.
fn logs_write_command(count: usize) -> WriteCommand {
    WriteCommand::InsertLogs(LogWriteBatch {
        origin: BatchOrigin::default(),
        records: WriteBatch::from_vec(records(0, count, unix_nano_now())),
        commit_seqs: Vec::new(),
    })
}

fn logs_chunk(count: usize) -> IngestMessage {
    IngestMessage::Logs(LogChunk {
        origin: BatchOrigin::default(),
        records: records(0, count, unix_nano_now()),
        commit_seq: 0,
    })
}

fn maintenance_config(
    c: &mut Criterion,
) -> criterion::BenchmarkGroup<'_, criterion::measurement::WallTime> {
    let mut group = c.benchmark_group("maintenance");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(15));
    group
}

// ---------------------------------------------------------------------------
// Fixed pipeline overhead
// ---------------------------------------------------------------------------

/// Opening (migration, WAL pragmas, thread spawn) plus graceful shutdown
/// (drain, final TRUNCATE checkpoint). Every other benchmark here pays this
/// cost implicitly through its per-iteration setup/teardown.
fn bench_pipeline_overhead(c: &mut Criterion) {
    let mut group = c.benchmark_group("pipeline_overhead");
    group.sample_size(30);

    group.bench_function("open_close_empty_db", |b| {
        b.iter(|| {
            // Full lifecycle inside the timed region: this is the additive
            // constant behind every other number here.
            close_pipeline(open_pipeline());
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Transaction-size sweep (write boundary)
// ---------------------------------------------------------------------------

/// One `WriteCommand::InsertLogs` per iteration == exactly one transaction.
/// Reveals where bigger transactions stop paying off.
fn bench_write_batch(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_batch");
    group.sample_size(30);

    for size in [1usize, 10, 100, 1_000, 5_000] {
        group.throughput(Throughput::Elements(size as u64));
        group.bench_function(format!("records_{size}"), |b| {
            b.iter_batched(
                open_pipeline,
                |pipeline| {
                    let target = pipeline.storage.stats().records_written + size as u64;
                    pipeline
                        .producer
                        .send(logs_write_command(size))
                        .expect("writer alive");
                    wait_written(&pipeline.storage, target);
                    // Teardown (drain + checkpoint) happens untimed.
                    pipeline
                },
                BatchSize::PerIteration,
            );
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Ingress-side path including the real insert batcher
// ---------------------------------------------------------------------------

/// `LogChunk -> InsertBatcher -> command queue -> writer -> commit`, forced
/// out by a Flush barrier so the age timer never contributes.
fn bench_ingest_path(c: &mut Criterion) {
    let mut group = c.benchmark_group("ingest_path");
    group.sample_size(30);

    for size in [1usize, 100, 1_000] {
        group.throughput(Throughput::Elements(size as u64));
        group.bench_function(format!("chunk_{size}"), |b| {
            b.iter_batched(
                open_pipeline,
                |pipeline| {
                    let target = pipeline.storage.stats().records_written + size as u64;
                    pipeline
                        .ingest
                        .send(logs_chunk(size))
                        .expect("batcher alive");
                    pipeline.ingest.send(IngestMessage::Flush).expect("alive");
                    wait_written(&pipeline.storage, target);
                    pipeline
                },
                BatchSize::PerIteration,
            );
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Maintenance operations against a populated database
// ---------------------------------------------------------------------------

fn populate(pipeline: &Pipeline, count: usize, time_unix_nano: i64) {
    const POPULATE_BATCH: usize = 1_000;
    let mut sent = 0usize;
    while sent < count {
        let batch = POPULATE_BATCH.min(count - sent);
        let target = pipeline.storage.stats().records_written + batch as u64;
        pipeline
            .producer
            .send(WriteCommand::InsertLogs(LogWriteBatch {
                origin: BatchOrigin::default(),
                records: WriteBatch::from_vec(records(sent, batch, time_unix_nano)),
                commit_seqs: Vec::new(),
            }))
            .expect("writer alive");
        wait_written(&pipeline.storage, target);
        sent += batch;
    }
}

/// Retention deletion of 10k expired rows in one prune.
fn bench_prune(c: &mut Criterion) {
    let mut group = maintenance_config(c);

    group.throughput(Throughput::Elements(10_000));
    group.bench_function("prune_expired_10k_rows", |b| {
        b.iter_batched(
            || {
                let pipeline = open_pipeline();
                let stale = unix_nano_now() - Duration::from_secs(6 * 3_600).as_nanos() as i64;
                populate(&pipeline, 10_000, stale);
                pipeline
            },
            |pipeline| {
                let runs = pipeline.storage.stats().maintenance_runs;
                pipeline
                    .producer
                    .send(WriteCommand::Maintenance(MaintenanceOperation::Prune(
                        // Logs-only: this benchmark measures the log-event
                        // deletion path specifically.
                        RetentionPolicy::logs_only(Duration::from_secs(3_600)),
                    )))
                    .expect("writer alive");
                wait_maintenance_run(&pipeline.storage, runs);
                pipeline
            },
            BatchSize::PerIteration,
        );
    });

    group.finish();
}

/// WAL checkpoint TRUNCATE after writing 10k rows (WAL is non-empty).
fn bench_checkpoint(c: &mut Criterion) {
    let mut group = maintenance_config(c);

    group.bench_function("checkpoint_truncate_after_10k", |b| {
        b.iter_batched(
            || {
                let pipeline = open_pipeline();
                populate(&pipeline, 10_000, unix_nano_now());
                pipeline
            },
            |pipeline| {
                let runs = pipeline.storage.stats().maintenance_runs;
                pipeline
                    .producer
                    .send(WriteCommand::Checkpoint(CheckpointMode::Truncate))
                    .expect("writer alive");
                wait_maintenance_run(&pipeline.storage, runs);
                pipeline
            },
            BatchSize::PerIteration,
        );
    });

    group.finish();
}

/// ANALYZE and VACUUM on a 10k-row database.
fn bench_analyze_vacuum(c: &mut Criterion) {
    let mut group = maintenance_config(c);

    group.bench_function("analyze_10k_rows", |b| {
        b.iter_batched(
            || {
                let pipeline = open_pipeline();
                populate(&pipeline, 10_000, unix_nano_now());
                pipeline
            },
            |pipeline| {
                let runs = pipeline.storage.stats().maintenance_runs;
                pipeline
                    .producer
                    .send(WriteCommand::Maintenance(MaintenanceOperation::Analyze))
                    .expect("writer alive");
                wait_maintenance_run(&pipeline.storage, runs);
                pipeline
            },
            BatchSize::PerIteration,
        );
    });

    group.bench_function("vacuum_10k_rows", |b| {
        b.iter_batched(
            || {
                let pipeline = open_pipeline();
                populate(&pipeline, 10_000, unix_nano_now());
                pipeline
            },
            |pipeline| {
                let runs = pipeline.storage.stats().maintenance_runs;
                pipeline
                    .producer
                    .send(WriteCommand::Maintenance(MaintenanceOperation::Vacuum))
                    .expect("writer alive");
                wait_maintenance_run(&pipeline.storage, runs);
                pipeline
            },
            BatchSize::PerIteration,
        );
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_pipeline_overhead,
    bench_write_batch,
    bench_ingest_path,
    bench_prune,
    bench_checkpoint,
    bench_analyze_vacuum
);
criterion_main!(benches);
