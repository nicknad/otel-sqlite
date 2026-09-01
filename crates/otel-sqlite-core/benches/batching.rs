//! Microbenchmarks for the in-memory batching and command boundaries:
//!
//! * `batch_construction`: [`InsertBatcher::push`] converting incoming record
//!   vectors into capacity-sized [`WriteBatch`]es (split path),
//! * `batch_merging`: many small pushes combining into full batches plus the
//!   final partial flush (low-volume path),
//! * `command_construction`: turning a record vector into a validated
//!   [`WriteCommand`] — the unit that becomes exactly one SQLite transaction.
//!
//! Run with `cargo bench -p otel-sqlite-core`.

use std::hint::black_box;
use std::time::Duration;

use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use otel_sqlite_core::model::{AttributeValue, LogRecord, Resource, Severity};
use otel_sqlite_core::storage::{
    BatchOrigin, InsertBatcher, InsertBatcherConfig, LogWriteBatch, WriteBatch, WriteCommand,
};

/// Realistic-ish log record: ~120 byte body, trace context, 4 attributes.
fn log_record(seq: usize) -> LogRecord {
    let seq = seq as i64;
    let stamp = 1_700_000_000_000_000_000 + seq;
    let id = (seq & 0xff) as u8;
    LogRecord {
        time_unix_nano: stamp,
        observed_time_unix_nano: stamp + 1,
        severity_number: Severity::Info,
        severity_text: "INFO".to_owned(),
        trace_id: [id; 16],
        span_id: [id; 8],
        body: "seq={seq} sample log body padded for measurement realism .............."
            .replace("{seq}", &seq.to_string()),
        attributes: vec![
            ("service.name", "checkout".to_owned()).into(),
            ("http.method", "POST".to_owned()).into(),
            ("http.status_code", AttributeValue::Int(200)).into(),
            ("duration.ms", AttributeValue::Double(12.5)).into(),
        ],
        event_name: "benchmark".to_owned(),
        ..LogRecord::default()
    }
}

fn records(count: usize) -> Vec<LogRecord> {
    (0..count).map(log_record).collect()
}

fn batcher_config(capacity: usize) -> InsertBatcherConfig {
    InsertBatcherConfig::new(capacity, Duration::from_secs(600))
}

fn origin() -> BatchOrigin {
    BatchOrigin {
        resource: Some(Resource::new(vec![("service.name", "checkout").into()])),
        schema_url: "https://example.test/schemas/logs".to_owned(),
    }
}

/// Cost of converting incoming record chunks into full storage batches at
/// various input sizes. Exposes linear vs. quadratic growth in input size.
fn bench_batch_construction(c: &mut Criterion) {
    const CAPACITY: usize = 1_000;
    let mut group = c.benchmark_group("batch_construction");

    for size in [1, 10, 100, 1_000, 10_000] {
        group.throughput(Throughput::Elements(size as u64));
        let chunk = records(size);
        group.bench_function(format!("push_{size}"), |b| {
            b.iter_batched(
                || chunk.clone(),
                |chunk| {
                    // Fresh batcher per iteration: measures pure conversion
                    // (splitting an oversized input into full batches).
                    let mut batcher = InsertBatcher::new(batcher_config(CAPACITY));
                    let output = batcher.push(chunk);
                    black_box(output.len());
                },
                BatchSize::LargeInput,
            );
        });
    }

    group.finish();
}

/// Cost of the combine path: repeated small pushes accumulating into
/// capacity-sized batches, then the age-deadline style flush of what is left.
fn bench_batch_merging(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch_merging");

    for (pushes, push_size) in [(20usize, 50usize), (100, 10)] {
        let total = pushes * push_size;
        group.throughput(Throughput::Elements(total as u64));
        let chunks: Vec<Vec<LogRecord>> = (0..pushes)
            .map(|round| {
                (0..push_size)
                    .map(|i| log_record(round * push_size + i))
                    .collect()
            })
            .collect();
        group.bench_function(format!("combine_{pushes}x{push_size}"), |b| {
            b.iter_batched(
                || chunks.clone(),
                |chunks| {
                    let mut batcher = InsertBatcher::new(batcher_config(1_000));
                    for chunk in chunks {
                        black_box(batcher.push(chunk).len());
                    }
                    black_box(batcher.flush().map(|partial| partial.len()));
                },
                BatchSize::LargeInput,
            );
        });
    }

    group.finish();
}

/// Cost of building the writer-facing command from a record vector: ownership
/// transfer via [`WriteBatch::from_vec`], then validation/metadata access.
fn bench_command_construction(c: &mut Criterion) {
    let mut group = c.benchmark_group("command_construction");

    for size in [1, 100, 1_000] {
        group.throughput(Throughput::Elements(size as u64));
        let records = records(size);
        group.bench_function(format!("write_command_{size}"), |b| {
            b.iter_batched(
                || records.clone(),
                |records| {
                    let command = WriteCommand::InsertLogs(LogWriteBatch {
                        origin: origin(),
                        records: WriteBatch::from_vec(records),
                        commit_seqs: Vec::new(),
                    });
                    black_box(command.record_count());
                    black_box(command.validate().is_ok());
                    black_box(command.kind());
                },
                BatchSize::LargeInput,
            );
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_batch_construction,
    bench_batch_merging,
    bench_command_construction
);
criterion_main!(benches);
