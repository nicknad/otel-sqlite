//! Microbenchmark for the ingress mapping boundary:
//!
//! ```text
//! OTLP ExportLogsServiceRequest payload (prost)
//!     │
//!     ▼
//! LogBatch::from(ResourceLogs)   <- measured
//!     │
//!     ▼
//! internal LogBatch / LogRecord model
//! ```
//!
//! Measures conversion cost across input sizes (linear vs. super-linear
//! growth) and attribute-count scaling at a fixed record count.
//!
//! Run with `cargo bench -p otel-sqlite-ingress --bench conversion`.

use std::hint::black_box;

use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use otel_sqlite_ingress::mapping::logs::try_convert_batch;
use otel_sqlite_ingress::mapping::pb::common::v1::{
    AnyValue, InstrumentationScope, KeyValue, any_value::Value,
};
use otel_sqlite_ingress::mapping::pb::logs::v1::{
    LogRecord as ProtoLogRecord, ResourceLogs, ScopeLogs, SeverityNumber,
};
use otel_sqlite_ingress::mapping::pb::resource::v1::Resource as ProtoResource;

const BODY_TEMPLATE: &str =
    "sample log body padded for measurement realism ................................";

fn any(kind: Value) -> AnyValue {
    AnyValue { value: Some(kind) }
}

fn key_value(key: &str, kind: Value) -> KeyValue {
    KeyValue {
        key: key.to_owned(),
        value: Some(any(kind)),
        ..Default::default()
    }
}

/// One OTLP log record: body + trace context + mixed-type attributes.
fn proto_record(seq: u64, attributes_per_record: usize) -> ProtoLogRecord {
    let mut attributes = Vec::with_capacity(attributes_per_record.max(1));
    attributes.push(key_value("bench.seq", Value::IntValue(seq as i64)));
    for i in 0..attributes_per_record.saturating_sub(1) {
        match i % 3 {
            0 => attributes.push(key_value(
                &format!("bench.attr.str_{i}"),
                Value::StringValue(format!("value-{seq}-{i}")),
            )),
            1 => attributes.push(key_value(
                &format!("bench.attr.int_{i}"),
                Value::IntValue((seq % 1000) as i64),
            )),
            _ => attributes.push(key_value(
                &format!("bench.attr.bool_{i}"),
                Value::BoolValue(i % 2 == 0),
            )),
        }
    }

    ProtoLogRecord {
        time_unix_nano: 1_700_000_000_000_000_000 + seq,
        observed_time_unix_nano: 1_700_000_000_000_000_001 + seq,
        severity_number: SeverityNumber::Info as i32,
        severity_text: "INFO".to_owned(),
        // Never all-zeroes: OTLP mapping rejects all-zero trace and span ids,
        // and seq=0 (used by the smallest sweep size) must stay a valid request.
        trace_id: {
            let mut id = vec![(seq & 0xff) as u8; 16];
            id[0] |= 1;
            id
        },
        span_id: {
            let mut id = vec![(seq & 0xff) as u8; 8];
            id[0] |= 1;
            id
        },
        body: Some(any(Value::StringValue(format!(
            "seq={seq} {BODY_TEMPLATE}"
        )))),
        attributes,
        flags: 1,
        event_name: "benchmark".to_owned(),
        ..Default::default()
    }
}

/// A single `ResourceLogs` group holding `count` records in one scope, with a
/// resource carrying `service.name`.
fn resource_logs(count: usize, attributes_per_record: usize) -> ResourceLogs {
    ResourceLogs {
        resource: Some(ProtoResource {
            attributes: vec![key_value(
                "service.name",
                Value::StringValue("otel-sqlite-bench".to_owned()),
            )],
            ..Default::default()
        }),
        scope_logs: vec![ScopeLogs {
            scope: Some(InstrumentationScope {
                name: "otel-sqlite-bench".to_owned(),
                version: "0.1.0".to_owned(),
                ..Default::default()
            }),
            log_records: (0..count as u64)
                .map(|seq| proto_record(seq, attributes_per_record))
                .collect(),
            ..Default::default()
        }],
        schema_url: "https://example.test/schemas/logs".to_owned(),
    }
}

fn bench_conversion(c: &mut Criterion) {
    let mut group = c.benchmark_group("otlp_conversion");

    // Input-size sweep: catches accidental quadratic behaviour in mapping.
    for size in [1usize, 10, 100, 1_000, 10_000] {
        group.throughput(Throughput::Elements(size as u64));
        let payload = resource_logs(size, 4);
        group.bench_function(format!("records_{size}_attrs_4"), |b| {
            b.iter_batched(
                || payload.clone(),
                |payload| {
                    let batch = try_convert_batch(payload).expect("bench payload is valid");
                    black_box(batch.len());
                },
                BatchSize::LargeInput,
            );
        });
    }

    // Attribute scaling at a fixed record count: attribute conversion is the
    // allocation-heavy part of mapping.
    for attrs in [0usize, 4, 16] {
        let payload = resource_logs(1_000, attrs);
        group.bench_function(format!("records_1000_attrs_{attrs}"), |b| {
            b.iter_batched(
                || payload.clone(),
                |payload| {
                    let batch = try_convert_batch(payload).expect("bench payload is valid");
                    black_box(batch.len());
                },
                BatchSize::LargeInput,
            );
        });
    }

    group.finish();
}

criterion_group!(benches, bench_conversion);
criterion_main!(benches);
