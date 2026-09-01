//! Fuzz target: the pure storage-side insert batcher plus core model
//! conversions.
//!
//! Drives [`InsertBatcher<LogRecord>`] with an arbitrary operation stream:
//! reconfiguration (always `max_batch_records >= 1`), pushes of arbitrary
//! record batches, explicit flushes and age-deadline flushes. The harness
//! checks the batcher's documented contract after every step:
//!
//! * every emitted batch holds at most `max_batch_records` records,
//! * no empty write batch is ever emitted,
//! * `pushed == emitted + buffered` at every point in time
//!   (reconfiguration flushes first, so nothing is lost),
//! * a final flush drains everything.

#![no_main]

use std::time::{Duration, Instant};

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use otel_sqlite_core::model::{Attribute, AttributeValue, LogRecord, Severity};
use otel_sqlite_core::storage::{InsertBatcher, InsertBatcherConfig, WriteBatch};

/// Arbitrary stand-in for [`LogRecord`]: keeps an `arbitrary` dependency out
/// of the core crate while still exercising every field via conversion.
#[derive(Arbitrary, Debug)]
struct FuzzLogRecord {
    time_unix_nano: i64,
    observed_time_unix_nano: i64,
    severity_number: u8,
    severity_text: String,
    trace_id: [u8; 16],
    span_id: [u8; 8],
    body: String,
    body_json: Option<String>,
    attributes: Vec<(String, FuzzAttributeValue)>,
    dropped_attributes_count: u32,
    flags: u32,
    event_name: String,
    scope_attributes: Vec<(String, FuzzAttributeValue)>,
    scope_schema_url: String,
}

#[derive(Arbitrary, Debug)]
enum FuzzAttributeValue {
    Null,
    String(String),
    Int(i64),
    Double(f64),
    Bool(bool),
    Bytes(Vec<u8>),
    Array(Vec<FuzzAttributeValue>),
    Kvlist(Vec<(String, FuzzAttributeValue)>),
}

impl From<FuzzAttributeValue> for AttributeValue {
    fn from(value: FuzzAttributeValue) -> Self {
        match value {
            FuzzAttributeValue::Null => AttributeValue::Null,
            FuzzAttributeValue::String(value) => AttributeValue::String(value),
            FuzzAttributeValue::Int(value) => AttributeValue::Int(value),
            FuzzAttributeValue::Double(value) => AttributeValue::Double(value),
            FuzzAttributeValue::Bool(value) => AttributeValue::Bool(value),
            FuzzAttributeValue::Bytes(value) => AttributeValue::Bytes(value),
            FuzzAttributeValue::Array(values) => {
                AttributeValue::Array(values.into_iter().map(AttributeValue::from).collect())
            }
            FuzzAttributeValue::Kvlist(values) => AttributeValue::Kvlist(
                values
                    .into_iter()
                    .map(|(key, value)| Attribute {
                        key,
                        value: AttributeValue::from(value),
                    })
                    .collect(),
            ),
        }
    }
}

fn fuzz_attributes(values: Vec<(String, FuzzAttributeValue)>) -> Vec<Attribute> {
    values
        .into_iter()
        .map(|(key, value)| Attribute {
            key,
            value: AttributeValue::from(value),
        })
        .collect()
}

impl From<FuzzLogRecord> for LogRecord {
    fn from(value: FuzzLogRecord) -> Self {
        Self {
            time_unix_nano: value.time_unix_nano,
            observed_time_unix_nano: value.observed_time_unix_nano,
            severity_number: Severity::from_u8(value.severity_number).unwrap_or_default(),
            severity_text: value.severity_text,
            trace_id: value.trace_id,
            span_id: value.span_id,
            body: value.body,
            body_json: value.body_json,
            attributes: fuzz_attributes(value.attributes),
            dropped_attributes_count: value.dropped_attributes_count,
            flags: value.flags,
            event_name: value.event_name,
            resource_id: String::new(),
            resource: None,
            scope_name: String::new(),
            scope_version: String::new(),
            scope_attributes: fuzz_attributes(value.scope_attributes),
            scope_schema_url: value.scope_schema_url,
        }
    }
}

#[derive(Arbitrary, Debug)]
enum Op {
    /// Replace the batcher; `max_batch_records` is clamped to >= 1 so only
    /// real batching logic is exercised, not its documented guard assert.
    /// The old partial buffer is flushed first, so accounting stays exact.
    Reconfigure {
        max_batch_records: u16,
        max_batch_age_millis: u32,
    },
    Push(Vec<FuzzLogRecord>),
    FlushIfExpired { advance_millis: u32 },
    Flush,
}

fuzz_target!(|ops: Vec<Op>| {
    let mut batcher = InsertBatcher::<LogRecord>::new(InsertBatcherConfig::default());
    let clock = Instant::now();
    let mut pushed = 0_usize;
    let mut emitted = 0_usize;

    for op in ops {
        match op {
            Op::Reconfigure {
                max_batch_records,
                max_batch_age_millis,
            } => {
                drain(&mut batcher, &mut emitted);
                batcher = InsertBatcher::new(InsertBatcherConfig {
                    max_batch_records: usize::from(max_batch_records.max(1)),
                    max_batch_age: Duration::from_millis(u64::from(max_batch_age_millis)),
                });
            }
            Op::Push(records) => {
                if records.is_empty() {
                    continue;
                }
                pushed += records.len();
                let limit = batcher.config().max_batch_records;
                let output = batcher.push(records.into_iter().map(LogRecord::from).collect());
                let mut ready = 0_usize;
                output.for_each(|batch| ready += check_batch(batch, limit));
                emitted += ready;
            }
            Op::FlushIfExpired { advance_millis } => {
                let deadline = clock + Duration::from_millis(u64::from(advance_millis));
                if let Some(batch) = batcher.flush_if_expired(deadline) {
                    emitted += check_batch(batch, batcher.config().max_batch_records);
                }
            }
            Op::Flush => drain(&mut batcher, &mut emitted),
        }

        assert_eq!(
            pushed,
            emitted + batcher.buffered(),
            "accounting violated: pushed={pushed} emitted={emitted} buffered={}",
            batcher.buffered()
        );
    }

    drain(&mut batcher, &mut emitted);
    assert_eq!(
        pushed, emitted,
        "final drain left records behind: pushed={pushed} emitted={emitted}"
    );
});

fn drain(batcher: &mut InsertBatcher<LogRecord>, emitted: &mut usize) {
    while let Some(batch) = batcher.flush() {
        *emitted += check_batch(batch, batcher.config().max_batch_records);
    }
}

fn check_batch(batch: WriteBatch<LogRecord>, max_batch_records: usize) -> usize {
    let records = batch.into_records();
    assert!(
        records.len() <= max_batch_records,
        "emitted {} records exceeds limit {max_batch_records}",
        records.len()
    );
    assert!(!records.is_empty(), "empty write batch emitted");
    records.len()
}
