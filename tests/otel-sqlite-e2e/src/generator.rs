//! Deterministic OTLP log workload generation.
//!
//! Every record carries three attributes used for post-run correctness
//! validation (see `validation.rs`):
//!
//! * `bench.run_id`   - unique per scenario step
//! * `bench.seq`      - globally unique, contiguous per run
//! * `bench.record_id`- identical to `bench.seq`
//!
//! Generation is fully deterministic for a given (`seed`, sequence number):
//! the same logical workload is reproduced across runs.

use otel_sqlite_ingress::mapping::pb::collector::logs::v1::ExportLogsServiceRequest;
use otel_sqlite_ingress::mapping::pb::common::v1::{
    AnyValue, KeyValue, any_value::Value as AnyValueKind,
};
use otel_sqlite_ingress::mapping::pb::logs::v1::{
    LogRecord as ProtoLogRecord, ResourceLogs, ScopeLogs, SeverityNumber,
};
use otel_sqlite_ingress::mapping::pb::resource::v1::Resource as ProtoResource;
use rand::Rng;
use rand::SeedableRng;

pub const RUN_ID_KEY: &str = "bench.run_id";
pub const SEQ_KEY: &str = "bench.seq";
pub const RECORD_ID_KEY: &str = "bench.record_id";
pub const SERVICE_PREFIX: &str = "otel-sqlite-bench";

#[derive(Debug, Clone)]
pub struct WorkloadSpec {
    pub run_id: String,
    pub seed: u64,
    pub body_size: usize,
    pub attributes_per_record: usize,
    pub resource_count: usize,
}

impl WorkloadSpec {
    /// Sequence numbers are handed out in contiguous chunks; each chunk is
    /// rendered into one OTLP request. Records are spread round-robin over
    /// the configured number of distinct resources.
    pub fn render_request(&self, start_seq: u64, count: usize) -> ExportLogsServiceRequest {
        let resource_count = self.resource_count.max(1);
        let base_time: i64 = 1_700_000_000_000_000_000;
        let mut per_resource: Vec<Vec<ProtoLogRecord>> = vec![Vec::new(); resource_count];

        for seq in start_seq..start_seq + count as u64 {
            let bucket = (seq % resource_count as u64) as usize;
            per_resource[bucket].push(self.render_record(seq, base_time));
        }

        let resource_logs: Vec<ResourceLogs> = per_resource
            .into_iter()
            .enumerate()
            .filter(|(_, records)| !records.is_empty())
            .map(|(resource_index, log_records)| ResourceLogs {
                resource: Some(ProtoResource {
                    attributes: vec![string_kv(
                        "service.name",
                        format!("{SERVICE_PREFIX}-{resource_index}"),
                    )],
                    ..Default::default()
                }),
                scope_logs: vec![ScopeLogs {
                    scope: Some(
                        otel_sqlite_ingress::mapping::pb::common::v1::InstrumentationScope {
                            name: "otel-sqlite-e2e".to_owned(),
                            version: env!("CARGO_PKG_VERSION").to_owned(),
                            ..Default::default()
                        },
                    ),
                    log_records,
                    ..Default::default()
                }],
                schema_url: String::new(),
            })
            .collect();

        ExportLogsServiceRequest { resource_logs }
    }

    fn render_record(&self, seq: u64, base_time: i64) -> ProtoLogRecord {
        let mut rng = rand::rngs::StdRng::seed_from_u64(self.seed ^ seq);
        let severity = match seq % 10 {
            0..=6 => SeverityNumber::Info,
            7..=8 => SeverityNumber::Warn,
            _ => SeverityNumber::Error,
        };

        let mut attributes = Vec::with_capacity(3 + self.attributes_per_record);
        attributes.push(string_kv(RUN_ID_KEY, self.run_id.clone()));
        attributes.push(KeyValue {
            key: SEQ_KEY.to_owned(),
            value: Some(AnyValue {
                value: Some(AnyValueKind::IntValue(seq as i64)),
            }),
            ..Default::default()
        });
        attributes.push(KeyValue {
            key: RECORD_ID_KEY.to_owned(),
            value: Some(AnyValue {
                value: Some(AnyValueKind::IntValue(seq as i64)),
            }),
            ..Default::default()
        });
        for index in 0..self.attributes_per_record {
            attributes.push(string_kv(
                &format!("bench.attr.{index}"),
                format!("value-{seq}-{index}"),
            ));
        }

        ProtoLogRecord {
            time_unix_nano: (base_time + seq as i64) as u64,
            observed_time_unix_nano: (base_time + seq as i64) as u64,
            severity_number: severity as i32,
            severity_text: severity
                .as_str_name()
                .trim_start_matches("SEVERITY_NUMBER_")
                .to_owned(),
            trace_id: trace_id_for(seq),
            span_id: span_id_for(seq),
            body: Some(AnyValue {
                value: Some(AnyValueKind::StringValue(self.body_for(&mut rng, seq))),
            }),
            attributes,
            flags: 0,
            event_name: "benchmark".to_owned(),
            ..Default::default()
        }
    }

    fn body_for(&self, rng: &mut rand::rngs::StdRng, seq: u64) -> String {
        const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789";
        let prefix = format!("seq={seq} ");
        let fill = self.body_size.saturating_sub(prefix.len());
        let suffix: String = if fill > 0 {
            (0..fill)
                .map(|_| ALPHABET[rng.random_range(0..ALPHABET.len())] as char)
                .collect()
        } else {
            String::new()
        };
        prefix + &suffix
    }
}

fn string_kv(key: &str, value: String) -> KeyValue {
    KeyValue {
        key: key.to_owned(),
        value: Some(AnyValue {
            value: Some(AnyValueKind::StringValue(value)),
        }),
        ..Default::default()
    }
}

fn trace_id_for(seq: u64) -> Vec<u8> {
    let mut bytes = vec![0u8; 16];
    bytes[..8].copy_from_slice(&seq.to_be_bytes());
    bytes[8] = 0xB7;
    bytes
}

fn span_id_for(seq: u64) -> Vec<u8> {
    let mut bytes = vec![0u8; 8];
    bytes.copy_from_slice(&(seq ^ 0x5EED_5EED_5EED_5EED).to_be_bytes());
    bytes
}
