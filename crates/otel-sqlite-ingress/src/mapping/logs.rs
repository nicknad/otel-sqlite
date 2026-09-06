use otel_sqlite_core::model::{Attribute, AttributeValue, LogBatch, LogRecord, Severity};
use otel_sqlite_core::storage::{BatchOrigin, IngestMessage, LogChunk};

use crate::error::IngressError;

use super::pb::common::v1::{AnyValue, InstrumentationScope, KeyValue};
use super::pb::logs::v1::{LogRecord as ProtoLogRecord, ResourceLogs, ScopeLogs};
use super::{
    MAX_NESTING_DEPTH, any_value_depth, attribute_value, attributes, convert_resource,
    parse_span_id, parse_trace_id,
};
use crate::config::IngressConfig;

/// Fallible proto → model conversion of one `ResourceLogs` group.
///
/// Unlike the previous infallible `From` impl, malformed trace/span ids are
/// rejected here instead of silently zero-filled: a request carrying an
/// invalid id is refused wholesale by the handler, so no record is persisted
/// under a corrupted trace context.
pub fn try_convert_batch(value: ResourceLogs) -> Result<LogBatch, IngressError> {
    let ResourceLogs {
        resource,
        scope_logs,
        schema_url,
    } = value;

    let capacity: usize = scope_logs.iter().map(|scope| scope.log_records.len()).sum();
    let mut batch = LogBatch::with_capacity(capacity);

    let mut resource = resource.map(convert_resource).transpose()?;
    if let Some(resource) = &mut resource {
        resource.schema_url.clone_from(&schema_url);
    }
    batch.resource = resource;

    for scope in scope_logs {
        let ScopeLogs {
            scope,
            log_records,
            schema_url: scope_schema_url,
        } = scope;
        let (scope_name, scope_version, scope_attributes) = scope
            .map(
                |InstrumentationScope {
                     name,
                     version,
                     attributes,
                     ..
                 }| (name, version, attributes),
            )
            .unwrap_or_default();
        let scope_attributes = attributes(scope_attributes)?;
        for record in log_records {
            batch.push(convert_record(
                record,
                &scope_name,
                &scope_version,
                &scope_attributes,
                &scope_schema_url,
            )?);
        }
    }

    batch.schema_url = schema_url;
    Ok(batch)
}

pub(crate) fn count(resource_logs: &[ResourceLogs]) -> usize {
    resource_logs
        .iter()
        .map(|resource| {
            resource
                .scope_logs
                .iter()
                .map(|scope| scope.log_records.len())
                .sum::<usize>()
        })
        .sum()
}

/// Maps an OTLP export request into mapped log chunks, one per
/// `ResourceLogs` group.
///
/// This is deliberately *not* a batching step: the OTLP batch boundaries are a
/// transport concern. Each chunk is handed to the storage-owned insert
/// batcher, which owns all grouping/splitting policy. Records move into the
/// chunks by ownership transfer without cloning. Mapping is fallible: a
/// malformed trace/span id fails the whole request before anything is
/// enqueued (all-or-nothing, matching P0-1).
pub(crate) fn map_chunks(
    resource_logs: Vec<ResourceLogs>,
) -> Result<Vec<(u64, IngestMessage)>, IngressError> {
    let mut chunks = Vec::with_capacity(resource_logs.len());
    for item in resource_logs {
        let LogBatch {
            records,
            resource,
            schema_url,
            ..
        } = try_convert_batch(item)?;
        if records.is_empty() {
            continue;
        }
        let count = records.len() as u64;
        chunks.push((
            count,
            IngestMessage::Logs(LogChunk {
                origin: BatchOrigin {
                    resource,
                    schema_url,
                },
                records,
                // No durability ledger exists yet; chunks start unstamped
                // and are stamped later once ticketing lands.
                commit_seq: 0,
            }),
        ));
    }
    Ok(chunks)
}

fn convert_record(
    value: ProtoLogRecord,
    scope_name: &str,
    scope_version: &str,
    scope_attributes: &[Attribute],
    scope_schema_url: &str,
) -> Result<LogRecord, IngressError> {
    let ProtoLogRecord {
        time_unix_nano,
        observed_time_unix_nano,
        severity_number,
        severity_text,
        trace_id,
        span_id,
        body,
        attributes: record_attributes,
        dropped_attributes_count,
        flags,
        event_name,
        ..
    } = value;

    let trace_id = parse_trace_id(trace_id)
        .map_err(|error| IngressError::Mapping(format!("invalid trace id: {error}")))?;
    let span_id = parse_span_id(span_id)
        .map_err(|error| IngressError::Mapping(format!("invalid span id: {error}")))?;
    let (body, body_json) = body_value(body)?;

    Ok(LogRecord {
        time_unix_nano: time_unix_nano as i64,
        observed_time_unix_nano: observed_time_unix_nano as i64,
        severity_number: severity(severity_number),
        severity_text,
        trace_id: trace_id.unwrap_or([0; 16]),
        span_id: span_id.unwrap_or([0; 8]),
        body,
        body_json,
        attributes: attributes(record_attributes)?,
        dropped_attributes_count,
        flags,
        event_name,
        resource_id: String::new(),
        resource: None,
        scope_name: scope_name.to_owned(),
        scope_version: scope_version.to_owned(),
        scope_attributes: scope_attributes.to_vec(),
        scope_schema_url: scope_schema_url.to_owned(),
    })
}

fn severity(value: i32) -> Severity {
    if let Some(severity) = u8::try_from(value).ok().and_then(Severity::from_u8) {
        severity
    } else {
        ::metrics::counter!(
            "otlp_mapping_loss_total",
            "signal" => "logs",
            "reason" => "unknown_severity"
        )
        .increment(1);
        Severity::Unspecified
    }
}

/// Maps the OTLP `AnyValue` body into its storage rendering.
///
/// Scalars render as today (bytes via lossy UTF-8). Structured bodies
/// (`ArrayValue` / `KvlistValue`) are preserved as canonical JSON in
/// `body_json`, which the storage layer persists into the `body` column —
/// the same canonical encoding as nested attribute values, so nothing is
/// silently flattened to an empty string.
///
/// Depth-limited like `attribute_value`: over-deep bodies fail instead of
/// overflowing the worker stack.
fn body_value(value: Option<super::AnyValue>) -> Result<(String, Option<String>), IngressError> {
    match value.and_then(|any| any.value) {
        Some(super::AnyValueKind::StringValue(value)) => Ok((value, None)),
        Some(super::AnyValueKind::IntValue(value)) => Ok((value.to_string(), None)),
        Some(super::AnyValueKind::DoubleValue(value)) => Ok((value.to_string(), None)),
        Some(super::AnyValueKind::BoolValue(value)) => Ok((value.to_string(), None)),
        Some(super::AnyValueKind::BytesValue(value)) => {
            Ok((String::from_utf8_lossy(&value).into_owned(), None))
        }
        Some(super::AnyValueKind::ArrayValue(values)) => {
            let items = values
                .values
                .into_iter()
                .map(attribute_value)
                .collect::<Result<Vec<_>, _>>()?;
            Ok((
                String::new(),
                Some(AttributeValue::Array(items).to_canonical_json()),
            ))
        }
        Some(super::AnyValueKind::KvlistValue(values)) => {
            let items = values
                .values
                .into_iter()
                .map(|KeyValue { key, value, .. }| {
                    Ok(Attribute {
                        key,
                        value: attribute_value(value.unwrap_or_default())?,
                    })
                })
                .collect::<Result<Vec<_>, IngressError>>()?;
            Ok((
                String::new(),
                Some(AttributeValue::Kvlist(items).to_canonical_json()),
            ))
        }
        _ => Ok((String::new(), None)),
    }
}

/// Pre-mapping size guard (H2): walks the borrowed request without allocating
/// model values and rejects hostile shapes with `INVALID_ARGUMENT` *before*
/// the blocking mapping task runs.
///
/// Checked per `IngressConfig` limits: attribute-vector lengths, key/value
/// byte lengths, scalar body bytes, and `AnyValue` nesting depth (via the
/// non-recursive `any_value_depth` probe, so the check itself cannot overflow).
/// Trace/span id *validity* stays in `try_convert_batch`; this only bounds
/// allocation/CPU.
pub(crate) fn validate_request(
    resource_logs: &[ResourceLogs],
    limits: &IngressConfig,
) -> Result<(), IngressError> {
    for group in resource_logs {
        check_attributes(
            group
                .resource
                .as_ref()
                .map_or(&[], |r| r.attributes.as_slice()),
            limits,
            "resource.attributes",
        )?;
        for scope in &group.scope_logs {
            if let Some(s) = scope.scope.as_ref() {
                check_attributes(&s.attributes, limits, "scope.attributes")?;
            }
            for record in &scope.log_records {
                check_attributes(&record.attributes, limits, "log.attributes")?;
                check_plain_string(&record.severity_text, limits, "severity_text")?;
                check_plain_string(&record.event_name, limits, "event_name")?;
                if let Some(s) = scope.scope.as_ref() {
                    check_plain_string(&s.name, limits, "scope.name")?;
                    check_plain_string(&s.version, limits, "scope.version")?;
                }
                check_body(record.body.as_ref(), limits)?;
            }
        }
    }
    Ok(())
}

fn check_attributes(
    values: &[KeyValue],
    limits: &IngressConfig,
    what: &str,
) -> Result<(), IngressError> {
    if values.len() > limits.max_attributes_per_record {
        return Err(IngressError::Mapping(format!(
            "{what} holds {} attributes, limit is {}",
            values.len(),
            limits.max_attributes_per_record
        )));
    }
    for entry in values {
        if entry.key.len() > limits.max_attribute_key_bytes {
            return Err(IngressError::Mapping(format!(
                "{what} key exceeds {} bytes (limit {})",
                entry.key.len(),
                limits.max_attribute_key_bytes
            )));
        }
        if let Some(value) = entry.value.as_ref() {
            check_any_value(value, limits, what)?;
        }
    }
    Ok(())
}

fn check_any_value(
    value: &AnyValue,
    limits: &IngressConfig,
    what: &str,
) -> Result<(), IngressError> {
    if any_value_depth(value) > MAX_NESTING_DEPTH {
        return Err(IngressError::Mapping(format!(
            "{what} exceeds maximum nesting depth of {MAX_NESTING_DEPTH}"
        )));
    }
    // Iterative leaf-size walk (explicit stack: never recurses).
    let mut stack: Vec<&AnyValue> = vec![value];
    while let Some(current) = stack.pop() {
        match &current.value {
            Some(super::AnyValueKind::StringValue(s)) => {
                if s.len() > limits.max_attribute_value_bytes {
                    return Err(IngressError::Mapping(format!(
                        "{what} string value exceeds {} bytes (limit {})",
                        s.len(),
                        limits.max_attribute_value_bytes
                    )));
                }
            }
            Some(super::AnyValueKind::BytesValue(b)) => {
                if b.len() > limits.max_attribute_value_bytes {
                    return Err(IngressError::Mapping(format!(
                        "{what} bytes value exceeds {} bytes (limit {})",
                        b.len(),
                        limits.max_attribute_value_bytes
                    )));
                }
            }
            Some(super::AnyValueKind::ArrayValue(array)) => {
                stack.extend(array.values.iter());
            }
            Some(super::AnyValueKind::KvlistValue(list)) => {
                for entry in &list.values {
                    if entry.key.len() > limits.max_attribute_key_bytes {
                        return Err(IngressError::Mapping(format!(
                            "{what} nested key exceeds {} bytes (limit {})",
                            entry.key.len(),
                            limits.max_attribute_key_bytes
                        )));
                    }
                    if let Some(nested) = entry.value.as_ref() {
                        stack.push(nested);
                    }
                }
            }
            _ => {}
        }
    }
    Ok(())
}

fn check_plain_string(value: &str, limits: &IngressConfig, what: &str) -> Result<(), IngressError> {
    if value.len() > limits.max_attribute_value_bytes {
        return Err(IngressError::Mapping(format!(
            "{what} exceeds {} bytes (limit {})",
            value.len(),
            limits.max_attribute_value_bytes
        )));
    }
    Ok(())
}

fn check_body(body: Option<&AnyValue>, limits: &IngressConfig) -> Result<(), IngressError> {
    let Some(value) = body else { return Ok(()) };
    match &value.value {
        Some(super::AnyValueKind::StringValue(s)) => {
            if s.len() > limits.max_body_bytes {
                return Err(IngressError::Mapping(format!(
                    "log body exceeds {} bytes (limit {})",
                    s.len(),
                    limits.max_body_bytes
                )));
            }
            Ok(())
        }
        Some(super::AnyValueKind::BytesValue(b)) => {
            if b.len() > limits.max_body_bytes {
                return Err(IngressError::Mapping(format!(
                    "log body exceeds {} bytes (limit {})",
                    b.len(),
                    limits.max_body_bytes
                )));
            }
            Ok(())
        }
        // Structured bodies reuse the attribute byte/depth budget: leaves are
        // attacker-controlled JSON that costs canonical-sort + FTS CPU.
        _ => check_any_value(value, limits, "log.body"),
    }
}

#[cfg(test)]
// Helpers mirror the `Option`-shaped proto fields they construct.
#[allow(clippy::unnecessary_wraps)]
mod tests {
    use super::*;
    use crate::mapping::pb::common::v1::{
        AnyValue, ArrayValue, KeyValue, KeyValueList, any_value::Value,
    };
    use crate::mapping::pb::logs::v1::SeverityNumber;
    use crate::mapping::pb::resource::v1::Resource as ProtoResource;
    use otel_sqlite_core::model::{AttributeValue, Resource, Severity};

    fn any(kind: Value) -> Option<AnyValue> {
        Some(AnyValue { value: Some(kind) })
    }

    fn sample_resource() -> Option<ProtoResource> {
        Some(ProtoResource {
            attributes: vec![KeyValue {
                key: "service.name".to_owned(),
                value: any(Value::StringValue("checkout".to_owned())),
                ..Default::default()
            }],
            ..Default::default()
        })
    }

    fn sample_record() -> ProtoLogRecord {
        ProtoLogRecord {
            time_unix_nano: 111,
            observed_time_unix_nano: 222,
            severity_number: SeverityNumber::Error as i32,
            severity_text: "ERROR".to_owned(),
            trace_id: vec![7; 16],
            span_id: vec![9; 8],
            body: any(Value::StringValue("hello".to_owned())),
            attributes: vec![KeyValue {
                key: "attempt".to_owned(),
                value: any(Value::IntValue(42)),
                ..Default::default()
            }],
            flags: 1,
            event_name: "order.placed".to_owned(),
            ..Default::default()
        }
    }

    #[test]
    fn converts_resource_logs_into_model_batch() {
        let proto = ResourceLogs {
            resource: sample_resource(),
            scope_logs: vec![ScopeLogs {
                scope: Some(InstrumentationScope {
                    name: "scope-a".to_owned(),
                    version: "1.2.3".to_owned(),
                    attributes: vec![KeyValue {
                        key: "scope.tag".to_owned(),
                        value: any(Value::StringValue("prod".to_owned())),
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
                log_records: vec![
                    sample_record(),
                    ProtoLogRecord {
                        severity_number: 99,
                        body: any(Value::IntValue(5)),
                        attributes: vec![KeyValue {
                            key: "complex".to_owned(),
                            value: any(Value::KvlistValue(KeyValueList::default())),
                            ..Default::default()
                        }],
                        ..Default::default()
                    },
                ],
                schema_url: "https://example.test/schemas/logs/scope".to_owned(),
            }],
            schema_url: "https://example.test/schemas/logs".to_owned(),
        };

        let batch = try_convert_batch(proto).expect("valid request converts");

        assert_eq!(batch.len(), 2);
        assert_eq!(batch.schema_url, "https://example.test/schemas/logs");
        assert_eq!(
            batch.resource.as_ref().map(Resource::service_name),
            Some("checkout")
        );
        assert_eq!(
            batch
                .resource
                .as_ref()
                .map(|resource| resource.schema_url.as_str()),
            Some("https://example.test/schemas/logs")
        );

        let first = &batch.records[0];
        assert_eq!(first.time_unix_nano, 111);
        assert_eq!(first.observed_time_unix_nano, 222);
        assert_eq!(first.severity_number, Severity::Error);
        assert_eq!(first.trace_id, [7; 16]);
        assert_eq!(first.span_id, [9; 8]);
        assert!(first.has_trace_context());
        assert_eq!(first.body, "hello");
        assert_eq!(first.attributes[0].value.as_int(), Some(42));
        assert_eq!(first.scope_name, "scope-a");
        assert_eq!(first.scope_version, "1.2.3");
        assert_eq!(first.event_name, "order.placed");
        assert_eq!(first.scope_attributes[0].value.as_str(), Some("prod"));
        assert_eq!(
            first.scope_schema_url,
            "https://example.test/schemas/logs/scope"
        );

        let second = &batch.records[1];
        assert_eq!(second.severity_number, Severity::Unspecified);
        assert!(!second.has_trace());
        assert!(!second.has_span());
        assert_eq!(second.body, "5");
        assert_eq!(second.attributes[0].value, AttributeValue::Kvlist(vec![]));
    }

    #[test]
    fn nested_array_and_kvlist_bodies_are_preserved_as_canonical_json() {
        let kvlist_body = AnyValue {
            value: Some(Value::KvlistValue(KeyValueList {
                values: vec![
                    KeyValue {
                        key: "b".to_owned(),
                        value: any(Value::IntValue(1)),
                        ..Default::default()
                    },
                    KeyValue {
                        key: "a".to_owned(),
                        value: any(Value::StringValue("x\"y".to_owned())),
                        ..Default::default()
                    },
                ],
            })),
        };
        let array_body = AnyValue {
            value: Some(Value::ArrayValue(ArrayValue {
                values: vec![
                    AnyValue {
                        value: Some(Value::BoolValue(true)),
                    },
                    AnyValue {
                        value: Some(Value::KvlistValue(KeyValueList {
                            values: vec![KeyValue {
                                key: "n".to_owned(),
                                value: any(Value::DoubleValue(1.5)),
                                ..Default::default()
                            }],
                        })),
                    },
                ],
            })),
        };

        let proto = ResourceLogs {
            scope_logs: vec![ScopeLogs {
                log_records: vec![
                    ProtoLogRecord {
                        body: Some(kvlist_body),
                        ..Default::default()
                    },
                    ProtoLogRecord {
                        body: Some(array_body),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }],
            ..Default::default()
        };

        let batch = try_convert_batch(proto).expect("structured bodies are valid");

        // Sorted ascending keys, JSON escaping, duplicate keys collapse.
        assert_eq!(
            batch.records[0].body_json.as_deref(),
            Some(r#"{"a":"x\"y","b":1}"#)
        );
        assert_eq!(
            batch.records[1].body_json.as_deref(),
            Some(r#"[true,{"n":1.5}]"#)
        );
        assert!(batch.records[0].body.is_empty());
    }

    #[test]
    fn rejects_malformed_trace_and_span_ids() {
        let malformed = |trace_id: Vec<u8>, span_id: Vec<u8>| {
            let proto = ResourceLogs {
                scope_logs: vec![ScopeLogs {
                    log_records: vec![ProtoLogRecord {
                        trace_id,
                        span_id,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            };
            map_chunks(vec![proto])
        };

        // Wrong trace length.
        assert!(malformed(vec![1, 2], vec![9; 8]).is_err());
        // Wrong span length.
        assert!(malformed(vec![7; 16], vec![1]).is_err());
        // Present-but-all-zeroes ids are invalid per the OTLP spec.
        assert!(malformed(vec![0; 16], vec![9; 8]).is_err());
        assert!(malformed(vec![7; 16], vec![0; 8]).is_err());
        // Absent ids (empty) are valid "no trace context".
        assert!(malformed(vec![], vec![]).is_ok());
    }

    #[test]
    fn count_sums_all_scope_records() {
        let proto = ResourceLogs {
            scope_logs: vec![
                ScopeLogs {
                    log_records: vec![sample_record(), sample_record()],
                    ..Default::default()
                },
                ScopeLogs {
                    log_records: vec![sample_record()],
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let empty = ResourceLogs::default();

        assert_eq!(count(&[proto, empty]), 3);
    }

    #[test]
    fn maps_one_chunk_per_resource_logs_group_without_batching_policy() {
        let shared = || ResourceLogs {
            resource: sample_resource(),
            schema_url: "https://example.test/schemas/logs".to_owned(),
            scope_logs: vec![ScopeLogs {
                log_records: vec![sample_record()],
                ..Default::default()
            }],
        };

        let other = ResourceLogs {
            resource: None,
            scope_logs: vec![ScopeLogs {
                log_records: vec![sample_record()],
                ..Default::default()
            }],
            ..Default::default()
        };

        let chunks = map_chunks(vec![shared(), shared(), other]).expect("valid request");

        // One chunk per OTLP group, in order; grouping/splitting is the
        // storage insert batcher's job, not ingress's.
        assert_eq!(chunks.len(), 3);
        for (records, command) in &chunks {
            assert_eq!(*records, 1);
            match command {
                IngestMessage::Logs(chunk) => assert_eq!(chunk.records.len(), 1),
                _ => panic!("expected insert logs chunk"),
            }
        }
        let IngestMessage::Logs(first) = &chunks[0].1 else {
            panic!("expected logs chunk");
        };
        assert!(first.origin.resource.is_some());
        assert_eq!(first.origin.schema_url, "https://example.test/schemas/logs");

        let oversized = ResourceLogs {
            scope_logs: vec![ScopeLogs {
                log_records: (0..5).map(|_| sample_record()).collect(),
                ..Default::default()
            }],
            ..Default::default()
        };

        let chunks = map_chunks(vec![oversized]).expect("valid request");
        assert_eq!(chunks.len(), 1);
        let (records, command) = &chunks[0];
        assert_eq!(*records, 5);
        match command {
            IngestMessage::Logs(chunk) => assert_eq!(chunk.records.len(), 5),
            _ => panic!("expected insert logs chunk"),
        }
    }

    #[test]
    fn empty_groups_produce_no_chunks() {
        let chunks = map_chunks(vec![ResourceLogs::default()]).expect("valid request");
        assert!(chunks.is_empty());
    }

    #[test]
    fn validate_rejects_too_many_attributes_per_record() {
        let proto = ResourceLogs {
            scope_logs: vec![ScopeLogs {
                log_records: vec![ProtoLogRecord {
                    attributes: (0..100)
                        .map(|i| KeyValue {
                            key: format!("k{i}"),
                            value: any(Value::IntValue(i as i64)),
                            ..Default::default()
                        })
                        .collect(),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let limits = IngressConfig {
            max_attributes_per_record: 50,
            ..IngressConfig::default()
        };
        let error =
            validate_request(&[proto], &limits).expect_err("100 attributes must exceed the 50 cap");
        assert!(error.to_string().contains("log.attributes"));
    }

    #[test]
    fn validate_rejects_attribute_key_and_value_bytes() {
        let long_key = KeyValue {
            key: "x".repeat(700),
            value: any(Value::IntValue(1)),
            ..Default::default()
        };
        let long_value = KeyValue {
            key: "k".to_owned(),
            value: any(Value::StringValue("y".repeat(70_000))),
            ..Default::default()
        };
        let limits = IngressConfig {
            max_attribute_key_bytes: 512,
            max_attribute_value_bytes: 4096,
            ..IngressConfig::default()
        };
        let record = |attrs: Vec<KeyValue>| ResourceLogs {
            scope_logs: vec![ScopeLogs {
                log_records: vec![ProtoLogRecord {
                    attributes: attrs,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        assert!(
            validate_request(&[record(vec![long_key])], &limits).is_err(),
            "over-long key must be rejected"
        );
        assert!(
            validate_request(&[record(vec![long_value])], &limits).is_err(),
            "over-long value must be rejected"
        );
    }

    #[test]
    fn validate_rejects_oversized_log_body() {
        let proto = ResourceLogs {
            scope_logs: vec![ScopeLogs {
                log_records: vec![ProtoLogRecord {
                    body: any(Value::StringValue("z".repeat(2_000_000))),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let limits = IngressConfig {
            max_body_bytes: 1024 * 1024,
            ..IngressConfig::default()
        };
        assert!(
            validate_request(&[proto], &limits).is_err(),
            "2 MiB scalar body must exceed the 1 MiB cap"
        );
    }

    #[test]
    fn validate_rejects_over_deep_attribute_nesting() {
        // 40 levels of ArrayValue(ArrayValue(...)) around a leaf.
        let mut value = AnyValue {
            value: Some(Value::StringValue("leaf".to_owned())),
        };
        for _ in 0..40 {
            value = AnyValue {
                value: Some(Value::ArrayValue(ArrayValue {
                    values: vec![value],
                })),
            };
        }
        let proto = ResourceLogs {
            scope_logs: vec![ScopeLogs {
                log_records: vec![ProtoLogRecord {
                    attributes: vec![KeyValue {
                        key: "deep".to_owned(),
                        value: Some(value),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        assert!(
            validate_request(&[proto], &IngressConfig::default()).is_err(),
            "40 levels of nesting must exceed MAX_NESTING_DEPTH"
        );
    }

    #[test]
    fn validate_accepts_bounded_shape() {
        let proto = ResourceLogs {
            resource: sample_resource(),
            scope_logs: vec![ScopeLogs {
                scope: Some(InstrumentationScope {
                    name: "scope-a".to_owned(),
                    version: "1.2.3".to_owned(),
                    attributes: vec![KeyValue {
                        key: "scope.tag".to_owned(),
                        value: any(Value::StringValue("prod".to_owned())),
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
                log_records: vec![sample_record()],
                ..Default::default()
            }],
            ..Default::default()
        };
        validate_request(&[proto], &IngressConfig::default())
            .expect("a bounded request passes validation");
    }
}
