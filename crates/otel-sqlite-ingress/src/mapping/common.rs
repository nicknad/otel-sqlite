//! Proto → model conversion helpers shared by the logs and metrics mappings.

use otel_sqlite_core::model::{Attribute, AttributeValue, Resource};

use crate::config::IngressConfig;
use crate::error::IngressError;
use crate::mapping::pb::common::v1::{
    AnyValue, InstrumentationScope, KeyValue, any_value::Value as AnyValueKind,
};
use crate::mapping::pb::resource::v1::Resource as ProtoResource;

/// Maximum nesting depth for `ArrayValue`/`KvlistValue` structures (H3).
///
/// OTLP `AnyValue` is recursively defined; without a bound a single record
/// carrying thousands of nested `ArrayValue(ArrayValue(...))` overflows the
/// `spawn_blocking` worker stack and aborts the whole process (not just the
/// RPC). Legitimate telemetry nests a handful of levels deep; 32 is generous
/// while keeping stack use bounded (~32 frames of a small converter).
pub(crate) const MAX_NESTING_DEPTH: usize = 32;

/// Outcome of parsing an OTLP-fixed-length trace/span id.
///
/// An absent id is an empty byte string and maps to "no trace context".
/// A present id that is not exactly the OTLP-fixed length (16 bytes trace,
/// 8 bytes span) or that is all zeroes carries no usable trace association:
/// the OTLP spec says the receiver SHOULD assume no trace association, so it
/// maps to "no trace context" too — counted as a mapping loss, never a
/// request failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ParsedId<const N: usize> {
    Absent,
    Valid([u8; N]),
    Invalid,
}

impl<const N: usize> ParsedId<N> {
    /// Returns the id when valid, `None` for `Absent`, and `None` plus an
    /// `otlp_mapping_loss_total` increment for present-but-invalid ids.
    pub(crate) fn into_id(self, signal: &'static str, reason: &'static str) -> Option<[u8; N]> {
        match self {
            Self::Valid(id) => Some(id),
            Self::Absent => None,
            Self::Invalid => {
                ::metrics::counter!(
                    "otlp_mapping_loss_total",
                    "signal" => signal,
                    "reason" => reason
                )
                .increment(1);
                None
            }
        }
    }
}

pub(crate) fn parse_trace_id(value: Vec<u8>) -> ParsedId<16> {
    parse_id(value)
}

pub(crate) fn parse_span_id(value: Vec<u8>) -> ParsedId<8> {
    parse_id(value)
}

fn parse_id<const N: usize>(value: Vec<u8>) -> ParsedId<N> {
    if value.is_empty() {
        return ParsedId::Absent;
    }
    match <[u8; N]>::try_from(value) {
        Ok(id) if id != [0; N] => ParsedId::Valid(id),
        _ => ParsedId::Invalid,
    }
}

/// Converts an OTLP `fixed64` unix-nano timestamp to the model's `i64`.
///
/// Values above `i64::MAX` saturate to `i64::MAX` instead of wrapping into a
/// negative instant that retention would delete; each saturation counts
/// `otlp_mapping_loss_total{reason="timestamp_overflow"}`.
pub(crate) fn timestamp(value: u64, signal: &'static str) -> i64 {
    i64::try_from(value).unwrap_or_else(|_| {
        ::metrics::counter!(
            "otlp_mapping_loss_total",
            "signal" => signal,
            "reason" => "timestamp_overflow"
        )
        .increment(1);
        i64::MAX
    })
}

pub(crate) fn attribute_value(
    value: AnyValue,
    signal: &'static str,
) -> Result<AttributeValue, IngressError> {
    attribute_value_with_depth(value, 0, signal)
}

fn attribute_value_with_depth(
    value: AnyValue,
    depth: usize,
    signal: &'static str,
) -> Result<AttributeValue, IngressError> {
    match value.value {
        Some(AnyValueKind::StringValue(value)) => Ok(AttributeValue::String(value)),
        Some(AnyValueKind::IntValue(value)) => Ok(AttributeValue::Int(value)),
        Some(AnyValueKind::DoubleValue(value)) => Ok(AttributeValue::Double(value)),
        Some(AnyValueKind::BoolValue(value)) => Ok(AttributeValue::Bool(value)),
        Some(AnyValueKind::BytesValue(value)) => Ok(AttributeValue::Bytes(value)),
        // Nested structures are preserved as model values; the canonical JSON
        // rendering is owned by the core model so ingress (structured bodies)
        // and storage (nested attribute values) persist byte-identical output.
        // Depth is enforced before recursing so a hostile nesting chain fails
        // as `INVALID_ARGUMENT` instead of overflowing the worker stack.
        Some(AnyValueKind::ArrayValue(array)) => {
            if depth >= MAX_NESTING_DEPTH {
                return Err(IngressError::Mapping(format!(
                    "attribute value exceeds maximum nesting depth of {MAX_NESTING_DEPTH}"
                )));
            }
            array
                .values
                .into_iter()
                .map(|nested| attribute_value_with_depth(nested, depth + 1, signal))
                .collect::<Result<Vec<_>, _>>()
                .map(AttributeValue::Array)
        }
        Some(AnyValueKind::KvlistValue(list)) => {
            if depth >= MAX_NESTING_DEPTH {
                return Err(IngressError::Mapping(format!(
                    "attribute value exceeds maximum nesting depth of {MAX_NESTING_DEPTH}"
                )));
            }
            list.values
                .into_iter()
                .map(|KeyValue { key, value, .. }| {
                    Ok(Attribute {
                        key,
                        value: attribute_value_with_depth(
                            value.unwrap_or_default(),
                            depth + 1,
                            signal,
                        )?,
                    })
                })
                .collect::<Result<Vec<_>, IngressError>>()
                .map(AttributeValue::Kvlist)
        }
        // `string_value_strindex` references the Profiling dictionary string
        // table; for non-Profiling signals the proto says to process the value
        // as if absent, so it maps to null and counts as an explicit loss.
        Some(AnyValueKind::StringValueStrindex(_)) => {
            ::metrics::counter!(
                "otlp_mapping_loss_total",
                "signal" => signal,
                "reason" => "string_value_strindex"
            )
            .increment(1);
            Ok(AttributeValue::Null)
        }
        // An empty AnyValue (no oneof set) is a valid "empty", not a loss.
        None => Ok(AttributeValue::Null),
    }
}

pub(crate) fn attributes(
    values: Vec<KeyValue>,
    signal: &'static str,
) -> Result<Vec<Attribute>, IngressError> {
    values
        .into_iter()
        .map(|KeyValue { key, value, .. }| {
            Ok(Attribute {
                key,
                value: attribute_value(value.unwrap_or_default(), signal)?,
            })
        })
        .collect()
}

/// Non-allocating depth probe over a borrowed `AnyValue`: returns the deepest
/// nesting level without building any model values. Implemented iteratively
/// with an explicit stack so the probe itself cannot overflow on hostile input
/// (the converting path above is only reached after the request-level
/// validator has already rejected over-deep payloads).
pub(crate) fn any_value_depth(value: &AnyValue) -> usize {
    let mut deepest = 0usize;
    let mut stack: Vec<(&AnyValue, usize)> = vec![(value, 0)];
    while let Some((current, depth)) = stack.pop() {
        deepest = deepest.max(depth);
        match &current.value {
            Some(AnyValueKind::ArrayValue(array)) => {
                for nested in &array.values {
                    stack.push((nested, depth + 1));
                }
            }
            Some(AnyValueKind::KvlistValue(list)) => {
                for entry in &list.values {
                    if let Some(nested) = entry.value.as_ref() {
                        stack.push((nested, depth + 1));
                    }
                }
            }
            _ => {}
        }
    }
    deepest
}

pub(crate) fn check_attributes(
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

pub(crate) fn check_any_value(
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
            Some(AnyValueKind::StringValue(s)) => {
                if s.len() > limits.max_attribute_value_bytes {
                    return Err(IngressError::Mapping(format!(
                        "{what} string value exceeds {} bytes (limit {})",
                        s.len(),
                        limits.max_attribute_value_bytes
                    )));
                }
            }
            Some(AnyValueKind::BytesValue(b)) => {
                if b.len() > limits.max_attribute_value_bytes {
                    return Err(IngressError::Mapping(format!(
                        "{what} bytes value exceeds {} bytes (limit {})",
                        b.len(),
                        limits.max_attribute_value_bytes
                    )));
                }
            }
            Some(AnyValueKind::ArrayValue(array)) => {
                stack.extend(array.values.iter());
            }
            Some(AnyValueKind::KvlistValue(list)) => {
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

pub(crate) fn check_plain_string(
    value: &str,
    limits: &IngressConfig,
    what: &str,
) -> Result<(), IngressError> {
    if value.len() > limits.max_attribute_value_bytes {
        return Err(IngressError::Mapping(format!(
            "{what} exceeds {} bytes (limit {})",
            value.len(),
            limits.max_attribute_value_bytes
        )));
    }
    Ok(())
}

/// Estimated model bytes of one `AnyValue`, including nested containers.
///
/// Only feeds the scope-expansion admission check, so the estimate is
/// deliberately coarse (container overhead approximated by the protobuf
/// struct size) and never allocates. Iterative like the other probes.
pub(crate) fn any_value_size(value: &AnyValue) -> usize {
    let mut total = 0usize;
    let mut stack: Vec<&AnyValue> = vec![value];
    while let Some(current) = stack.pop() {
        total = total.saturating_add(std::mem::size_of::<AnyValue>());
        match &current.value {
            Some(AnyValueKind::StringValue(s)) => {
                total = total.saturating_add(s.len());
            }
            Some(AnyValueKind::BytesValue(b)) => {
                total = total.saturating_add(b.len());
            }
            Some(AnyValueKind::ArrayValue(array)) => {
                stack.extend(array.values.iter());
            }
            Some(AnyValueKind::KvlistValue(list)) => {
                for entry in &list.values {
                    total = total.saturating_add(
                        entry
                            .key
                            .len()
                            .saturating_add(std::mem::size_of::<KeyValue>()),
                    );
                    if let Some(nested) = entry.value.as_ref() {
                        stack.push(nested);
                    }
                }
            }
            _ => {}
        }
    }
    total
}

/// Estimated model bytes of one scope metadata block; see
/// `IngressConfig::max_scope_metadata_expansion_bytes`.
pub(crate) fn scope_metadata_bytes(
    scope: Option<&InstrumentationScope>,
    schema_url: &str,
) -> usize {
    let Some(scope) = scope else {
        return schema_url.len();
    };
    let mut total = scope
        .name
        .len()
        .saturating_add(scope.version.len())
        .saturating_add(schema_url.len());
    for entry in &scope.attributes {
        total = total.saturating_add(
            entry
                .key
                .len()
                .saturating_add(std::mem::size_of::<Attribute>()),
        );
        if let Some(value) = entry.value.as_ref() {
            total = total.saturating_add(any_value_size(value));
        }
    }
    total
}

/// Rejects requests whose scope metadata is repeated across so many member
/// records that the mapped model would amplify far beyond the wire budget.
///
/// The budget models `metadata_bytes * members` — the amplification the
/// request could force if every record copied its scope metadata. Since the
/// mapped model now shares one `LogScope` per group, the check is a
/// conservative worst-case bound, not the actual mapped footprint. Checked
/// with division to stay overflow-free for hostile `members` values.
pub(crate) fn check_scope_expansion(
    metadata_bytes: usize,
    members: usize,
    limits: &IngressConfig,
    what: &str,
) -> Result<(), IngressError> {
    if members == 0 || metadata_bytes == 0 {
        return Ok(());
    }
    let budget = limits.max_scope_metadata_expansion_bytes;
    if metadata_bytes > budget / members {
        return Err(IngressError::Mapping(format!(
            "{what} scope metadata ({metadata_bytes} bytes) repeated across {members} records \
             exceeds the {budget}-byte expansion budget; reduce scope attributes or split the request"
        )));
    }
    Ok(())
}

/// Converts one OTLP resource.
///
/// `Resource.dropped_attributes_count` and `Resource.entity_refs` are not
/// persisted: the model has no columns for them, so they are dropped without
/// a counter (they describe sender-side loss and entity identity, not record
/// content).
pub(crate) fn convert_resource(
    value: ProtoResource,
    signal: &'static str,
) -> Result<Resource, IngressError> {
    Ok(Resource {
        id: String::new(),
        attributes: attributes(value.attributes, signal)?,
        schema_url: String::new(),
    })
}
