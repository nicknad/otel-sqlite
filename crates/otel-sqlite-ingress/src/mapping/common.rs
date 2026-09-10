//! Proto → model conversion helpers shared by the logs and metrics mappings.

use otel_sqlite_core::model::{Attribute, AttributeValue, Resource};

use crate::config::IngressConfig;
use crate::error::IngressError;
use crate::mapping::pb::common::v1::{AnyValue, KeyValue, any_value::Value as AnyValueKind};
use crate::mapping::pb::resource::v1::Resource as ProtoResource;

/// Maximum nesting depth for `ArrayValue`/`KvlistValue` structures (H3).
///
/// OTLP `AnyValue` is recursively defined; without a bound a single record
/// carrying thousands of nested `ArrayValue(ArrayValue(...))` overflows the
/// `spawn_blocking` worker stack and aborts the whole process (not just the
/// RPC). Legitimate telemetry nests a handful of levels deep; 32 is generous
/// while keeping stack use bounded (~32 frames of a small converter).
pub(crate) const MAX_NESTING_DEPTH: usize = 32;

/// Why a trace/span id present in the request cannot be mapped.
///
/// An absent id is an empty byte string and maps to "no trace context".
/// A present id that is not exactly the OTLP-fixed length (16 bytes trace,
/// 8 bytes span) or that is all zeroes is invalid per the OTLP spec and is
/// rejected rather than silently replaced.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IdError {
    InvalidLength { expected: usize, actual: usize },
    AllZeroes,
}

impl std::fmt::Display for IdError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidLength { expected, actual } => {
                write!(f, "id length {actual}, expected {expected} bytes")
            }
            Self::AllZeroes => f.write_str("id is all zeroes"),
        }
    }
}

pub(crate) fn parse_trace_id(value: Vec<u8>) -> Result<Option<[u8; 16]>, IdError> {
    parse_id(value)
}

pub(crate) fn parse_span_id(value: Vec<u8>) -> Result<Option<[u8; 8]>, IdError> {
    parse_id(value)
}

fn parse_id<const N: usize>(value: Vec<u8>) -> Result<Option<[u8; N]>, IdError> {
    if value.is_empty() {
        return Ok(None);
    }
    let actual = value.len();
    let Ok(id) = <[u8; N]>::try_from(value) else {
        return Err(IdError::InvalidLength {
            expected: N,
            actual,
        });
    };
    if id == [0; N] {
        return Err(IdError::AllZeroes);
    }
    Ok(Some(id))
}

pub(crate) fn attribute_value(value: AnyValue) -> Result<AttributeValue, IngressError> {
    attribute_value_with_depth(value, 0)
}

fn attribute_value_with_depth(
    value: AnyValue,
    depth: usize,
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
                .map(|nested| attribute_value_with_depth(nested, depth + 1))
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
                        value: attribute_value_with_depth(value.unwrap_or_default(), depth + 1)?,
                    })
                })
                .collect::<Result<Vec<_>, IngressError>>()
                .map(AttributeValue::Kvlist)
        }
        // Profiling-only `string_value_strindex` and empty AnyValues carry no
        // non-Profiling semantic content (per the proto); they map to null.
        _ => Ok(AttributeValue::Null),
    }
}

pub(crate) fn attributes(values: Vec<KeyValue>) -> Result<Vec<Attribute>, IngressError> {
    values
        .into_iter()
        .map(|KeyValue { key, value, .. }| {
            Ok(Attribute {
                key,
                value: attribute_value(value.unwrap_or_default())?,
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

pub(crate) fn convert_resource(value: ProtoResource) -> Result<Resource, IngressError> {
    Ok(Resource {
        id: String::new(),
        attributes: attributes(value.attributes)?,
        schema_url: String::new(),
    })
}
