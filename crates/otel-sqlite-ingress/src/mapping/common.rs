//! Proto → model conversion helpers shared by the logs and metrics mappings.

use otel_sqlite_core::model::{Attribute, AttributeValue, Resource};

use crate::mapping::pb::common::v1::{AnyValue, KeyValue, any_value::Value as AnyValueKind};
use crate::mapping::pb::resource::v1::Resource as ProtoResource;

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

pub(crate) fn attribute_value(value: AnyValue) -> AttributeValue {
    match value.value {
        Some(AnyValueKind::StringValue(value)) => AttributeValue::String(value),
        Some(AnyValueKind::IntValue(value)) => AttributeValue::Int(value),
        Some(AnyValueKind::DoubleValue(value)) => AttributeValue::Double(value),
        Some(AnyValueKind::BoolValue(value)) => AttributeValue::Bool(value),
        Some(AnyValueKind::BytesValue(value)) => AttributeValue::Bytes(value),
        // Nested structures are preserved as model values; the canonical JSON
        // rendering is owned by the core model so ingress (structured bodies)
        // and storage (nested attribute values) persist byte-identical output.
        Some(AnyValueKind::ArrayValue(array)) => {
            AttributeValue::Array(array.values.into_iter().map(attribute_value).collect())
        }
        Some(AnyValueKind::KvlistValue(list)) => AttributeValue::Kvlist(
            list.values
                .into_iter()
                .map(|KeyValue { key, value, .. }| Attribute {
                    key,
                    value: attribute_value(value.unwrap_or_default()),
                })
                .collect(),
        ),
        // Profiling-only `string_value_strindex` and empty AnyValues carry no
        // non-Profiling semantic content (per the proto); they map to null.
        _ => AttributeValue::Null,
    }
}

pub(crate) fn attributes(values: Vec<KeyValue>) -> Vec<Attribute> {
    values
        .into_iter()
        .map(|KeyValue { key, value, .. }| Attribute {
            key,
            value: attribute_value(value.unwrap_or_default()),
        })
        .collect()
}

impl From<ProtoResource> for Resource {
    fn from(value: ProtoResource) -> Self {
        Self {
            id: String::new(),
            attributes: attributes(value.attributes),
            schema_url: String::new(),
        }
    }
}
