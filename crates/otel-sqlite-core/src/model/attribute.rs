use std::fmt;

use serde::ser::{SerializeMap, SerializeSeq};
use serde::{Serialize, Serializer};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[repr(u8)]
pub enum ValueType {
    #[default]
    Null = 0,
    String = 1,
    Int = 2,
    Double = 3,
    Bool = 4,
    Bytes = 5,
    Array = 6,
    Kvlist = 7,
}

impl ValueType {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Null => "null",
            Self::String => "string",
            Self::Int => "int",
            Self::Double => "double",
            Self::Bool => "bool",
            Self::Bytes => "bytes",
            Self::Array => "array",
            Self::Kvlist => "kvlist",
        }
    }
}

impl fmt::Display for ValueType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Default)]
pub enum AttributeValue {
    #[default]
    Null,
    String(String),
    Int(i64),
    Double(f64),
    Bool(bool),
    Bytes(Vec<u8>),
    Array(Vec<AttributeValue>),
    Kvlist(Vec<Attribute>),
}

impl AttributeValue {
    pub fn value_type(&self) -> ValueType {
        match self {
            Self::Null => ValueType::Null,
            Self::String(_) => ValueType::String,
            Self::Int(_) => ValueType::Int,
            Self::Double(_) => ValueType::Double,
            Self::Bool(_) => ValueType::Bool,
            Self::Bytes(_) => ValueType::Bytes,
            Self::Array(_) => ValueType::Array,
            Self::Kvlist(_) => ValueType::Kvlist,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::String(value) => Some(value),
            _ => None,
        }
    }

    pub fn as_int(&self) -> Option<i64> {
        match self {
            Self::Int(value) => Some(*value),
            _ => None,
        }
    }

    pub fn as_double(&self) -> Option<f64> {
        match self {
            Self::Double(value) => Some(*value),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Self::Bool(value) => Some(*value),
            _ => None,
        }
    }

    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Self::Bytes(value) => Some(value),
            _ => None,
        }
    }

    pub fn as_array(&self) -> Option<&[AttributeValue]> {
        match self {
            Self::Array(value) => Some(value),
            _ => None,
        }
    }

    pub fn as_kvlist(&self) -> Option<&[Attribute]> {
        match self {
            Self::Kvlist(value) => Some(value),
            _ => None,
        }
    }

    /// Canonical JSON rendering of this value: native JSON types, byte
    /// arrays hex-encoded, non-finite doubles as `null`, nested kvlists as
    /// sorted-key objects with duplicate keys collapsed to the last value.
    ///
    /// This is the single source of truth for how nested OTLP structures are
    /// represented in the JSON storage columns (structured bodies, nested
    /// attribute values). Serializing a JSON string into a `String` cannot
    /// fail.
    pub fn to_canonical_json(&self) -> String {
        serde_json::to_string(self)
            .expect("canonical JSON serialization of an attribute value cannot fail")
    }
}

impl From<String> for AttributeValue {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl From<&str> for AttributeValue {
    fn from(value: &str) -> Self {
        Self::String(value.to_owned())
    }
}

impl From<i64> for AttributeValue {
    fn from(value: i64) -> Self {
        Self::Int(value)
    }
}

impl From<f64> for AttributeValue {
    fn from(value: f64) -> Self {
        Self::Double(value)
    }
}

impl From<bool> for AttributeValue {
    fn from(value: bool) -> Self {
        Self::Bool(value)
    }
}

impl From<Vec<u8>> for AttributeValue {
    fn from(value: Vec<u8>) -> Self {
        Self::Bytes(value)
    }
}

impl From<Vec<AttributeValue>> for AttributeValue {
    fn from(value: Vec<AttributeValue>) -> Self {
        Self::Array(value)
    }
}

impl From<Vec<Attribute>> for AttributeValue {
    fn from(value: Vec<Attribute>) -> Self {
        Self::Kvlist(value)
    }
}

/// The canonical JSON encoding of an [`AttributeValue`]. Keys inside nested
/// kvlists are emitted ascending with duplicate keys collapsed to the
/// last-inserted value, byte arrays are hex-encoded, and non-finite doubles
/// become `null` — exactly the rules of the flat attribute object used by
/// the storage layer, so structured values persist identically wherever they
/// appear.
impl Serialize for AttributeValue {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Null => serializer.serialize_none(),
            Self::Int(value) => serializer.serialize_i64(*value),
            Self::Double(value) => Float(*value).serialize(serializer),
            Self::Bool(value) => serializer.serialize_bool(*value),
            Self::String(value) => serializer.serialize_str(value),
            Self::Bytes(value) => serializer.serialize_str(&hex_encode(value)),
            Self::Array(values) => {
                let mut seq = serializer.serialize_seq(Some(values.len()))?;
                for value in values {
                    seq.serialize_element(value)?;
                }
                seq.end()
            }
            Self::Kvlist(attributes) => {
                let mut order: Vec<usize> = (0..attributes.len()).collect();
                order.sort_unstable_by_key(|&index| (attributes[index].key.as_str(), index));
                let mut map = serializer.serialize_map(Some(order.len()))?;
                for (position, &index) in order.iter().enumerate() {
                    let followed_by_same_key = order
                        .get(position + 1)
                        .is_some_and(|&next| attributes[next].key == attributes[index].key);
                    if followed_by_same_key {
                        continue;
                    }
                    let attribute = &attributes[index];
                    map.serialize_entry(&attribute.key, &attribute.value)?;
                }
                map.end()
            }
        }
    }
}

/// An `f64` that serializes like `serde_json::Value::from(f64)` did:
/// non-finite values become `null` instead of failing the serializer.
struct Float(f64);

impl Serialize for Float {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if self.0.is_finite() {
            serializer.serialize_f64(self.0)
        } else {
            serializer.serialize_none()
        }
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    use std::fmt::Write as _;

    let mut hex = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(hex, "{byte:02x}");
    }
    hex
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct Attribute {
    pub key: String,
    pub value: AttributeValue,
}

impl<K: Into<String>, V: Into<AttributeValue>> From<(K, V)> for Attribute {
    fn from((key, value): (K, V)) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
        }
    }
}
