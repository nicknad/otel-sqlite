use std::io;
use std::str;

use serde::ser::{SerializeMap, SerializeSeq};
use serde::{Serialize, Serializer};

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

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Self::Bool(value) => Some(*value),
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

/// Writes `attributes` as the canonical flat JSON object into `out` (cleared
/// first), reusing `order` as key-ordering scratch.
///
/// Canonical form mirrors `BTreeMap` iteration: keys ascending, and among
/// equal keys only the last-inserted value survives. Byte arrays are
/// hex-encoded and values serialize through [`AttributeValue`]'s own
/// `Serialize` implementation, so this is the single source of truth for the
/// JSON attribute columns and for precomputed scope metadata alike.
///
/// This encoding is part of the storage contract and must stay stable:
/// resource dimension IDs are SHA-256 fingerprints over these bytes, and the
/// persisted JSON columns are expected to remain byte-identical across
/// versions.
///
/// Serializing JSON into a `String` cannot fail.
pub fn write_attributes_json_into(
    attributes: &[Attribute],
    order: &mut Vec<u32>,
    out: &mut String,
) {
    order.clear();
    order.extend(0..attributes.len() as u32);
    order.sort_unstable_by_key(|&index| (&attributes[index as usize].key, index));

    write_json(&AttributesJson { attributes, order }, out);
}

/// Writes `document`'s compact JSON into `out` (cleared first), reusing the
/// buffer instead of allocating a fresh `String` per document.
fn write_json<T: Serialize + ?Sized>(document: &T, out: &mut String) {
    out.clear();
    serde_json::to_writer(StringWriter(out), document)
        .expect("serializing JSON into a String buffer cannot fail");
}

/// Adapts `&mut String` to `io::Write` for [`write_json`].
///
/// serde_json only emits valid UTF-8 chunks (its string escaper handles
/// everything outside plain text), so appending each chunk verbatim keeps
/// the buffer valid UTF-8.
struct StringWriter<'a>(&'a mut String);

impl io::Write for StringWriter<'_> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let chunk = str::from_utf8(buf)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        self.0.push_str(chunk);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Canonical attribute object: sorted keys, duplicates collapsed to the
/// last-inserted value. Each value serializes through `AttributeValue`'s
/// own `Serialize` implementation (see above), so nested structures use the
/// same canonical rules as structured bodies.
struct AttributesJson<'a> {
    attributes: &'a [Attribute],
    order: &'a [u32],
}

impl Serialize for AttributesJson<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(None)?;
        for (position, &index) in self.order.iter().enumerate() {
            // Sorted by (key, insertion index): equal keys sit adjacent in
            // insertion order, so skipping every entry that another equal
            // key follows keeps exactly the last-inserted one.
            let followed_by_same_key = self.order.get(position + 1).is_some_and(|&next| {
                self.attributes[next as usize].key == self.attributes[index as usize].key
            });
            if followed_by_same_key {
                continue;
            }
            let attribute = &self.attributes[index as usize];
            map.serialize_entry(&attribute.key, &attribute.value)?;
        }
        map.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn attr(key: &str, value: AttributeValue) -> Attribute {
        Attribute {
            key: key.to_owned(),
            value,
        }
    }

    fn encode(attributes: &[Attribute]) -> String {
        let mut order = Vec::new();
        let mut out = String::new();
        write_attributes_json_into(attributes, &mut order, &mut out);
        out
    }

    #[test]
    fn empty_attributes_render_an_empty_object() {
        assert_eq!(encode(&[]), "{}");
    }

    #[test]
    fn keys_sort_and_duplicates_keep_the_last_value() {
        let attributes = vec![
            attr("b", AttributeValue::Int(1)),
            attr("a", AttributeValue::Int(2)),
            attr("b", AttributeValue::Int(3)),
        ];
        assert_eq!(encode(&attributes), r#"{"a":2,"b":3}"#);
    }

    #[test]
    fn value_kinds_match_the_historical_value_tree_output() {
        let attributes = vec![
            attr("bytes", AttributeValue::Bytes(vec![0xde, 0xad])),
            attr("double", AttributeValue::Double(1.5)),
            attr("flag", AttributeValue::Bool(true)),
            attr("nan", AttributeValue::Double(f64::NAN)),
            attr("neg", AttributeValue::Int(-7)),
            attr("nil", AttributeValue::Null),
            attr("text", AttributeValue::String("he\"llo\u{7}".to_owned())),
        ];
        assert_eq!(
            encode(&attributes),
            concat!(
                r#"{"bytes":"dead","#,
                r#""double":1.5,"#,
                r#""flag":true,"#,
                r#""nan":null,"#,
                r#""neg":-7,"#,
                r#""nil":null,"#,
                r#""text":"he\"llo\u0007"}"#
            )
        );
    }

    #[test]
    fn nested_attributes_match_the_canonical_value_json() {
        // Nested structures inside the flat attribute object must serialize
        // through the same rules as AttributeValue::to_canonical_json (used by
        // ingress for structured bodies), so both paths persist byte-identical
        // documents.
        let attributes = vec![
            attr(
                "arr",
                AttributeValue::Array(vec![
                    AttributeValue::Bool(true),
                    AttributeValue::Double(1.5),
                    AttributeValue::Bytes(vec![0xde, 0xad]),
                    AttributeValue::Kvlist(vec![
                        attr("z", AttributeValue::Int(1)),
                        attr("a", AttributeValue::String("x".to_owned())),
                    ]),
                ]),
            ),
            attr(
                "kv",
                AttributeValue::Kvlist(vec![
                    attr("b", AttributeValue::Int(1)),
                    attr("a", AttributeValue::Int(2)),
                    attr("b", AttributeValue::Int(3)),
                    attr("nan", AttributeValue::Double(f64::NAN)),
                ]),
            ),
        ];

        let encoded = encode(&attributes);
        assert_eq!(
            encoded,
            concat!(
                r#"{"arr":[true,1.5,"dead",{"a":"x","z":1}],"#,
                r#""kv":{"a":2,"b":3,"nan":null}}"#
            )
        );

        // Cross-check against the core rendering for the same nested values.
        let kv = AttributeValue::Kvlist(vec![
            attr("b", AttributeValue::Int(1)),
            attr("a", AttributeValue::Int(2)),
            attr("b", AttributeValue::Int(3)),
            attr("nan", AttributeValue::Double(f64::NAN)),
        ]);
        assert_eq!(kv.to_canonical_json(), r#"{"a":2,"b":3,"nan":null}"#);
    }
}
