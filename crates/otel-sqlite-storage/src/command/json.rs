//! Attribute serialization for the JSON columns.
//!
//! Attributes are stored as a flat JSON object keyed by attribute name.
//! Byte-array values are hex-encoded because JSON has no binary type; the
//! encoding is part of the storage contract and must stay stable: keys are
//! emitted in ascending order and duplicate keys collapse to the last value —
//! exactly the output of the historical `serde_json::Map` (BTreeMap-backed)
//! implementation.
//!
//! Scalar and nested attribute values serialize through
//! [`AttributeValue`]'s own `Serialize` implementation in
//! `otel-sqlite-core`, so a structured body persisted by ingress and a
//! nested attribute value persisted here are guaranteed to be byte-identical.
//!
//! Everything streams straight into a caller-owned reusable [`String`]
//! scratch buffer via `serde_json::to_writer`. The former `Value`-tree path
//! cloned every key and string payload on every record.

use std::io;
use std::str;

use otel_sqlite_core::model::Attribute;
use serde::ser::SerializeMap;
use serde::{Serialize, Serializer};

use super::InsertScratch;

/// Writes `attributes` as the canonical flat JSON object into `scratch.json`
/// (cleared first), reusing the output buffer and the key-order scratch.
pub(super) fn write_attributes_json(attributes: &[Attribute], scratch: &mut InsertScratch) {
    write_attributes_json_into(attributes, &mut scratch.order, &mut scratch.json);
}

/// [`write_attributes_json`] with an explicit output buffer, for the two JSON
/// documents a single row may carry (record attributes + scope attributes).
/// The `order` scratch is reused sequentially by the caller between calls.
pub(super) fn write_attributes_json_into(
    attributes: &[Attribute],
    order: &mut Vec<u32>,
    out: &mut String,
) {
    // Canonical form mirrors BTreeMap iteration: keys ascending, and among
    // equal keys only the last-inserted value survives.
    order.clear();
    order.extend(0..attributes.len() as u32);
    order.sort_unstable_by_key(|&index| (&attributes[index as usize].key, index));

    write_json(&AttributesJson { attributes, order }, out);
}

/// Writes `document`'s compact JSON into `out` (cleared first), reusing the
/// buffer instead of allocating a fresh `String` per row.
pub(super) fn write_json<T: Serialize + ?Sized>(document: &T, out: &mut String) {
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
/// own `Serialize` implementation (core), so nested structures use the same
/// canonical rules as structured bodies.
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
    use otel_sqlite_core::model::{Attribute, AttributeValue};

    fn attr(key: &str, value: AttributeValue) -> Attribute {
        Attribute {
            key: key.to_owned(),
            value,
        }
    }

    fn encode(attributes: &[Attribute]) -> String {
        let mut scratch = InsertScratch::new();
        write_attributes_json(attributes, &mut scratch);
        scratch.json.clone()
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
