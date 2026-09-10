//! Attribute and metric-payload serialization for the JSON columns.
//!
//! Attributes are stored as a flat JSON object keyed by attribute name;
//! metric payloads (histogram buckets, summary quantiles, ...) as small
//! JSON documents. Byte-array values are hex-encoded because JSON has no
//! binary type; the encoding is part of the storage contract and must stay
//! stable: keys are emitted in ascending order and duplicate keys collapse
//! to the last value — exactly the output of the historical
//! `serde_json::Map` (BTreeMap-backed) implementation.
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

use otel_sqlite_core::model::{Attribute, Exemplar, ExponentialBucket, NumberValue, QuantileValue};
use serde::ser::{SerializeMap, SerializeSeq};
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

/// Histogram payload: `{"bounds":[...],"counts":[...]}`.
pub(super) struct HistogramDoc<'a> {
    pub(super) bounds: &'a [f64],
    pub(super) counts: &'a [u64],
}

impl Serialize for HistogramDoc<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(2))?;
        map.serialize_entry("bounds", &Floats(self.bounds))?;
        map.serialize_entry("counts", &self.counts)?;
        map.end()
    }
}

/// Exponential histogram payload. Fields are serialized in canonical sorted
/// key order: negative, positive, scale, zero_count, zero_threshold.
pub(super) struct ExponentialDoc<'a> {
    pub(super) negative: Option<&'a ExponentialBucket>,
    pub(super) positive: Option<&'a ExponentialBucket>,
    pub(super) scale: i32,
    pub(super) zero_count: u64,
    pub(super) zero_threshold: f64,
}

impl Serialize for ExponentialDoc<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(5))?;
        map.serialize_entry("negative", &self.negative.map(BucketDoc))?;
        map.serialize_entry("positive", &self.positive.map(BucketDoc))?;
        map.serialize_entry("scale", &self.scale)?;
        map.serialize_entry("zero_count", &self.zero_count)?;
        map.serialize_entry("zero_threshold", &Float(self.zero_threshold))?;
        map.end()
    }
}

/// `{"bucket_counts":[...],"offset":n}`.
struct BucketDoc<'a>(&'a ExponentialBucket);

impl Serialize for BucketDoc<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(2))?;
        map.serialize_entry("bucket_counts", &self.0.bucket_counts)?;
        map.serialize_entry("offset", &self.0.offset)?;
        map.end()
    }
}

/// Summary payload:
/// `{"count":n,"quantile_values":[{"quantile":q,"value":v},...],"sum":x}`.
pub(super) struct SummaryDoc<'a> {
    pub(super) count: u64,
    pub(super) quantile_values: &'a [QuantileValue],
    pub(super) sum: f64,
}

impl Serialize for SummaryDoc<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(3))?;
        map.serialize_entry("count", &self.count)?;
        map.serialize_entry("quantile_values", &Quantiles(self.quantile_values))?;
        map.serialize_entry("sum", &Float(self.sum))?;
        map.end()
    }
}

struct Quantiles<'a>(&'a [QuantileValue]);

impl Serialize for Quantiles<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut seq = serializer.serialize_seq(Some(self.0.len()))?;
        for quantile in self.0 {
            seq.serialize_element(&QuantileDoc {
                quantile: quantile.quantile,
                value: quantile.value,
            })?;
        }
        seq.end()
    }
}

/// `{"quantile":q,"value":v}`.
struct QuantileDoc {
    quantile: f64,
    value: f64,
}

impl Serialize for QuantileDoc {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(2))?;
        map.serialize_entry("quantile", &Float(self.quantile))?;
        map.serialize_entry("value", &Float(self.value))?;
        map.end()
    }
}

/// Exemplar payload: a JSON array of exemplar objects. Each object uses
/// sorted ascending keys (`filtered_attributes`, `span_id`, `time_unix_nano`,
/// `trace_id`, `value`); absent trace/span ids are omitted, the value is a
/// native number (`null` for non-finite doubles or an absent oneof), and
/// filtered attributes are a flat canonical attribute object.
pub(super) struct ExemplarsDoc<'a>(pub(super) &'a [Exemplar]);

impl Serialize for ExemplarsDoc<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut seq = serializer.serialize_seq(Some(self.0.len()))?;
        for exemplar in self.0 {
            let order = exemplar_filter_order(&exemplar.filtered_attributes);
            seq.serialize_element(&ExemplarDoc {
                exemplar,
                order: &order,
            })?;
        }
        seq.end()
    }
}

struct ExemplarDoc<'a> {
    exemplar: &'a Exemplar,
    order: &'a [u32],
}

impl Serialize for ExemplarDoc<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let exemplar = self.exemplar;
        // Fixed order is already the sorted-key order for this field set.
        let mut map = serializer.serialize_map(None)?;
        map.serialize_entry(
            "filtered_attributes",
            &AttributesJson {
                attributes: &exemplar.filtered_attributes,
                order: self.order,
            },
        )?;
        if let Some(span_id) = &exemplar.span_id {
            map.serialize_entry("span_id", &hex::encode(span_id))?;
        }
        map.serialize_entry("time_unix_nano", &exemplar.time_unix_nano)?;
        if let Some(trace_id) = &exemplar.trace_id {
            map.serialize_entry("trace_id", &hex::encode(trace_id))?;
        }
        match &exemplar.value {
            Some(NumberValue::Int(value)) => map.serialize_entry("value", value)?,
            Some(NumberValue::Double(value)) => {
                map.serialize_entry("value", &Float(*value))?;
            }
            None => map.serialize_entry("value", &Option::<f64>::None)?,
        }
        map.end()
    }
}

/// Sorted insertion-order indices for the exemplar's filtered attributes,
/// mirroring [`InsertScratch::order`] semantics for the canonical object
/// encoding. Exemplars are not on the record-per-row hot path, so a small
/// allocation here is acceptable.
fn exemplar_filter_order(attributes: &[Attribute]) -> Vec<u32> {
    let mut order: Vec<u32> = (0..attributes.len() as u32).collect();
    order.sort_unstable_by_key(|&index| (attributes[index as usize].key.as_str(), index));
    order
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

/// A slice of doubles with [`Float`] semantics per element.
struct Floats<'a>(&'a [f64]);

impl Serialize for Floats<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut seq = serializer.serialize_seq(Some(self.0.len()))?;
        for value in self.0 {
            seq.serialize_element(&Float(*value))?;
        }
        seq.end()
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
    fn payload_documents_match_the_previous_json_macro_output() {
        let mut out = String::new();

        write_json(
            &HistogramDoc {
                bounds: &[0.5, 1.5],
                counts: &[1, 2, 1],
            },
            &mut out,
        );
        assert_eq!(out, r#"{"bounds":[0.5,1.5],"counts":[1,2,1]}"#);

        write_json(
            &ExponentialDoc {
                negative: None,
                positive: Some(&ExponentialBucket {
                    offset: 3,
                    bucket_counts: vec![1, 1],
                }),
                scale: -2,
                zero_count: 1,
                zero_threshold: 0.001,
            },
            &mut out,
        );
        assert_eq!(
            out,
            r#"{"negative":null,"positive":{"bucket_counts":[1,1],"offset":3},"scale":-2,"zero_count":1,"zero_threshold":0.001}"#
        );

        write_json(
            &SummaryDoc {
                count: 10,
                quantile_values: &[
                    QuantileValue {
                        quantile: 0.5,
                        value: 5.0,
                    },
                    QuantileValue {
                        quantile: 0.99,
                        value: 9.9,
                    },
                ],
                sum: 55.0,
            },
            &mut out,
        );
        assert_eq!(
            out,
            r#"{"count":10,"quantile_values":[{"quantile":0.5,"value":5.0},{"quantile":0.99,"value":9.9}],"sum":55.0}"#
        );

        // Non-finite doubles keep the Value-tree behaviour of becoming null.
        write_json(
            &HistogramDoc {
                bounds: &[f64::NAN, f64::INFINITY],
                counts: &[],
            },
            &mut out,
        );
        assert_eq!(out, r#"{"bounds":[null,null],"counts":[]}"#);
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

    #[test]
    fn exemplars_document_is_canonical() {
        let exemplars = [
            Exemplar {
                filtered_attributes: vec![attr("z", AttributeValue::Bool(true))],
                time_unix_nano: 150,
                value: Some(NumberValue::Double(0.5)),
                trace_id: Some([8; 16]),
                span_id: Some([9; 8]),
            },
            // Absent ids and int values render as omitted fields / an integer.
            Exemplar {
                filtered_attributes: vec![],
                time_unix_nano: 151,
                value: Some(NumberValue::Int(7)),
                trace_id: None,
                span_id: None,
            },
            // Non-finite doubles become null, like every other JSON column.
            Exemplar {
                filtered_attributes: vec![],
                time_unix_nano: 152,
                value: Some(NumberValue::Double(f64::NAN)),
                trace_id: None,
                span_id: None,
            },
        ];
        let mut out = String::new();
        write_json(&ExemplarsDoc(&exemplars), &mut out);
        assert_eq!(
            out,
            concat!(
                r#"[{"filtered_attributes":{"z":true},"#,
                r#""span_id":"0909090909090909","#,
                r#""time_unix_nano":150,"#,
                r#""trace_id":"08080808080808080808080808080808","#,
                r#""value":0.5},"#,
                r#"{"filtered_attributes":{},"time_unix_nano":151,"value":7},"#,
                r#"{"filtered_attributes":{},"time_unix_nano":152,"value":null}]"#
            )
        );
    }
}
