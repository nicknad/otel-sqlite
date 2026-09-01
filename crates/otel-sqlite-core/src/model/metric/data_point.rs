//! Data-point types shared by all metric kinds.
//!
//! A data point is one timestamped observation plus its attribute set. The
//! value is either an integer or a double ([`NumberValue`], mirroring the
//! OTLP oneof); histogram payloads carry their buckets inline.

use crate::model::attribute::Attribute;

#[derive(Debug, Clone, PartialEq)]
pub enum NumberValue {
    Int(i64),
    Double(f64),
}

impl Default for NumberValue {
    fn default() -> Self {
        Self::Int(0)
    }
}

impl NumberValue {
    pub const fn as_int(&self) -> Option<i64> {
        match self {
            Self::Int(value) => Some(*value),
            Self::Double(_) => None,
        }
    }

    pub const fn as_double(&self) -> Option<f64> {
        match self {
            Self::Int(_) => None,
            Self::Double(value) => Some(*value),
        }
    }
}

impl From<i64> for NumberValue {
    fn from(value: i64) -> Self {
        Self::Int(value)
    }
}

impl From<f64> for NumberValue {
    fn from(value: f64) -> Self {
        Self::Double(value)
    }
}

/// A sample input measurement recorded alongside an aggregation data point.
///
/// Mirrors the OTLP `Exemplar` message: the filtered attributes, the
/// measurement time, the sampled value (int or double), and the optional
/// trace context. Absent trace/span ids are `None`; present ids are always
/// the exact 16/8-byte forms (malformed ids are rejected at mapping).
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Exemplar {
    pub filtered_attributes: Vec<Attribute>,
    pub time_unix_nano: i64,
    pub value: Option<NumberValue>,
    pub trace_id: Option<[u8; 16]>,
    pub span_id: Option<[u8; 8]>,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct NumberDataPoint {
    pub attributes: Vec<Attribute>,
    pub start_time_unix_nano: i64,
    pub time_unix_nano: i64,
    pub value: Option<NumberValue>,
    pub flags: u32,
    pub exemplars: Vec<Exemplar>,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct HistogramDataPoint {
    pub attributes: Vec<Attribute>,
    pub start_time_unix_nano: i64,
    pub time_unix_nano: i64,
    pub count: u64,
    pub sum: Option<f64>,
    pub bucket_counts: Vec<u64>,
    pub explicit_bounds: Vec<f64>,
    pub min: Option<f64>,
    pub max: Option<f64>,
    pub flags: u32,
    pub exemplars: Vec<Exemplar>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ExponentialBucket {
    pub offset: i32,
    pub bucket_counts: Vec<u64>,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct ExponentialHistogramDataPoint {
    pub attributes: Vec<Attribute>,
    pub start_time_unix_nano: i64,
    pub time_unix_nano: i64,
    pub count: u64,
    pub sum: Option<f64>,
    pub scale: i32,
    pub zero_count: u64,
    pub zero_threshold: f64,
    pub positive: Option<ExponentialBucket>,
    pub negative: Option<ExponentialBucket>,
    pub min: Option<f64>,
    pub max: Option<f64>,
    pub flags: u32,
    pub exemplars: Vec<Exemplar>,
}

#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct QuantileValue {
    pub quantile: f64,
    pub value: f64,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct SummaryDataPoint {
    pub attributes: Vec<Attribute>,
    pub start_time_unix_nano: i64,
    pub time_unix_nano: i64,
    pub count: u64,
    pub sum: f64,
    pub quantile_values: Vec<QuantileValue>,
    pub flags: u32,
    pub exemplars: Vec<Exemplar>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn number_value_accessors() {
        let int = NumberValue::from(42_i64);
        assert_eq!(int.as_int(), Some(42));
        assert_eq!(int.as_double(), None);

        let double = NumberValue::from(1.5_f64);
        assert_eq!(double.as_double(), Some(1.5));
        assert_eq!(double.as_int(), None);

        assert_eq!(NumberValue::default(), NumberValue::Int(0));
    }
}
