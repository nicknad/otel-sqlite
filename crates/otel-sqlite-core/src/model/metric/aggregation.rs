//! Metric aggregations: the kind-specific container of data points.
//!
//! [`MetricData`] mirrors the OTLP metric `data` oneof; every variant pairs
//! its data points with the aggregation temporality where OTLP defines one.

use std::fmt;

use super::data_point::{
    ExponentialHistogramDataPoint, HistogramDataPoint, NumberDataPoint, SummaryDataPoint,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[repr(u8)]
pub enum Temporality {
    #[default]
    Unspecified = 0,
    Delta = 1,
    Cumulative = 2,
}

impl Temporality {
    pub const fn from_i32(value: i32) -> Option<Self> {
        match value {
            0 => Some(Self::Unspecified),
            1 => Some(Self::Delta),
            2 => Some(Self::Cumulative),
            _ => None,
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Unspecified => "unspecified",
            Self::Delta => "delta",
            Self::Cumulative => "cumulative",
        }
    }
}

impl fmt::Display for Temporality {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct Gauge {
    pub data_points: Vec<NumberDataPoint>,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct Sum {
    pub data_points: Vec<NumberDataPoint>,
    pub aggregation_temporality: Temporality,
    pub is_monotonic: bool,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct Histogram {
    pub data_points: Vec<HistogramDataPoint>,
    pub aggregation_temporality: Temporality,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct ExponentialHistogram {
    pub data_points: Vec<ExponentialHistogramDataPoint>,
    pub aggregation_temporality: Temporality,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct Summary {
    pub data_points: Vec<SummaryDataPoint>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum MetricData {
    Gauge(Gauge),
    Sum(Sum),
    Histogram(Histogram),
    ExponentialHistogram(ExponentialHistogram),
    Summary(Summary),
}

impl Default for MetricData {
    fn default() -> Self {
        Self::Gauge(Gauge::default())
    }
}

impl MetricData {
    pub const fn data_point_count(&self) -> usize {
        match self {
            Self::Gauge(gauge) => gauge.data_points.len(),
            Self::Sum(sum) => sum.data_points.len(),
            Self::Histogram(histogram) => histogram.data_points.len(),
            Self::ExponentialHistogram(histogram) => histogram.data_points.len(),
            Self::Summary(summary) => summary.data_points.len(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn temporality_matches_otlp_numbers() {
        assert_eq!(Temporality::from_i32(0), Some(Temporality::Unspecified));
        assert_eq!(Temporality::from_i32(1), Some(Temporality::Delta));
        assert_eq!(Temporality::from_i32(2), Some(Temporality::Cumulative));
        assert_eq!(Temporality::from_i32(3), None);
        assert_eq!(Temporality::Delta.to_string(), "delta");
        assert_eq!(Temporality::Cumulative.as_str(), "cumulative");
        assert_eq!(Temporality::default() as u8, 0);
    }
}
