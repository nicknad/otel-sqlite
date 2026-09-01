//! Metric signal model.
//!
//! Split by concern:
//!
//! * `data_point` - timestamped observations and their values,
//! * `aggregation` - the kind-specific containers ([`Gauge`], [`Sum`],
//!   [`Histogram`], [`ExponentialHistogram`], [`Summary`]) plus
//!   [`Temporality`](aggregation::Temporality),
//! * `record` - pipeline-level [`MetricRecord`]/[`MetricBatch`] handed from
//!   ingress mapping to the storage insert batcher.

mod aggregation;
mod data_point;
mod record;

pub use aggregation::{
    ExponentialHistogram, Gauge, Histogram, MetricData, Sum, Summary, Temporality,
};
pub use data_point::{
    Exemplar, ExponentialBucket, ExponentialHistogramDataPoint, HistogramDataPoint,
    NumberDataPoint, NumberValue, QuantileValue, SummaryDataPoint,
};
pub use record::{MetricBatch, MetricRecord};
