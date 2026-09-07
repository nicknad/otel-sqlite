//! Pipeline-level metric types: one record per metric, plus the batch shape
//! handed from ingress mapping to the storage insert batcher.

use super::aggregation::MetricData;
use crate::model::attribute::Attribute;
use crate::model::resource::Resource;

#[derive(Debug, Clone, PartialEq, Default)]
pub struct MetricRecord {
    pub name: String,
    pub description: String,
    pub unit: String,
    pub data: MetricData,
    pub metadata: Vec<Attribute>,
    pub resource_id: String,
    pub resource: Option<Resource>,
    pub scope_name: String,
    pub scope_version: String,
    pub scope_attributes: Vec<Attribute>,
    pub scope_schema_url: String,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct MetricBatch {
    pub records: Vec<MetricRecord>,
    pub resource: Option<Resource>,
    pub schema_url: String,
}

impl MetricBatch {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            records: Vec::with_capacity(capacity),
            ..Self::default()
        }
    }

    pub fn push(&mut self, record: MetricRecord) {
        self.records.push(record);
    }

    pub fn len(&self) -> usize {
        self.records.len()
    }

    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    pub fn data_point_count(&self) -> usize {
        self.records
            .iter()
            .map(|record| record.data.data_point_count())
            .sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::metric::aggregation::{Gauge, Summary};
    use crate::model::metric::data_point::{NumberDataPoint, SummaryDataPoint};

    #[test]
    fn batch_operations_and_point_counts() {
        let mut batch = MetricBatch::with_capacity(4);
        assert!(batch.is_empty());

        let mut gauge = Gauge::default();
        gauge.data_points.push(NumberDataPoint::default());
        gauge.data_points.push(NumberDataPoint::default());

        batch.push(MetricRecord {
            name: "cpu.usage".to_owned(),
            data: MetricData::Gauge(gauge),
            ..MetricRecord::default()
        });

        let mut summary = Summary::default();
        summary.data_points.push(SummaryDataPoint::default());
        batch.push(MetricRecord {
            name: "rpc.duration".to_owned(),
            data: MetricData::Summary(summary),
            ..MetricRecord::default()
        });

        assert_eq!(batch.len(), 2);
        assert_eq!(batch.data_point_count(), 3);

        batch.records.clear();
        assert!(batch.is_empty());
        assert_eq!(batch.data_point_count(), 0);
    }
}
