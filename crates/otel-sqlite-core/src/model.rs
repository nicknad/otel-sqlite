pub mod attribute;
pub mod log;
pub mod metric;
pub mod resource;
pub mod severity;

pub use attribute::{Attribute, AttributeValue, ValueType};
pub use log::{LogBatch, LogRecord};
pub use metric::{
    Exemplar, ExponentialBucket, ExponentialHistogram, ExponentialHistogramDataPoint, Gauge,
    Histogram, HistogramDataPoint, MetricBatch, MetricData, MetricRecord, NumberDataPoint,
    NumberValue, QuantileValue, Sum, Summary, SummaryDataPoint, Temporality,
};
pub use resource::Resource;
pub use severity::Severity;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn severity_display_matches_base_levels() {
        assert_eq!(Severity::Unspecified.as_str(), "UNSPECIFIED");
        assert_eq!(Severity::Trace.to_string(), "TRACE");
        assert_eq!(Severity::Debug.to_string(), "DEBUG");
        assert_eq!(Severity::Info.to_string(), "INFO");
        assert_eq!(Severity::Warn.to_string(), "WARN");
        assert_eq!(Severity::Error.to_string(), "ERROR");
        assert_eq!(Severity::Fatal.to_string(), "FATAL");
    }

    #[test]
    fn severity_discriminants_match_otlp_numbers() {
        assert_eq!(Severity::Unspecified as u8, 0);
        assert_eq!(Severity::Trace as u8, 1);
        assert_eq!(Severity::Debug as u8, 5);
        assert_eq!(Severity::Info as u8, 9);
        assert_eq!(Severity::Warn as u8, 13);
        assert_eq!(Severity::Error as u8, 17);
        assert_eq!(Severity::Fatal as u8, 21);
        assert_eq!(Severity::Fatal4 as u8, 24);
    }

    #[test]
    fn attribute_value_types_and_accessors() {
        let value = AttributeValue::from(42_i64);
        assert_eq!(value.value_type(), ValueType::Int);
        assert_eq!(ValueType::Int.as_str(), "int");
        assert_eq!(value.as_int(), Some(42));
        assert_eq!(value.as_str(), None);

        let text = AttributeValue::from("api");
        assert_eq!(text.value_type(), ValueType::String);
        assert_eq!(ValueType::String.as_str(), "string");
        assert_eq!(text.as_str(), Some("api"));

        assert_eq!(AttributeValue::default().value_type(), ValueType::Null);
    }

    #[test]
    fn resource_attribute_lookup() {
        let resource = Resource::new(vec![
            ("service.name", "checkout").into(),
            ("host.name", "node-1").into(),
            ("retries", 3_i64).into(),
        ]);

        assert_eq!(resource.service_name(), "checkout");
        assert_eq!(resource.host_name(), "node-1");
        assert_eq!(
            resource.get("retries").and_then(AttributeValue::as_int),
            Some(3)
        );
        assert_eq!(Resource::default().service_name(), "");
    }

    #[test]
    fn record_trace_context_presence() {
        let mut record = LogRecord {
            time_unix_nano: 1_000,
            observed_time_unix_nano: -2_000,
            ..LogRecord::default()
        };
        assert!(!record.has_trace_context());

        record.trace_id = [7; 16];
        assert!(record.has_trace());
        assert!(!record.has_trace_context());

        record.span_id = [9; 8];
        assert!(record.has_trace_context());
    }

    #[test]
    fn batch_operations() {
        let mut batch = LogBatch::with_capacity(4);
        assert!(batch.is_empty());
        assert_eq!(batch.len(), 0);

        batch.push(LogRecord {
            severity_number: Severity::Error,
            ..LogRecord::default()
        });
        assert_eq!(batch.len(), 1);

        batch.clear();
        assert!(batch.is_empty());
    }
}
