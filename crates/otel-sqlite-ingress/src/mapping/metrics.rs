use otel_sqlite_core::model::{
    Attribute, Exemplar, ExponentialBucket, ExponentialHistogram, ExponentialHistogramDataPoint,
    Gauge, Histogram, HistogramDataPoint, MetricBatch, MetricData, MetricRecord, NumberDataPoint,
    NumberValue, QuantileValue, Sum, Summary, SummaryDataPoint, Temporality,
};
use otel_sqlite_core::storage::{BatchOrigin, IngestMessage, MetricChunk};

use crate::error::IngressError;

use super::pb::common::v1::InstrumentationScope;
use super::pb::metrics::v1 as proto;
use super::pb::metrics::v1::{ResourceMetrics, ScopeMetrics};
use super::{Resource, attributes, parse_span_id, parse_trace_id};

/// Fallible proto → model conversion of one `ResourceMetrics` group.
///
/// Preserves exemplars, metric metadata, scope attributes and schema URLs;
/// malformed exemplar trace/span ids reject the whole request instead of
/// being silently dropped (all-or-nothing, matching P0-1).
pub fn try_convert_batch(value: ResourceMetrics) -> Result<MetricBatch, IngressError> {
    let ResourceMetrics {
        resource,
        scope_metrics,
        schema_url,
    } = value;

    let capacity: usize = scope_metrics
        .iter()
        .map(|scope| scope.metrics.iter().map(metric_point_count).sum::<usize>())
        .sum();
    let mut batch = MetricBatch::with_capacity(capacity);

    let mut resource = resource.map(Resource::from);
    if let Some(resource) = &mut resource {
        resource.schema_url.clone_from(&schema_url);
    }
    batch.resource = resource;

    for scope in scope_metrics {
        let ScopeMetrics {
            scope,
            metrics,
            schema_url: scope_schema_url,
        } = scope;
        let (scope_name, scope_version, scope_attributes) = scope
            .map(
                |InstrumentationScope {
                     name,
                     version,
                     attributes,
                     ..
                 }| (name, version, attributes),
            )
            .unwrap_or_default();
        let scope_attributes = attributes(scope_attributes);
        for metric in metrics {
            if let Some(record) = convert_metric(
                metric,
                &scope_name,
                &scope_version,
                &scope_attributes,
                &scope_schema_url,
            )? {
                batch.push(record);
            }
        }
    }

    batch.schema_url = schema_url;
    Ok(batch)
}

pub(crate) fn count(resource_metrics: &[ResourceMetrics]) -> usize {
    resource_metrics
        .iter()
        .flat_map(|resource| resource.scope_metrics.iter())
        .flat_map(|scope| scope.metrics.iter())
        .map(metric_point_count)
        .sum()
}

/// Maps an OTLP metrics export request into mapped metric chunks, one per
/// `ResourceMetrics` group. Grouping/splitting policy belongs to the storage
/// insert batcher; records move into chunks by ownership transfer. Mapping is
/// fallible: a malformed exemplar trace/span id fails the whole request
/// before anything is enqueued.
pub(crate) fn map_chunks(
    resource_metrics: Vec<ResourceMetrics>,
) -> Result<Vec<(u64, IngestMessage)>, IngressError> {
    let mut chunks = Vec::with_capacity(resource_metrics.len());
    for item in resource_metrics {
        let MetricBatch {
            records,
            resource,
            schema_url,
            ..
        } = try_convert_batch(item)?;
        if records.is_empty() {
            continue;
        }
        let points: u64 = records
            .iter()
            .map(|record| record.data.data_point_count() as u64)
            .sum();
        if points == 0 {
            continue;
        }
        chunks.push((
            points,
            IngestMessage::Metrics(MetricChunk {
                origin: BatchOrigin {
                    resource,
                    schema_url,
                },
                records,
                // No durability ledger exists yet; chunks start unstamped
                // and are stamped later once ticketing lands.
                commit_seq: 0,
            }),
        ));
    }
    Ok(chunks)
}

fn convert_metric(
    value: proto::Metric,
    scope_name: &str,
    scope_version: &str,
    scope_attributes: &[Attribute],
    scope_schema_url: &str,
) -> Result<Option<MetricRecord>, IngressError> {
    let proto::Metric {
        name,
        description,
        unit,
        metadata,
        data,
    } = value;

    match data {
        Some(data) => Ok(Some(MetricRecord {
            name,
            description,
            unit,
            metadata: attributes(metadata),
            data: convert_data(data)?,
            resource_id: String::new(),
            resource: None,
            scope_name: scope_name.to_owned(),
            scope_version: scope_version.to_owned(),
            scope_attributes: scope_attributes.to_vec(),
            scope_schema_url: scope_schema_url.to_owned(),
        })),
        None => Ok(None),
    }
}

fn metric_point_count(value: &proto::Metric) -> usize {
    match &value.data {
        Some(proto::metric::Data::Gauge(gauge)) => gauge.data_points.len(),
        Some(proto::metric::Data::Sum(sum)) => sum.data_points.len(),
        Some(proto::metric::Data::Histogram(histogram)) => histogram.data_points.len(),
        Some(proto::metric::Data::ExponentialHistogram(histogram)) => histogram.data_points.len(),
        Some(proto::metric::Data::Summary(summary)) => summary.data_points.len(),
        None => 0,
    }
}

fn convert_data(value: proto::metric::Data) -> Result<MetricData, IngressError> {
    use proto::metric::Data;
    Ok(match value {
        Data::Gauge(gauge) => MetricData::Gauge(Gauge {
            data_points: gauge
                .data_points
                .into_iter()
                .map(convert_number_point)
                .collect::<Result<Vec<_>, _>>()?,
        }),
        Data::Sum(sum) => MetricData::Sum(Sum {
            data_points: sum
                .data_points
                .into_iter()
                .map(convert_number_point)
                .collect::<Result<Vec<_>, _>>()?,
            aggregation_temporality: temporality(sum.aggregation_temporality),
            is_monotonic: sum.is_monotonic,
        }),
        Data::Histogram(histogram) => MetricData::Histogram(Histogram {
            data_points: histogram
                .data_points
                .into_iter()
                .map(convert_histogram_point)
                .collect::<Result<Vec<_>, _>>()?,
            aggregation_temporality: temporality(histogram.aggregation_temporality),
        }),
        Data::ExponentialHistogram(histogram) => {
            MetricData::ExponentialHistogram(ExponentialHistogram {
                data_points: histogram
                    .data_points
                    .into_iter()
                    .map(convert_exponential_histogram_point)
                    .collect::<Result<Vec<_>, _>>()?,
                aggregation_temporality: temporality(histogram.aggregation_temporality),
            })
        }
        Data::Summary(summary) => MetricData::Summary(Summary {
            data_points: summary
                .data_points
                .into_iter()
                .map(convert_summary_point)
                .collect(),
        }),
    })
}

fn temporality(value: i32) -> Temporality {
    if let Some(temporality) = Temporality::from_i32(value) {
        temporality
    } else {
        // Forward-compatible unknown enum values are counted as an
        // explicit loss instead of being silently downgraded.
        ::metrics::counter!(
            "otlp_mapping_loss_total",
            "signal" => "metrics",
            "reason" => "unknown_temporality"
        )
        .increment(1);
        Temporality::Unspecified
    }
}

/// Converts OTLP exemplars, rejecting malformed trace/span ids rather than
/// silently dropping the trace context of a sampled measurement.
fn exemplars(values: Vec<proto::Exemplar>) -> Result<Vec<Exemplar>, IngressError> {
    let mut exemplars = Vec::with_capacity(values.len());
    for exemplar in values {
        let proto::Exemplar {
            filtered_attributes,
            time_unix_nano,
            value,
            span_id,
            trace_id,
        } = exemplar;
        let trace_id = parse_trace_id(trace_id).map_err(|error| {
            IngressError::Mapping(format!("invalid exemplar trace id: {error}"))
        })?;
        let span_id = parse_span_id(span_id)
            .map_err(|error| IngressError::Mapping(format!("invalid exemplar span id: {error}")))?;
        exemplars.push(Exemplar {
            filtered_attributes: attributes(filtered_attributes),
            time_unix_nano: time_unix_nano as i64,
            value: match value {
                Some(proto::exemplar::Value::AsDouble(value)) => Some(NumberValue::Double(value)),
                Some(proto::exemplar::Value::AsInt(value)) => Some(NumberValue::Int(value)),
                None => None,
            },
            trace_id,
            span_id,
        });
    }
    Ok(exemplars)
}

fn convert_number_point(value: proto::NumberDataPoint) -> Result<NumberDataPoint, IngressError> {
    let proto::NumberDataPoint {
        attributes: point_attributes,
        start_time_unix_nano,
        time_unix_nano,
        value,
        exemplars: point_exemplars,
        flags,
    } = value;

    Ok(NumberDataPoint {
        attributes: attributes(point_attributes),
        start_time_unix_nano: start_time_unix_nano as i64,
        time_unix_nano: time_unix_nano as i64,
        value: value.map(number_value),
        flags,
        exemplars: exemplars(point_exemplars)?,
    })
}

fn number_value(value: proto::number_data_point::Value) -> NumberValue {
    match value {
        proto::number_data_point::Value::AsDouble(value) => NumberValue::Double(value),
        proto::number_data_point::Value::AsInt(value) => NumberValue::Int(value),
    }
}

fn convert_histogram_point(
    value: proto::HistogramDataPoint,
) -> Result<HistogramDataPoint, IngressError> {
    let proto::HistogramDataPoint {
        attributes: point_attributes,
        start_time_unix_nano,
        time_unix_nano,
        count,
        sum,
        bucket_counts,
        explicit_bounds,
        exemplars: point_exemplars,
        flags,
        min,
        max,
    } = value;

    Ok(HistogramDataPoint {
        attributes: attributes(point_attributes),
        start_time_unix_nano: start_time_unix_nano as i64,
        time_unix_nano: time_unix_nano as i64,
        count,
        sum,
        bucket_counts,
        explicit_bounds,
        min,
        max,
        flags,
        exemplars: exemplars(point_exemplars)?,
    })
}

fn convert_exponential_histogram_point(
    value: proto::ExponentialHistogramDataPoint,
) -> Result<ExponentialHistogramDataPoint, IngressError> {
    let proto::ExponentialHistogramDataPoint {
        attributes: point_attributes,
        start_time_unix_nano,
        time_unix_nano,
        count,
        sum,
        scale,
        zero_count,
        positive,
        negative,
        flags,
        exemplars: point_exemplars,
        min,
        max,
        zero_threshold,
    } = value;

    Ok(ExponentialHistogramDataPoint {
        attributes: attributes(point_attributes),
        start_time_unix_nano: start_time_unix_nano as i64,
        time_unix_nano: time_unix_nano as i64,
        count,
        sum,
        scale,
        zero_count,
        positive: positive.map(convert_bucket),
        negative: negative.map(convert_bucket),
        zero_threshold,
        min,
        max,
        flags,
        exemplars: exemplars(point_exemplars)?,
    })
}

fn convert_bucket(value: proto::exponential_histogram_data_point::Buckets) -> ExponentialBucket {
    let proto::exponential_histogram_data_point::Buckets {
        offset,
        bucket_counts,
    } = value;
    ExponentialBucket {
        offset,
        bucket_counts,
    }
}

fn convert_summary_point(value: proto::SummaryDataPoint) -> SummaryDataPoint {
    let proto::SummaryDataPoint {
        attributes: point_attributes,
        start_time_unix_nano,
        time_unix_nano,
        count,
        sum,
        quantile_values,
        flags,
    } = value;

    SummaryDataPoint {
        attributes: attributes(point_attributes),
        start_time_unix_nano: start_time_unix_nano as i64,
        time_unix_nano: time_unix_nano as i64,
        count,
        sum,
        quantile_values: quantile_values
            .into_iter()
            .map(|value_at_quantile| QuantileValue {
                quantile: value_at_quantile.quantile,
                value: value_at_quantile.value,
            })
            .collect(),
        flags,
        // The pinned OTLP proto defines no exemplars on SummaryDataPoint;
        // the model field stays for forward compatibility and the storage
        // column remains `NULL` for summary points.
        exemplars: Vec::new(),
    }
}

#[cfg(test)]
// Helpers mirror the `Option`-shaped proto fields they construct.
#[allow(clippy::unnecessary_wraps)]
mod tests {
    use super::*;
    use crate::mapping::pb::common::v1::{
        AnyValue, InstrumentationScope, KeyValue, any_value::Value,
    };
    use crate::mapping::pb::metrics::v1::{
        AggregationTemporality, Exemplar as ProtoExemplar,
        ExponentialHistogramDataPoint as ProtoExpPoint, HistogramDataPoint as ProtoHistPoint,
        Metric as ProtoMetric, NumberDataPoint as ProtoNumberPoint,
        SummaryDataPoint as ProtoSummaryPoint, exemplar, exponential_histogram_data_point::Buckets,
        number_data_point, summary_data_point::ValueAtQuantile,
    };
    use crate::mapping::pb::resource::v1::Resource as ProtoResource;
    use otel_sqlite_core::model::{AttributeValue, NumberValue, Resource, Temporality};

    fn any(kind: Value) -> Option<AnyValue> {
        Some(AnyValue { value: Some(kind) })
    }

    fn sample_resource() -> Option<ProtoResource> {
        Some(ProtoResource {
            attributes: vec![KeyValue {
                key: "service.name".to_owned(),
                value: any(Value::StringValue("payments".to_owned())),
                ..Default::default()
            }],
            ..Default::default()
        })
    }

    fn int_point(time: u64, value: i64) -> ProtoNumberPoint {
        ProtoNumberPoint {
            start_time_unix_nano: 10,
            time_unix_nano: time,
            value: Some(number_data_point::Value::AsInt(value)),
            ..Default::default()
        }
    }

    fn double_point(time: u64, value: f64) -> ProtoNumberPoint {
        ProtoNumberPoint {
            time_unix_nano: time,
            value: Some(number_data_point::Value::AsDouble(value)),
            ..Default::default()
        }
    }

    /// A valid exemplar with trace context, reused across point kinds.
    fn example_exemplar() -> ProtoExemplar {
        ProtoExemplar {
            time_unix_nano: 99,
            value: Some(exemplar::Value::AsDouble(1.5)),
            span_id: vec![9; 8],
            trace_id: vec![8; 16],
            ..Default::default()
        }
    }

    fn metric(name: &str, data: proto::metric::Data) -> ProtoMetric {
        ProtoMetric {
            name: name.to_owned(),
            description: "test metric".to_owned(),
            unit: "1".to_owned(),
            data: Some(data),
            ..Default::default()
        }
    }

    fn sum_metric(points: Vec<ProtoNumberPoint>) -> ProtoMetric {
        metric(
            "requests.total",
            proto::metric::Data::Sum(proto::Sum {
                data_points: points,
                aggregation_temporality: AggregationTemporality::Cumulative as i32,
                is_monotonic: true,
            }),
        )
    }

    #[test]
    fn converts_all_metric_kinds_into_model_batch() {
        let proto_batch = ResourceMetrics {
            resource: sample_resource(),
            schema_url: "https://example.test/schemas/metrics".to_owned(),
            scope_metrics: vec![ScopeMetrics {
                scope: Some(InstrumentationScope {
                    name: "scope-m".to_owned(),
                    version: "0.9.9".to_owned(),
                    ..Default::default()
                }),
                metrics: vec![
                    sum_metric(vec![int_point(100, 7), int_point(200, 3)]),
                    metric(
                        "cpu.usage",
                        proto::metric::Data::Gauge(proto::Gauge {
                            data_points: vec![double_point(300, 0.25)],
                        }),
                    ),
                    metric(
                        "latency",
                        proto::metric::Data::Histogram(proto::Histogram {
                            data_points: vec![ProtoHistPoint {
                                attributes: vec![KeyValue {
                                    key: "route".to_owned(),
                                    value: any(Value::StringValue("/api".to_owned())),
                                    ..Default::default()
                                }],
                                start_time_unix_nano: 5,
                                time_unix_nano: 6,
                                count: 4,
                                sum: Some(2.5),
                                bucket_counts: vec![1, 2, 1],
                                explicit_bounds: vec![0.5, 1.5],
                                min: Some(0.1),
                                max: Some(1.4),
                                flags: 0,
                                exemplars: vec![example_exemplar()],
                            }],
                            aggregation_temporality: AggregationTemporality::Delta as i32,
                        }),
                    ),
                    metric(
                        "sizes",
                        proto::metric::Data::ExponentialHistogram(proto::ExponentialHistogram {
                            data_points: vec![ProtoExpPoint {
                                start_time_unix_nano: 7,
                                time_unix_nano: 8,
                                count: 3,
                                sum: Some(9.0),
                                scale: -2,
                                zero_count: 1,
                                zero_threshold: 0.001,
                                positive: Some(Buckets {
                                    offset: 3,
                                    bucket_counts: vec![1, 1],
                                }),
                                negative: None,
                                min: Some(0.0),
                                max: Some(8.0),
                                exemplars: vec![example_exemplar()],
                                ..Default::default()
                            }],
                            aggregation_temporality: AggregationTemporality::Cumulative as i32,
                        }),
                    ),
                    metric(
                        "quantiles",
                        proto::metric::Data::Summary(proto::Summary {
                            data_points: vec![ProtoSummaryPoint {
                                attributes: vec![KeyValue {
                                    key: "flagged".to_owned(),
                                    value: any(Value::BoolValue(true)),
                                    ..Default::default()
                                }],
                                start_time_unix_nano: 11,
                                time_unix_nano: 12,
                                count: 10,
                                sum: 55.0,
                                quantile_values: vec![
                                    ValueAtQuantile {
                                        quantile: 0.5,
                                        value: 5.0,
                                    },
                                    ValueAtQuantile {
                                        quantile: 0.99,
                                        value: 9.9,
                                    },
                                ],
                                flags: 0,
                            }],
                        }),
                    ),
                ],
                ..Default::default()
            }],
        };

        assert_eq!(count(std::slice::from_ref(&proto_batch)), 6);

        let batch = try_convert_batch(proto_batch).expect("valid request converts");

        assert_eq!(batch.len(), 5);
        assert_eq!(batch.data_point_count(), 6);
        assert_eq!(batch.schema_url, "https://example.test/schemas/metrics");
        assert_eq!(
            batch.resource.as_ref().map(Resource::service_name),
            Some("payments")
        );

        let sum_record = &batch.records[0];
        assert_eq!(sum_record.name, "requests.total");
        assert_eq!(sum_record.description, "test metric");
        assert_eq!(sum_record.unit, "1");
        assert_eq!(sum_record.scope_name, "scope-m");
        assert_eq!(sum_record.scope_version, "0.9.9");
        let MetricData::Sum(sum) = &sum_record.data else {
            panic!("expected sum")
        };
        assert!(sum.is_monotonic);
        assert_eq!(sum.aggregation_temporality, Temporality::Cumulative);
        assert_eq!(sum.data_points.len(), 2);
        assert_eq!(sum.data_points[0].start_time_unix_nano, 10);
        assert_eq!(sum.data_points[0].time_unix_nano, 100);
        assert_eq!(sum.data_points[0].value, Some(NumberValue::Int(7)));
        assert_eq!(sum.data_points[1].value, Some(NumberValue::Int(3)));
        assert!(
            sum.data_points[0]
                .attributes
                .iter()
                .all(|attribute| attribute.value == AttributeValue::Null)
        );

        let MetricData::Gauge(gauge) = &batch.records[1].data else {
            panic!("expected gauge")
        };
        assert_eq!(gauge.data_points[0].value, Some(NumberValue::Double(0.25)));

        let MetricData::Histogram(histogram) = &batch.records[2].data else {
            panic!("expected histogram")
        };
        assert_eq!(histogram.aggregation_temporality, Temporality::Delta);
        let point = &histogram.data_points[0];
        assert_eq!(point.count, 4);
        assert_eq!(point.sum, Some(2.5));
        assert_eq!(point.bucket_counts, vec![1, 2, 1]);
        assert_eq!(point.explicit_bounds, vec![0.5, 1.5]);
        assert_eq!(point.min, Some(0.1));
        assert_eq!(point.max, Some(1.4));
        assert_eq!(point.attributes[0].key, "route");
        assert_eq!(point.attributes[0].value.as_str(), Some("/api"));
        assert_eq!(point.exemplars.len(), 1);
        assert_eq!(point.exemplars[0].trace_id, Some([8; 16]));
        assert_eq!(point.exemplars[0].value, Some(NumberValue::Double(1.5)));

        let MetricData::ExponentialHistogram(exponential) = &batch.records[3].data else {
            panic!("expected exponential histogram")
        };
        let point = &exponential.data_points[0];
        assert_eq!(exponential.aggregation_temporality, Temporality::Cumulative);
        assert_eq!(point.count, 3);
        assert_eq!(point.sum, Some(9.0));
        assert_eq!(point.scale, -2);
        assert_eq!(point.zero_count, 1);
        assert_eq!(point.zero_threshold, 0.001);
        assert_eq!(
            point.positive,
            Some(ExponentialBucket {
                offset: 3,
                bucket_counts: vec![1, 1],
            })
        );
        assert_eq!(point.negative, None);
        assert_eq!(point.min, Some(0.0));
        assert_eq!(point.max, Some(8.0));
        assert_eq!(point.exemplars.len(), 1);
        assert_eq!(point.exemplars[0].span_id, Some([9; 8]));

        let MetricData::Summary(summary) = &batch.records[4].data else {
            panic!("expected summary")
        };
        let point = &summary.data_points[0];
        assert_eq!(point.count, 10);
        assert_eq!(point.sum, 55.0);
        assert_eq!(point.attributes[0].value.as_bool(), Some(true));
        assert_eq!(point.quantile_values.len(), 2);
        assert_eq!(point.quantile_values[0].quantile, 0.5);
        assert_eq!(point.quantile_values[1].value, 9.9);
    }

    #[test]
    fn counts_data_points_across_scopes() {
        let proto_batch = ResourceMetrics {
            scope_metrics: vec![
                ScopeMetrics {
                    metrics: vec![sum_metric(vec![int_point(1, 1), int_point(2, 2)])],
                    ..Default::default()
                },
                ScopeMetrics {
                    metrics: vec![metric(
                        "cpu.usage",
                        proto::metric::Data::Gauge(proto::Gauge {
                            data_points: vec![double_point(3, 1.0), double_point(4, 2.0)],
                        }),
                    )],
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        assert_eq!(count(&[proto_batch]), 4);
    }

    #[test]
    fn maps_one_chunk_per_resource_metrics_group_without_batching_policy() {
        let shared = || ResourceMetrics {
            resource: sample_resource(),
            schema_url: "https://example.test/schemas/metrics".to_owned(),
            scope_metrics: vec![ScopeMetrics {
                metrics: vec![sum_metric(vec![int_point(1, 1)])],
                ..Default::default()
            }],
        };

        let other = ResourceMetrics {
            scope_metrics: vec![ScopeMetrics {
                metrics: vec![sum_metric(vec![int_point(2, 2)])],
                ..Default::default()
            }],
            ..Default::default()
        };

        let chunks = map_chunks(vec![shared(), shared(), other]).expect("valid request");

        assert_eq!(chunks.len(), 3);
        for (points, command) in &chunks {
            assert_eq!(*points, 1);
            match command {
                IngestMessage::Metrics(chunk) => assert_eq!(chunk.records.len(), 1),
                _ => panic!("expected insert metrics chunk"),
            }
        }
        let IngestMessage::Metrics(first) = &chunks[0].1 else {
            panic!("expected metrics chunk");
        };
        assert!(first.origin.resource.is_some());

        let oversized = ResourceMetrics {
            scope_metrics: vec![ScopeMetrics {
                metrics: (0..4i64)
                    .map(|i| sum_metric(vec![int_point(i as u64, i)]))
                    .collect(),
                ..Default::default()
            }],
            ..Default::default()
        };

        let chunks = map_chunks(vec![oversized]).expect("valid request");
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].0, 4);
    }

    #[test]
    fn skips_groups_without_data_points() {
        let chunks = map_chunks(vec![ResourceMetrics {
            scope_metrics: vec![ScopeMetrics {
                metrics: vec![ProtoMetric {
                    name: "empty".to_owned(),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }])
        .expect("valid request");
        assert!(chunks.is_empty());
    }

    #[test]
    fn skips_metrics_without_data() {
        let proto_batch = ResourceMetrics {
            scope_metrics: vec![ScopeMetrics {
                metrics: vec![ProtoMetric {
                    name: "empty".to_owned(),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };

        let batch = try_convert_batch(proto_batch).expect("valid request converts");
        assert!(batch.is_empty());
        assert_eq!(
            count(&[ResourceMetrics {
                scope_metrics: vec![],
                ..Default::default()
            }]),
            0
        );
    }

    #[test]
    fn preserves_exemplars_metadata_and_scope_attributes() {
        let exemplar = |trace_id: Vec<u8>, span_id: Vec<u8>| ProtoExemplar {
            filtered_attributes: vec![KeyValue {
                key: "filtered".to_owned(),
                value: any(Value::BoolValue(true)),
                ..Default::default()
            }],
            time_unix_nano: 400,
            value: Some(exemplar::Value::AsDouble(0.125)),
            span_id,
            trace_id,
        };

        let proto_batch = ResourceMetrics {
            resource: sample_resource(),
            scope_metrics: vec![ScopeMetrics {
                scope: Some(InstrumentationScope {
                    name: "scope-m".to_owned(),
                    version: "0.9.9".to_owned(),
                    attributes: vec![KeyValue {
                        key: "scope.tag".to_owned(),
                        value: any(Value::StringValue("prod".to_owned())),
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
                metrics: vec![ProtoMetric {
                    name: "requests.total".to_owned(),
                    metadata: vec![KeyValue {
                        key: "unit.origin".to_owned(),
                        value: any(Value::StringValue("prometheus".to_owned())),
                        ..Default::default()
                    }],
                    data: Some(proto::metric::Data::Gauge(proto::Gauge {
                        data_points: vec![ProtoNumberPoint {
                            time_unix_nano: 100,
                            value: Some(number_data_point::Value::AsInt(7)),
                            exemplars: vec![
                                exemplar(vec![8; 16], vec![9; 8]),
                                exemplar(vec![], vec![]),
                            ],
                            ..Default::default()
                        }],
                    })),
                    ..Default::default()
                }],
                schema_url: "https://example.test/schemas/metrics/scope".to_owned(),
            }],
            schema_url: "https://example.test/schemas/metrics".to_owned(),
        };

        let batch = try_convert_batch(proto_batch).expect("valid request converts");

        assert_eq!(batch.schema_url, "https://example.test/schemas/metrics");
        assert_eq!(
            batch
                .resource
                .as_ref()
                .map(|resource| resource.schema_url.as_str()),
            Some("https://example.test/schemas/metrics")
        );

        let record = &batch.records[0];
        assert_eq!(record.metadata[0].key, "unit.origin");
        assert_eq!(record.metadata[0].value.as_str(), Some("prometheus"));
        assert_eq!(record.scope_name, "scope-m");
        assert_eq!(record.scope_version, "0.9.9");
        assert_eq!(record.scope_attributes[0].value.as_str(), Some("prod"));
        assert_eq!(
            record.scope_schema_url,
            "https://example.test/schemas/metrics/scope"
        );

        let MetricData::Gauge(gauge) = &record.data else {
            panic!("expected gauge")
        };
        let point = &gauge.data_points[0];
        assert_eq!(point.exemplars.len(), 2);
        assert_eq!(point.exemplars[0].trace_id, Some([8; 16]));
        assert_eq!(point.exemplars[0].span_id, Some([9; 8]));
        assert_eq!(point.exemplars[0].value, Some(NumberValue::Double(0.125)));
        assert_eq!(
            point.exemplars[0].filtered_attributes[0].value,
            AttributeValue::Bool(true)
        );
        assert_eq!(point.exemplars[1].trace_id, None);
        assert_eq!(point.exemplars[1].span_id, None);
    }

    #[test]
    fn rejects_malformed_exemplar_ids() {
        let metric_with_exemplar = |trace_id: Vec<u8>, span_id: Vec<u8>| ResourceMetrics {
            scope_metrics: vec![ScopeMetrics {
                metrics: vec![ProtoMetric {
                    name: "requests.total".to_owned(),
                    data: Some(proto::metric::Data::Gauge(proto::Gauge {
                        data_points: vec![ProtoNumberPoint {
                            time_unix_nano: 100,
                            value: Some(number_data_point::Value::AsInt(7)),
                            exemplars: vec![ProtoExemplar {
                                span_id,
                                trace_id,
                                ..Default::default()
                            }],
                            ..Default::default()
                        }],
                    })),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };

        assert!(map_chunks(vec![metric_with_exemplar(vec![1], vec![9; 8])]).is_err());
        assert!(map_chunks(vec![metric_with_exemplar(vec![8; 16], vec![1])]).is_err());
        assert!(map_chunks(vec![metric_with_exemplar(vec![0; 16], vec![])]).is_err());
        assert!(map_chunks(vec![metric_with_exemplar(vec![], vec![])]).is_ok());
    }
}
