use std::sync::Arc;
use std::time::Instant;

use otel_sqlite_core::storage::{CommitLedger, DurabilityMode};
use tonic::{Request, Response, Status};

use crate::IngestSender;
use crate::config::IngressConfig;
use crate::enqueue::enqueue;
use crate::mapping::metrics::{count, map_chunks};
use crate::mapping::pb::collector::metrics::v1::{
    ExportMetricsServiceRequest, ExportMetricsServiceResponse,
    metrics_service_server::MetricsService,
};

#[derive(Debug, Clone)]
pub struct MetricsIngress {
    queue: IngestSender,
    config: Arc<IngressConfig>,
    /// Durability-ticket book shared with the storage pipeline. In
    /// `DurabilityMode::Commit` the export response waits here until the
    /// writer has committed everything this request had accepted.
    commit: Arc<CommitLedger>,
}

impl MetricsIngress {
    pub fn new(queue: IngestSender, config: Arc<IngressConfig>, commit: Arc<CommitLedger>) -> Self {
        Self {
            queue,
            config,
            commit,
        }
    }
}

#[tonic::async_trait]
impl MetricsService for MetricsIngress {
    // The async signature is fixed by the tonic service trait.
    #[allow(clippy::unused_async)]
    #[tracing::instrument(name = "otlp.metrics.export", skip_all)]
    async fn export(
        &self,
        request: Request<ExportMetricsServiceRequest>,
    ) -> Result<Response<ExportMetricsServiceResponse>, Status> {
        let started = Instant::now();
        ::metrics::counter!("otlp_requests_total", "signal" => "metrics").increment(1);

        let response = self.handle(request.into_inner()).await;

        ::metrics::histogram!("otlp_request_duration", "signal" => "metrics")
            .record(started.elapsed().as_secs_f64());

        match response {
            Ok(response) => Ok(Response::new(response)),
            Err(status) => {
                ::metrics::counter!("otlp_request_errors_total", "signal" => "metrics",
                    "code" => format!("{:?}", status.code()))
                .increment(1);
                Err(status)
            }
        }
    }
}

impl MetricsIngress {
    async fn handle(
        &self,
        request: ExportMetricsServiceRequest,
    ) -> Result<ExportMetricsServiceResponse, Status> {
        let total = {
            let _span = tracing::info_span!("validate").entered();
            let total = count(&request.resource_metrics);
            if total > self.config.max_records_per_request {
                return Err(Status::invalid_argument(format!(
                    "request contains {total} data points, limit is {}",
                    self.config.max_records_per_request
                )));
            }
            total
        };

        ::metrics::counter!("otlp_records_received_total", "signal" => "metrics")
            .increment(total as u64);

        let work = {
            let _span = tracing::info_span!("map").entered();
            map_chunks(request.resource_metrics)
                .map_err(|error| Status::invalid_argument(error.to_string()))?
        };

        let outcome = {
            let _span = tracing::info_span!("enqueue").entered();
            enqueue("metrics", &self.queue, &self.commit, work)
        };

        // All-or-nothing admission: a request is either fully accepted or
        // fully rejected. Both failure checks run before the durable-ack wait
        // so a failed request returns promptly instead of waiting on a commit
        // that cannot come (nothing was accepted, no ticket was issued).
        if outcome.disconnected {
            return Err(Status::unavailable("ingress queue is closed"));
        }

        if outcome.rejected > 0 {
            tracing::warn!(
                accepted = outcome.accepted,
                rejected = outcome.rejected,
                "ingress queue backpressure"
            );
            return Err(Status::unavailable("ingress queue is full"));
        }

        if let Some(ticket) = outcome.last_ticket
            && self.config.durability_mode == DurabilityMode::Commit
        {
            // Durable ack: hold the response until this request's records
            // are committed (or the pipeline died, which fails the wait).
            self.commit
                .committed(ticket)
                .await
                .map_err(|_| Status::unavailable("storage pipeline shut down before commit"))?;
        }

        Ok(ExportMetricsServiceResponse::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::channel;
    use crate::mapping::pb::collector::metrics::v1::ExportMetricsServiceRequest;
    use crate::mapping::pb::metrics::v1 as proto;
    use crate::mapping::pb::metrics::v1::{
        AggregationTemporality, Metric as ProtoMetric, NumberDataPoint as ProtoNumberPoint,
        ResourceMetrics, ScopeMetrics, number_data_point,
    };
    use std::sync::Arc;

    fn metric_group(name: &str) -> ResourceMetrics {
        ResourceMetrics {
            scope_metrics: vec![ScopeMetrics {
                metrics: vec![ProtoMetric {
                    name: name.to_owned(),
                    data: Some(proto::metric::Data::Sum(proto::Sum {
                        data_points: vec![ProtoNumberPoint {
                            time_unix_nano: 1,
                            value: Some(number_data_point::Value::AsInt(1)),
                            ..Default::default()
                        }],
                        aggregation_temporality: AggregationTemporality::Cumulative as i32,
                        is_monotonic: true,
                    })),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn saturated_queue_rejects_the_whole_request_without_a_durable_wait() {
        let ledger = Arc::new(CommitLedger::new());
        let config = Arc::new(IngressConfig {
            durability_mode: DurabilityMode::Commit,
            ..IngressConfig::default()
        });
        // One ingest slot, never drained: the two mapped chunks of this
        // request can never both fit, so the whole request is refused.
        let (queue, receiver) = channel(1);
        std::mem::forget(receiver);
        let ingress = MetricsIngress::new(queue, config, Arc::clone(&ledger));

        let request = ExportMetricsServiceRequest {
            resource_metrics: vec![metric_group("req.a"), metric_group("req.b")],
        };

        // Even in durable mode the request must return promptly as UNAVAILABLE:
        // nothing was accepted, so there is no commit to wait for.
        let status =
            tokio::time::timeout(std::time::Duration::from_secs(2), ingress.handle(request))
                .await
                .expect("rejection must not wait for a commit that cannot come")
                .expect_err("whole request rejected");
        assert_eq!(status.code(), tonic::Code::Unavailable);
        assert_eq!(
            ledger.watermark().committed_through,
            0,
            "nothing was accepted, so the watermark never moved"
        );
        assert!(!ledger.watermark().closed);
    }

    #[tokio::test]
    async fn malformed_exemplar_trace_id_rejects_the_whole_request_before_enqueue() {
        let ledger = Arc::new(CommitLedger::new());
        let config = Arc::new(IngressConfig {
            durability_mode: DurabilityMode::Commit,
            ..IngressConfig::default()
        });
        let (queue, receiver) = channel(4);
        let ingress = MetricsIngress::new(queue.clone(), config, Arc::clone(&ledger));

        // A wrong-length exemplar trace id must surface as INVALID_ARGUMENT
        // and enqueue nothing (all-or-nothing: no partial acceptance, no
        // ticket issued).
        let request = ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                scope_metrics: vec![ScopeMetrics {
                    metrics: vec![ProtoMetric {
                        name: "requests.total".to_owned(),
                        data: Some(proto::metric::Data::Gauge(proto::Gauge {
                            data_points: vec![ProtoNumberPoint {
                                time_unix_nano: 1,
                                value: Some(number_data_point::Value::AsInt(1)),
                                exemplars: vec![proto::Exemplar {
                                    trace_id: vec![1],
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
            }],
        };

        let status =
            tokio::time::timeout(std::time::Duration::from_secs(2), ingress.handle(request))
                .await
                .expect("rejection returns promptly")
                .expect_err("malformed exemplar id must be rejected");
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert!(
            receiver.is_empty(),
            "a rejected request must not leave chunks behind"
        );
        assert_eq!(ledger.watermark().committed_through, 0);
    }
}
