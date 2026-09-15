use std::sync::Arc;
use std::time::Instant;

use otel_sqlite_core::storage::{CommitLedger, DurabilityMode};
use tokio::sync::Semaphore;
use tonic::{Request, Response, Status};

use crate::IngestSender;
use crate::config::IngressConfig;
use crate::enqueue::enqueue;
use crate::error::IngressError;
use crate::mapping::metrics::{count, map_chunks, validate_request};
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
    /// Process-wide export concurrency guard shared with the logs service;
    /// the HTTP/2 `max_concurrent_streams` setting is per connection, so this
    /// is what bounds in-flight exports across all connections. `new` leaves
    /// it effectively unlimited; production wiring attaches the shared one.
    stream_limit: Arc<Semaphore>,
}

impl MetricsIngress {
    pub fn new(queue: IngestSender, config: Arc<IngressConfig>, commit: Arc<CommitLedger>) -> Self {
        Self {
            queue,
            config,
            commit,
            stream_limit: Arc::new(Semaphore::new(Semaphore::MAX_PERMITS)),
        }
    }

    /// Shares `limit` with other OTLP services so all exports draw from one
    /// process-wide concurrency budget.
    pub fn with_stream_limit(mut self, limit: Arc<Semaphore>) -> Self {
        self.stream_limit = limit;
        self
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
    pub(crate) async fn handle(
        &self,
        request: ExportMetricsServiceRequest,
    ) -> Result<ExportMetricsServiceResponse, Status> {
        // Process-wide export concurrency: the transport's per-connection
        // stream cap is not enough to bound in-flight memory across
        // connections. Acquirable unless the semaphore is closed, which never
        // happens here.
        let _permit = self
            .stream_limit
            .acquire()
            .await
            .map_err(|_| Status::internal("metrics stream limiter is closed"))?;

        // Request-level counting and validation run on the blocking pool with
        // the CPU-heavy mapping: the async path only ever does channel and
        // ledger work, so a walk of borrowed proto fields cannot stall a tokio
        // worker. The data-point cap is checked before mapping allocates.
        let resource_metrics = request.resource_metrics;
        let limits = Arc::clone(&self.config);
        let work = tokio::time::timeout(
            crate::config::MAPPING_TIMEOUT,
            tokio::task::spawn_blocking(move || {
                let _span = tracing::info_span!("map").entered();
                let total = count(&resource_metrics);
                if total > limits.max_records_per_request {
                    return Err(IngressError::Mapping(format!(
                        "request contains {total} data points, limit is {}",
                        limits.max_records_per_request
                    )));
                }
                validate_request(&resource_metrics, &limits)?;
                ::metrics::counter!("otlp_records_received_total", "signal" => "metrics")
                    .increment(total as u64);
                map_chunks(resource_metrics)
            }),
        )
        .await
        .map_err(|_| {
            // tokio cannot cancel `spawn_blocking`; the mapping task keeps
            // running in the background, but the future, stream slot and
            // concurrency permit are released so the exporter can retry.
            ::metrics::counter!("otlp_mapping_timeout_total", "signal" => "metrics").increment(1);
            Status::unavailable("metrics mapping timed out")
        })?
        .map_err(|error| Status::internal(format!("metrics mapping task failed: {error}")))?
        .map_err(|error| Status::invalid_argument(error.to_string()))?;

        let outcome = {
            let _span = tracing::info_span!("enqueue").entered();
            // A request mapping to more chunks than the queue can ever hold
            // is permanent: answer INVALID_ARGUMENT (via the mapping error)
            // instead of a retryable UNAVAILABLE.
            enqueue("metrics", &self.queue, &self.commit, work)
                .map_err(|error| Status::invalid_argument(error.to_string()))?
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
        // Two of the three ingest slots are already occupied, so this
        // request's two chunks cannot both fit right now. That is transient
        // backpressure (a drain could make room), so it stays retryable.
        let (queue, receiver) = channel(3);
        queue
            .send(otel_sqlite_core::storage::IngestMessage::Flush)
            .expect("prefill");
        queue
            .send(otel_sqlite_core::storage::IngestMessage::Flush)
            .expect("prefill");
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
        assert_eq!(receiver.len(), 2, "only the prefill messages remain");
    }

    #[tokio::test]
    async fn request_mapping_to_more_chunks_than_capacity_is_invalid_argument() {
        let ledger = Arc::new(CommitLedger::new());
        let config = Arc::new(IngressConfig {
            durability_mode: DurabilityMode::Commit,
            ..IngressConfig::default()
        });
        // One ingest slot total: no drain can ever fit a two-chunk request.
        let (queue, receiver) = channel(1);
        let ingress = MetricsIngress::new(queue, config, Arc::clone(&ledger));

        let request = ExportMetricsServiceRequest {
            resource_metrics: vec![metric_group("req.a"), metric_group("req.b")],
        };

        let status =
            tokio::time::timeout(std::time::Duration::from_secs(2), ingress.handle(request))
                .await
                .expect("permanent rejection returns promptly")
                .expect_err("a request that can never fit must be rejected permanently");
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert!(
            status.message().contains("split the request"),
            "the error must tell the client how to proceed: {}",
            status.message()
        );
        assert!(receiver.is_empty(), "nothing may be enqueued");
        assert_eq!(ledger.watermark().committed_through, 0);
    }

    #[tokio::test]
    async fn invalid_exemplar_trace_id_is_accepted_with_absent_context() {
        let ledger = Arc::new(CommitLedger::new());
        let config = Arc::new(IngressConfig {
            durability_mode: DurabilityMode::Enqueue,
            ..IngressConfig::default()
        });
        let (queue, receiver) = channel(4);
        let ingress = MetricsIngress::new(queue, config, Arc::clone(&ledger));

        // A wrong-length exemplar trace id carries no trace association: it
        // must be zeroed and counted, never reject the request (OTLP SHOULD).
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
                                    span_id: vec![9; 8],
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

        let _response =
            tokio::time::timeout(std::time::Duration::from_secs(2), ingress.handle(request))
                .await
                .expect("invalid ids must not stall the export")
                .expect("invalid ids must not reject the request");
        let message = receiver.recv().expect("record enqueued");
        let otel_sqlite_core::storage::IngestMessage::Metrics(chunk) = message else {
            panic!("expected metrics chunk");
        };
        let otel_sqlite_core::model::MetricData::Gauge(gauge) = &chunk.records[0].data else {
            panic!("expected gauge");
        };
        let exemplar = &gauge.data_points[0].exemplars[0];
        assert_eq!(exemplar.trace_id, None);
        assert_eq!(exemplar.span_id, Some([9; 8]));
    }
}
