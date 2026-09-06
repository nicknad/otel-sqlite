use std::sync::Arc;
use std::time::Instant;

use otel_sqlite_core::storage::{CommitLedger, DurabilityMode};
use tonic::{Request, Response, Status};

use crate::IngestSender;
use crate::config::IngressConfig;
use crate::enqueue::enqueue;
use crate::mapping::logs::{count, map_chunks, validate_request};
use crate::mapping::pb::collector::logs::v1::{
    ExportLogsServiceRequest, ExportLogsServiceResponse, logs_service_server::LogsService,
};

#[derive(Debug, Clone)]
pub struct LogsIngress {
    queue: IngestSender,
    config: Arc<IngressConfig>,
    /// Durability-ticket book shared with the storage pipeline. In
    /// `DurabilityMode::Commit` the export response waits here until the
    /// writer has committed everything this request had accepted.
    commit: Arc<CommitLedger>,
}

impl LogsIngress {
    pub fn new(queue: IngestSender, config: Arc<IngressConfig>, commit: Arc<CommitLedger>) -> Self {
        Self {
            queue,
            config,
            commit,
        }
    }
}

#[tonic::async_trait]
impl LogsService for LogsIngress {
    // The async signature is fixed by the tonic service trait.
    #[allow(clippy::unused_async)]
    #[tracing::instrument(name = "otlp.logs.export", skip_all)]
    async fn export(
        &self,
        request: Request<ExportLogsServiceRequest>,
    ) -> Result<Response<ExportLogsServiceResponse>, Status> {
        let started = Instant::now();
        ::metrics::counter!("otlp_requests_total", "signal" => "logs").increment(1);

        let response = self.handle(request.into_inner()).await;

        ::metrics::histogram!("otlp_request_duration", "signal" => "logs")
            .record(started.elapsed().as_secs_f64());

        match response {
            Ok(response) => Ok(Response::new(response)),
            Err(status) => {
                ::metrics::counter!("otlp_request_errors_total", "signal" => "logs",
                    "code" => format!("{:?}", status.code()))
                .increment(1);
                Err(status)
            }
        }
    }
}

impl LogsIngress {
    async fn handle(
        &self,
        request: ExportLogsServiceRequest,
    ) -> Result<ExportLogsServiceResponse, Status> {
        let total = {
            let _span = tracing::info_span!("validate").entered();
            let total = count(&request.resource_logs);
            if total > self.config.max_records_per_request {
                return Err(Status::invalid_argument(format!(
                    "request contains {total} log records, limit is {}",
                    self.config.max_records_per_request
                )));
            }
            // Per-field caps (H2) + nesting depth (H3) before any allocation:
            // a single record with millions of attributes/bytes must fail here,
            // not inside the blocking mapping task.
            validate_request(&request.resource_logs, &self.config)
                .map_err(|error| Status::invalid_argument(error.to_string()))?;
            total
        };

        ::metrics::counter!("otlp_records_received_total", "signal" => "logs")
            .increment(total as u64);

        // CPU-heavy proto→model conversion (attribute cloning, canonical
        // JSON) runs on the blocking pool so this tokio worker stays free
        // to serve other streams while a large request maps.
        let resource_logs = request.resource_logs;
        let work = tokio::task::spawn_blocking(move || {
            let _span = tracing::info_span!("map").entered();
            map_chunks(resource_logs)
        })
        .await
        .map_err(|error| Status::internal(format!("log mapping task failed: {error}")))?
        .map_err(|error| Status::invalid_argument(error.to_string()))?;

        let outcome = {
            let _span = tracing::info_span!("enqueue").entered();
            enqueue("logs", &self.queue, &self.commit, work)
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

        Ok(ExportLogsServiceResponse::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::channel;
    use crate::mapping::pb::logs::v1::{LogRecord as ProtoLogRecord, ResourceLogs, ScopeLogs};
    use std::sync::atomic::{AtomicU64, Ordering};

    fn ingress_with(
        capacity: usize,
        durability_mode: DurabilityMode,
        commit: Arc<CommitLedger>,
    ) -> (LogsIngress, Arc<IngressConfig>) {
        let config = Arc::new(IngressConfig {
            durability_mode,
            ..IngressConfig::default()
        });
        let (queue, receiver) = channel(capacity);
        std::mem::forget(receiver);
        (LogsIngress::new(queue, Arc::clone(&config), commit), config)
    }

    fn request_with(records: usize) -> ExportLogsServiceRequest {
        use crate::mapping::pb::logs::v1::{LogRecord as ProtoLogRecord, ResourceLogs, ScopeLogs};
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let salt = SEQ.fetch_add(1, Ordering::Relaxed);
        ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                scope_logs: vec![ScopeLogs {
                    log_records: (0..records)
                        .map(|i| ProtoLogRecord {
                            severity_text: format!("INFO{i}{salt}"),
                            ..ProtoLogRecord::default()
                        })
                        .collect(),
                    ..ScopeLogs::default()
                }],
                ..ResourceLogs::default()
            }],
        }
    }

    #[tokio::test]
    async fn durable_mode_releases_the_response_only_after_commit() {
        let ledger = Arc::new(CommitLedger::new());
        let (ingress, _) = ingress_with(8, DurabilityMode::Commit, Arc::clone(&ledger));

        let started = Instant::now();
        let handle = tokio::spawn(async move { ingress.handle(request_with(4)).await });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(
            !handle.is_finished(),
            "durable ack must wait for the writer's commit"
        );

        // Simulate the writer committing the request's ticket.
        ledger.complete(1);

        let response = tokio::time::timeout(std::time::Duration::from_secs(2), handle)
            .await
            .expect("response released after commit")
            .expect("handler did not panic")
            .expect("export succeeds");
        assert!(response.partial_success.is_none());
        assert!(started.elapsed() >= std::time::Duration::from_millis(50));
    }

    #[tokio::test]
    async fn durable_mode_fails_fast_when_the_pipeline_closes() {
        let ledger = Arc::new(CommitLedger::new());
        let (ingress, _) = ingress_with(8, DurabilityMode::Commit, Arc::clone(&ledger));

        let handle = tokio::spawn(async move { ingress.handle(request_with(2)).await });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        ledger.close();

        let status = tokio::time::timeout(std::time::Duration::from_secs(2), handle)
            .await
            .expect("closed ledger must release the waiter")
            .expect("handler did not panic")
            .expect_err("uncommittable records must fail the export");
        assert_eq!(status.code(), tonic::Code::Unavailable);
    }

    #[tokio::test]
    async fn enqueue_mode_never_waits_for_commits() {
        let ledger = Arc::new(CommitLedger::new());
        let (ingress, _) = ingress_with(8, DurabilityMode::Enqueue, Arc::clone(&ledger));

        let response = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            ingress.handle(request_with(2)),
        )
        .await
        .expect("enqueue mode returns immediately")
        .expect("export succeeds");
        assert!(response.partial_success.is_none());
        // Nothing was committed, yet the ack already went out.
        assert_eq!(ledger.watermark().committed_through, 0);
    }

    #[tokio::test]
    async fn saturated_queue_rejects_the_whole_request_without_a_durable_wait() {
        let ledger = Arc::new(CommitLedger::new());
        let config = Arc::new(IngressConfig {
            durability_mode: DurabilityMode::Commit,
            ..IngressConfig::default()
        });
        // One ingest slot, never drained: no chunk of this request can fit.
        let (queue, receiver) = channel(1);
        std::mem::forget(receiver);
        let ingress = LogsIngress::new(queue, config, Arc::clone(&ledger));

        let group = |severity: &str| ResourceLogs {
            scope_logs: vec![ScopeLogs {
                log_records: vec![ProtoLogRecord {
                    severity_text: severity.to_owned(),
                    ..ProtoLogRecord::default()
                }],
                ..ScopeLogs::default()
            }],
            ..ResourceLogs::default()
        };
        let request = ExportLogsServiceRequest {
            resource_logs: vec![group("A"), group("B")],
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
    async fn malformed_trace_id_rejects_the_whole_request_before_enqueue() {
        let ledger = Arc::new(CommitLedger::new());
        let config = Arc::new(IngressConfig {
            durability_mode: DurabilityMode::Commit,
            ..IngressConfig::default()
        });
        let (queue, receiver) = channel(4);
        let ingress = LogsIngress::new(queue.clone(), config, Arc::clone(&ledger));

        // A wrong-length trace id must surface as INVALID_ARGUMENT and enqueue
        // nothing (all-or-nothing: no partial acceptance, no ticket issued).
        let request = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                scope_logs: vec![ScopeLogs {
                    log_records: vec![ProtoLogRecord {
                        trace_id: vec![1, 2],
                        ..ProtoLogRecord::default()
                    }],
                    ..ScopeLogs::default()
                }],
                ..ResourceLogs::default()
            }],
        };

        let status =
            tokio::time::timeout(std::time::Duration::from_secs(2), ingress.handle(request))
                .await
                .expect("rejection returns promptly")
                .expect_err("malformed id must be rejected");
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert!(
            receiver.is_empty(),
            "a rejected request must not leave chunks behind"
        );
        assert_eq!(ledger.watermark().committed_through, 0);
    }
}
