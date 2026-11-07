use std::sync::Arc;
use std::time::Instant;

use otel_sqlite_core::storage::{CommitLedger, DurabilityMode, TicketState};
use tokio::sync::Semaphore;
use tonic::{Request, Response, Status};

use crate::IngestSender;
use crate::config::IngressConfig;
use crate::enqueue::{EnqueueOutcome, enqueue};
use crate::error::IngressError;
use crate::mapping::budget::{MappingBudget, MappingError};
use crate::mapping::logs::{count, map_chunks, validate_request};
use crate::mapping::pb::collector::logs::v1::{
    ExportLogsPartialSuccess, ExportLogsServiceRequest, ExportLogsServiceResponse,
    logs_service_server::LogsService,
};

#[derive(Debug, Clone)]
pub struct LogsIngress {
    queue: IngestSender,
    config: Arc<IngressConfig>,
    /// Durability-ticket book shared with the storage pipeline. In
    /// `DurabilityMode::Commit` the export response waits here until the
    /// writer has committed everything this request had accepted.
    commit: Arc<CommitLedger>,
    /// Process-wide export concurrency guard shared by every connection;
    /// the HTTP/2 `max_concurrent_streams` setting is per connection, so this
    /// is what bounds in-flight exports across all connections. `new` leaves
    /// it effectively unlimited; production wiring attaches the shared one.
    stream_limit: Arc<Semaphore>,
    /// Process-wide budget for proto→model mapping jobs. Because tokio
    /// cannot cancel `spawn_blocking`, the permit is held by the blocking
    /// closure rather than this request future; a timed-out export therefore
    /// frees its stream slot while its mapping job stays counted. `new` leaves
    /// it unlimited; production wiring attaches the configured bound.
    mapping_budget: MappingBudget,
}

impl LogsIngress {
    pub fn new(queue: IngestSender, config: Arc<IngressConfig>, commit: Arc<CommitLedger>) -> Self {
        Self {
            queue,
            config,
            commit,
            stream_limit: Arc::new(Semaphore::new(Semaphore::MAX_PERMITS)),
            mapping_budget: MappingBudget::unlimited(crate::config::MAPPING_TIMEOUT),
        }
    }

    /// Sets the process-wide export concurrency budget: all OTLP exports draw
    /// from this one shared limit, not the per-connection stream cap.
    pub fn with_stream_limit(mut self, limit: Arc<Semaphore>) -> Self {
        self.stream_limit = limit;
        self
    }

    /// Sets the process-wide mapping-job budget from a concurrency limit,
    /// using the standard mapping deadline.
    pub fn with_mapping_limit(mut self, concurrency: usize) -> Self {
        self.mapping_budget = MappingBudget::new(concurrency, crate::config::MAPPING_TIMEOUT);
        self
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
    pub(crate) async fn handle(
        &self,
        request: ExportLogsServiceRequest,
    ) -> Result<ExportLogsServiceResponse, Status> {
        // Process-wide export concurrency: the transport's per-connection
        // stream cap is not enough to bound in-flight memory across
        // connections. An *owned* permit is used so it can follow the request
        // into the detached enqueue task below, keeping the bound even if the
        // handler future is cancelled mid-enqueue. Acquirable unless the
        // semaphore is closed, which never happens here.
        let permit = Arc::clone(&self.stream_limit)
            .acquire_owned()
            .await
            .map_err(|_| Status::internal("log stream limiter is closed"))?;

        // Request-level counting and validation run on the blocking pool with
        // the CPU-heavy mapping: the async path only ever does channel and
        // ledger work, so a walk of borrowed proto fields cannot stall a tokio
        // worker. The record cap is checked before mapping allocates chunks.
        let resource_logs = request.resource_logs;
        let limits = Arc::clone(&self.config);
        let work = self
            .mapping_budget
            .run(move || {
                let _span = tracing::info_span!("map").entered();
                let total = count(&resource_logs);
                if total > limits.max_records_per_request {
                    return Err(IngressError::Mapping(format!(
                        "request contains {total} log records, limit is {}",
                        limits.max_records_per_request
                    )));
                }
                validate_request(&resource_logs, &limits)?;
                ::metrics::counter!("otlp_records_received_total", "signal" => "logs")
                    .increment(total as u64);
                map_chunks(resource_logs)
            })
            .await
            .map_err(|error| match error {
                MappingError::Timeout => {
                    // The abandoned job keeps its mapping permit until it
                    // finishes, but the future and stream slot are released so
                    // the exporter can retry.
                    ::metrics::counter!("otlp_mapping_timeout_total", "signal" => "logs")
                        .increment(1);
                    Status::unavailable("log mapping timed out")
                }
                MappingError::Closed => Status::internal("log mapping budget is closed"),
                MappingError::Join(error) => {
                    Status::internal(format!("log mapping task failed: {error}"))
                }
            })?
            .map_err(|error| Status::invalid_argument(error.to_string()))?;

        // Enqueue runs on the blocking pool, not the async worker: it acquires
        // the process-wide admission gate and can briefly wait on it under
        // contention, so keeping it off the tokio workers stops one busy export
        // from stalling unrelated RPCs. It is deliberately not wrapped in the
        // mapping timeout — cancelling `spawn_blocking` is impossible, and a
        // timed-out enqueue that then completed would break all-or-nothing.
        // The owned concurrency permit moves into the task so the export bound
        // holds for the whole enqueue even if this future is dropped.
        let outcome = {
            let queue = self.queue.clone();
            let ledger = Arc::clone(&self.commit);
            tokio::task::spawn_blocking(move || {
                let _permit = permit;
                let _span = tracing::info_span!("enqueue").entered();
                // A request mapping to more chunks than the queue can ever
                // hold is permanent: answer INVALID_ARGUMENT (via the mapping
                // error) instead of a retryable UNAVAILABLE.
                enqueue(&queue, &ledger, work)
            })
            .await
            .map_err(|error| Status::internal(format!("enqueue task failed: {error}")))?
            .map_err(|error| Status::invalid_argument(error.to_string()))?
        };

        self.finish(outcome).await
    }

    /// Turns an enqueue outcome into the wire response.
    ///
    /// A partially accepted request is reported with OTLP `partial_success`:
    /// records that committed are not retried, so a retrying exporter cannot
    /// duplicate them. A retryable `UNAVAILABLE` is reserved for requests with
    /// no durable records at all.
    async fn finish(&self, outcome: EnqueueOutcome) -> Result<ExportLogsServiceResponse, Status> {
        if outcome.accepted == 0 {
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
            // Empty request: nothing was accepted and nothing was rejected.
            return Ok(ExportLogsServiceResponse::default());
        }

        if self.config.durability_mode == DurabilityMode::Commit {
            if let Some(ticket) = outcome.last_ticket {
                // Wait for the contiguous watermark, not just this ticket's
                // own outcome: a later ticket can fail while an earlier
                // split chunk is still buffered, and reporting early would
                // reject records that are about to commit.
                let _ = self.commit.wait_watermark(ticket).await;
            }

            let mut committed = 0u64;
            let mut any_failed = false;
            for (ticket, records) in &outcome.tickets {
                match self.commit.state(*ticket) {
                    TicketState::Completed => committed += records,
                    TicketState::Failed { progress } => {
                        committed += progress.committed;
                        any_failed = true;
                    }
                    TicketState::Unsettled { progress } => {
                        committed += progress.committed;
                    }
                }
            }

            let rejected = outcome.total_records.saturating_sub(committed);
            if rejected == 0 {
                return Ok(ExportLogsServiceResponse::default());
            }
            if any_failed || committed > 0 {
                tracing::warn!(accepted = committed, rejected, "export partially accepted");
                ::metrics::counter!(
                    "otlp_records_rejected_total",
                    "signal" => "logs",
                    "reason" => if any_failed { "quarantined" } else { "not_committed" }
                )
                .increment(rejected);
                return Ok(partial_success(
                    rejected,
                    "storage accepted part of the request; retry only the rejected records",
                ));
            }
            return Err(Status::unavailable(
                "storage pipeline shut down before commit",
            ));
        }

        // Enqueue mode acknowledges at queue entry. A tail that could not be
        // queued is reported as rejected without asking the sender to resend
        // the chunks that were queued.
        if outcome.rejected > 0 {
            tracing::warn!(
                accepted = outcome.accepted,
                rejected = outcome.rejected,
                "ingress queue backpressure"
            );
            ::metrics::counter!(
                "otlp_records_rejected_total",
                "signal" => "logs",
                "reason" => "queue"
            )
            .increment(outcome.rejected);
            return Ok(partial_success(
                outcome.rejected,
                "ingress accepted part of the request; retry only the rejected records",
            ));
        }

        Ok(ExportLogsServiceResponse::default())
    }
}

fn partial_success(rejected: u64, message: &str) -> ExportLogsServiceResponse {
    ExportLogsServiceResponse {
        partial_success: Some(ExportLogsPartialSuccess {
            rejected_log_records: i64::try_from(rejected).unwrap_or(i64::MAX),
            error_message: message.to_owned(),
        }),
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

        let _response = tokio::time::timeout(std::time::Duration::from_secs(2), handle)
            .await
            .expect("response released after commit")
            .expect("handler did not panic")
            .expect("export succeeds");
        assert_eq!(
            ledger.watermark().committed_through,
            1,
            "the response must not be released before the ticket committed"
        );
        assert!(started.elapsed() >= std::time::Duration::from_millis(50));
    }

    #[tokio::test]
    async fn mapping_permit_is_released_before_the_durable_ack_waits() {
        let ledger = Arc::new(CommitLedger::new());
        let config = Arc::new(IngressConfig {
            durability_mode: DurabilityMode::Commit,
            ..IngressConfig::default()
        });
        let (queue, receiver) = channel(8);
        let ingress = LogsIngress::new(queue, config, Arc::clone(&ledger)).with_mapping_limit(1);
        let probe = ingress.mapping_budget.clone();

        let handle = tokio::spawn(async move { ingress.handle(request_with(2)).await });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(
            !handle.is_finished(),
            "the request waits for the durable ack"
        );
        assert_eq!(
            probe.available_permits(),
            1,
            "the mapping permit must not be held while waiting for a commit"
        );

        ledger.complete(1);
        tokio::time::timeout(std::time::Duration::from_secs(2), handle)
            .await
            .expect("released after commit")
            .expect("handler did not panic")
            .expect("export succeeds");
        drop(receiver);
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

        let _response = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            ingress.handle(request_with(2)),
        )
        .await
        .expect("enqueue mode returns immediately")
        .expect("export succeeds");
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
        // Two of the three ingest slots are already occupied, so this
        // request's two chunks can never both fit right now. That is
        // transient backpressure (a drain could make room), not a permanent
        // mapping error, so it stays retryable UNAVAILABLE.
        let (queue, receiver) = channel(3);
        queue
            .send(otel_sqlite_core::storage::IngestMessage::Flush)
            .expect("prefill");
        queue
            .send(otel_sqlite_core::storage::IngestMessage::Flush)
            .expect("prefill");
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
    async fn invalid_trace_id_is_accepted_with_zeroed_id() {
        let ledger = Arc::new(CommitLedger::new());
        let config = Arc::new(IngressConfig {
            durability_mode: DurabilityMode::Enqueue,
            ..IngressConfig::default()
        });
        let (queue, receiver) = channel(4);
        let ingress = LogsIngress::new(queue, config, Arc::clone(&ledger));

        // A wrong-length trace id carries no trace association: it must be
        // zeroed and counted, never reject the request (OTLP SHOULD).
        let request = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                scope_logs: vec![ScopeLogs {
                    log_records: vec![ProtoLogRecord {
                        trace_id: vec![1, 2],
                        span_id: vec![9; 8],
                        ..ProtoLogRecord::default()
                    }],
                    ..ScopeLogs::default()
                }],
                ..ResourceLogs::default()
            }],
        };

        let _response =
            tokio::time::timeout(std::time::Duration::from_secs(2), ingress.handle(request))
                .await
                .expect("invalid ids must not stall the export")
                .expect("invalid ids must not reject the request");
        let message = receiver.recv().expect("record enqueued");
        let otel_sqlite_core::storage::IngestMessage::Logs(chunk) = message else {
            panic!("expected logs chunk");
        };
        assert_eq!(chunk.records[0].trace_id, [0; 16]);
        assert_eq!(chunk.records[0].span_id, [9; 8]);
    }

    fn outcome(accepted: u64, rejected: u64, tickets: &[(u64, u64)]) -> EnqueueOutcome {
        EnqueueOutcome {
            accepted,
            rejected,
            total_records: accepted + rejected,
            last_ticket: tickets.last().map(|(ticket, _)| *ticket),
            tickets: tickets.to_vec(),
            ..EnqueueOutcome::default()
        }
    }

    #[tokio::test]
    async fn partial_admission_reports_committed_records_as_partial_success() {
        let ledger = Arc::new(CommitLedger::new());
        let (ingress, _) = ingress_with(8, DurabilityMode::Commit, Arc::clone(&ledger));
        ledger.complete(1);

        let response = ingress
            .finish(outcome(1, 1, &[(1, 1)]))
            .await
            .expect("a request with a committed chunk must not ask for a full retry");
        let partial = response.partial_success.expect("partial success");
        assert_eq!(partial.rejected_log_records, 1);
    }

    #[tokio::test]
    async fn partial_commit_reports_only_uncommitted_records() {
        let ledger = Arc::new(CommitLedger::new());
        let (ingress, _) = ingress_with(8, DurabilityMode::Commit, Arc::clone(&ledger));
        ledger.complete(1);
        ledger.close();

        let response = ingress
            .finish(outcome(2, 0, &[(1, 1), (2, 1)]))
            .await
            .expect("committed records must not be resent");
        let partial = response.partial_success.expect("partial success");
        assert_eq!(partial.rejected_log_records, 1);
    }

    #[tokio::test]
    async fn quarantine_reports_dropped_records_as_partial_success() {
        let ledger = Arc::new(CommitLedger::new());
        let (ingress, _) = ingress_with(8, DurabilityMode::Commit, Arc::clone(&ledger));
        ledger.note_committed(1, 1);
        ledger.note_dropped(1, 1);
        ledger.complete(1);

        let response = ingress
            .finish(outcome(2, 0, &[(1, 2)]))
            .await
            .expect("a quarantined ticket is a completed request, not a retryable failure");
        let partial = response.partial_success.expect("partial success");
        assert_eq!(partial.rejected_log_records, 1);
    }

    #[tokio::test]
    async fn no_durable_records_stays_retryable() {
        let ledger = Arc::new(CommitLedger::new());
        let (ingress, _) = ingress_with(8, DurabilityMode::Commit, Arc::clone(&ledger));
        ledger.close();

        let status = ingress
            .finish(outcome(1, 1, &[(1, 1)]))
            .await
            .expect_err("nothing committed must stay retryable");
        assert_eq!(status.code(), tonic::Code::Unavailable);
    }

    #[tokio::test]
    async fn out_of_order_failure_waits_for_the_earlier_ticket() {
        let ledger = Arc::new(CommitLedger::new());
        let (ingress, _) = ingress_with(8, DurabilityMode::Commit, Arc::clone(&ledger));
        ledger.note_dropped(2, 1);
        ledger.complete(2);

        let handle =
            tokio::spawn(async move { ingress.finish(outcome(2, 0, &[(1, 1), (2, 1)])).await });
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert!(
            !handle.is_finished(),
            "the handler must wait for ticket 1 before reporting ticket 2's failure"
        );

        ledger.complete(1);
        let response = tokio::time::timeout(std::time::Duration::from_secs(2), handle)
            .await
            .expect("handler finishes after the gap closes")
            .expect("no panic")
            .expect("partial success");
        let partial = response.partial_success.expect("partial success");
        assert_eq!(partial.rejected_log_records, 1);
    }

    #[tokio::test]
    async fn enqueue_mode_reports_a_rejected_tail_as_partial_success() {
        let ledger = Arc::new(CommitLedger::new());
        let (ingress, _) = ingress_with(8, DurabilityMode::Enqueue, ledger);

        let response = ingress
            .finish(outcome(1, 1, &[(1, 1)]))
            .await
            .expect("queued records must not be resent");
        let partial = response.partial_success.expect("partial success");
        assert_eq!(partial.rejected_log_records, 1);
    }
}
