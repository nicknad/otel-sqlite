//! OTLP/gRPC benchmark client.
//!
//! Latency measured here is **request latency**: the gRPC round-trip from the
//! client's perspective, including server-side decode/validate/map/enqueue.
//! It is *not* persistence latency: an OK response means records were accepted
//! into the bounded ingress queue (or partially rejected via OTLP
//! `partial_success` backpressure), not durably committed to SQLite.
//!
//! Outcome classification for correctness validation:
//!
//! * `AllAccepted` / `PartiallyRejected` - authoritative counts reported by
//!   the server.
//! * `Rejected`                          - definitively not enqueued
//!   (`InvalidArgument` / `Unavailable`).
//! * `Ambiguous`                         - timeout or transport failure; the
//!   records may still have been enqueued before the failure, so validation
//!   must tolerate them being either present or absent.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use hdrhistogram::Histogram;
use otel_sqlite_ingress::mapping::pb::collector::logs::v1::ExportLogsServiceRequest;
use otel_sqlite_ingress::mapping::pb::collector::logs::v1::logs_service_client::LogsServiceClient;
use tonic::transport::Channel;

use crate::generator::WorkloadSpec;

/// Globally unique, contiguous sequence allocation across all workers.
#[derive(Debug)]
pub struct SequenceAllocator {
    next: AtomicU64,
}

impl SequenceAllocator {
    pub fn new(start: u64) -> Self {
        Self {
            next: AtomicU64::new(start),
        }
    }

    /// Reserve a contiguous chunk; returns `(first_seq, count)`.
    pub fn allocate(&self, count: usize) -> (u64, usize) {
        let first = self.next.fetch_add(count as u64, Ordering::Relaxed);
        (first + 1, count)
    }
}

/// Classification of one export request. `records` always covers the full
/// contiguous chunk `[start_seq, start_seq + records)` handed to this request;
/// every record falls into exactly one bucket below.
#[derive(Debug, Clone, Copy)]
pub enum OutcomeKind {
    /// Server acknowledged every record without rejections.
    AllAccepted,
    /// Server reported rejections through OTLP `partial_success`.
    PartiallyRejected { accepted: u64, rejected: u64 },
    /// Definitively not enqueued (queue disconnected / invalid request).
    Rejected,
    /// Unknown fate: timed out or failed before a definitive answer.
    Ambiguous,
}

#[derive(Debug, Clone, Copy)]
pub struct RequestResult {
    pub start_seq: u64,
    pub records: usize,
    pub kind: OutcomeKind,
}

#[derive(Debug, Default)]
pub struct ClientCounters {
    pub requests: AtomicU64,
    pub records_generated: AtomicU64,
    pub records_accepted: AtomicU64,
    pub records_rejected: AtomicU64,
    pub records_ambiguous: AtomicU64,
    pub requests_failed: AtomicU64,
    pub requests_timed_out: AtomicU64,
}

impl ClientCounters {
    pub fn snapshot(&self) -> ClientCountersSnapshot {
        ClientCountersSnapshot {
            requests: self.requests.load(Ordering::Relaxed),
            records_generated: self.records_generated.load(Ordering::Relaxed),
            records_accepted: self.records_accepted.load(Ordering::Relaxed),
            records_rejected: self.records_rejected.load(Ordering::Relaxed),
            records_ambiguous: self.records_ambiguous.load(Ordering::Relaxed),
            requests_failed: self.requests_failed.load(Ordering::Relaxed),
            requests_timed_out: self.requests_timed_out.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, serde::Serialize)]
pub struct ClientCountersSnapshot {
    pub requests: u64,
    pub records_generated: u64,
    pub records_accepted: u64,
    pub records_rejected: u64,
    pub records_ambiguous: u64,
    pub requests_failed: u64,
    pub requests_timed_out: u64,
}

/// Shared client-side request-latency histogram, microseconds.
#[derive(Debug)]
pub struct LatencyHistogram {
    inner: tokio::sync::Mutex<Histogram<u64>>,
}

impl Default for LatencyHistogram {
    fn default() -> Self {
        Self::new()
    }
}

impl LatencyHistogram {
    pub fn new() -> Self {
        Self {
            inner: tokio::sync::Mutex::new(Self::blank()),
        }
    }

    fn blank() -> Histogram<u64> {
        Histogram::<u64>::new_with_max(3_600_000_000, 3).expect("valid histogram configuration")
    }

    pub async fn record_us(&self, micros: u64) {
        self.inner.lock().await.saturating_record(micros);
    }

    pub async fn snapshot(&self) -> Histogram<u64> {
        self.inner.lock().await.clone()
    }
}

#[derive(Debug, Clone)]
pub struct LoadClient {
    stub: LogsServiceClient<Channel>,
    spec: Arc<WorkloadSpec>,
    allocator: Arc<SequenceAllocator>,
    records_per_request: usize,
    request_timeout: Duration,
    latencies: Arc<LatencyHistogram>,
    counters: Arc<ClientCounters>,
}

impl LoadClient {
    #[allow(clippy::too_many_arguments)]
    pub async fn connect(
        endpoint: &str,
        spec: Arc<WorkloadSpec>,
        allocator: Arc<SequenceAllocator>,
        records_per_request: usize,
        request_timeout: Duration,
        latencies: Arc<LatencyHistogram>,
        counters: Arc<ClientCounters>,
    ) -> anyhow::Result<Self> {
        let stub = LogsServiceClient::connect(endpoint.to_owned())
            .await?
            .max_decoding_message_size(16 * 1024 * 1024);
        Ok(Self {
            stub,
            spec,
            allocator,
            records_per_request,
            request_timeout,
            latencies,
            counters,
        })
    }

    /// Generate, send and classify exactly one request.
    pub async fn send_one(&mut self) -> RequestResult {
        let (start_seq, count) = self.allocator.allocate(self.records_per_request);
        let request: ExportLogsServiceRequest = self.spec.render_request(start_seq, count);

        self.counters.requests.fetch_add(1, Ordering::Relaxed);
        self.counters
            .records_generated
            .fetch_add(count as u64, Ordering::Relaxed);

        let started = Instant::now();
        let kind = match tokio::time::timeout(
            self.request_timeout,
            self.stub.export(tonic::Request::new(request)),
        )
        .await
        {
            Err(_elapsed) => {
                self.counters
                    .requests_timed_out
                    .fetch_add(1, Ordering::Relaxed);
                self.counters
                    .records_ambiguous
                    .fetch_add(count as u64, Ordering::Relaxed);
                OutcomeKind::Ambiguous
            }
            Ok(Err(status)) => match status.code() {
                tonic::Code::InvalidArgument | tonic::Code::Unavailable => {
                    self.counters
                        .requests_failed
                        .fetch_add(1, Ordering::Relaxed);
                    self.counters
                        .records_rejected
                        .fetch_add(count as u64, Ordering::Relaxed);
                    OutcomeKind::Rejected
                }
                _ => {
                    self.counters
                        .requests_failed
                        .fetch_add(1, Ordering::Relaxed);
                    self.counters
                        .records_ambiguous
                        .fetch_add(count as u64, Ordering::Relaxed);
                    OutcomeKind::Ambiguous
                }
            },
            Ok(Ok(response)) => {
                let body = response.into_inner();
                match body.partial_success {
                    Some(partial) if partial.rejected_log_records > 0 => {
                        let rejected = partial.rejected_log_records.max(0) as u64;
                        let accepted = (count as i64 - partial.rejected_log_records)
                            .clamp(0, count as i64) as u64;
                        self.counters
                            .records_rejected
                            .fetch_add(rejected, Ordering::Relaxed);
                        self.counters
                            .records_accepted
                            .fetch_add(accepted, Ordering::Relaxed);
                        OutcomeKind::PartiallyRejected { accepted, rejected }
                    }
                    _ => {
                        self.counters
                            .records_accepted
                            .fetch_add(count as u64, Ordering::Relaxed);
                        OutcomeKind::AllAccepted
                    }
                }
            }
        };

        self.latencies
            .record_us(u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX))
            .await;

        RequestResult {
            start_seq,
            records: count,
            kind,
        }
    }
}
