//! Load execution mechanics: pacing, worker loops, sampling and drain.
//!
//! One scenario step runs `clients` concurrent [`worker_loop`]s against the
//! shared [`Pacer`], while an optional one-second [`sampler_task`] records
//! gauge and file-size samples. After producers stop, [`drain_storage`]
//! flushes the pipeline (embedded mode) and polls SQLite until row counts
//! stabilize.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde::Serialize;
use tokio::time::Instant as TokioInstant;

use crate::accounting::RunAccounting;
use crate::client::{ClientCounters, LatencyHistogram, LoadClient, OutcomeKind, SequenceAllocator};
use crate::db;
use crate::generator::WorkloadSpec;
use crate::ledger::Ledger;
use crate::metrics::TelemetryHandle;
use crate::pacer::Pacer;
use crate::params::{RunContext, StepParams};

/// Maximum consecutive transport-classified (`Ambiguous`) requests tolerated
/// before a worker concludes the endpoint is gone. Definitive server
/// verdicts — including queue-full backpressure (`UNAVAILABLE` from the
/// admission gate) and `InvalidArgument` — never count: the server answered,
/// so it is demonstrably alive.
const WORKER_FAILURE_ABORT: u32 = 25;

/// Whether a worker aborts after repeated transport failures.
///
/// Benchmark steps treat a lost endpoint as fatal. Crash tests deliberately
/// kill the endpoint mid-run, so every request must still be classified
/// rather than aborting the driver.
#[derive(Debug, Clone, Copy)]
pub(crate) enum TransportFailurePolicy {
    Abort,
    Tolerate,
}

#[derive(Clone)]
pub(crate) struct SamplingConfig {
    pub telemetry: TelemetryHandle,
    pub db_path: PathBuf,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct SamplePoint {
    pub elapsed_secs: f64,
    pub gauges: std::collections::BTreeMap<String, f64>,
    pub database_bytes: u64,
    pub wal_bytes: u64,
}

async fn sampler_task(
    telemetry: TelemetryHandle,
    db_path: PathBuf,
    deadline: TokioInstant,
    sink: Arc<Mutex<Vec<SamplePoint>>>,
) {
    let started = TokioInstant::now();
    let mut ticker = tokio::time::interval(Duration::from_secs(1));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        ticker.tick().await;
        if TokioInstant::now() >= deadline {
            break;
        }
        sink.lock().expect("sample sink lock").push(SamplePoint {
            elapsed_secs: started.elapsed().as_secs_f64(),
            gauges: telemetry.sample_gauges(),
            database_bytes: otel_sqlite_test_support::file_len(&db_path),
            wal_bytes: otel_sqlite_test_support::file_len(&otel_sqlite_test_support::wal_path(
                &db_path,
            )),
        });
    }
}

/// Everything one load driver run needs. The two callers (benchmark steps and
/// crash recovery) differ only in `transport_failures` and `sampling`.
pub(crate) struct LoadRequest {
    pub endpoint: String,
    pub spec: Arc<WorkloadSpec>,
    pub clients: usize,
    pub records_per_request: usize,
    pub offered_records_per_second: Option<u64>,
    pub duration: Duration,
    pub request_timeout: Duration,
    pub transport_failures: TransportFailurePolicy,
    pub sampling: Option<SamplingConfig>,
}

#[derive(Debug)]
pub(crate) struct LoadRun {
    pub accounting: RunAccounting,
    pub latency_us: hdrhistogram::Histogram<u64>,
    pub samples: Vec<SamplePoint>,
    pub measured_seconds: f64,
}

#[allow(clippy::too_many_arguments)]
async fn worker_loop(
    endpoint: String,
    spec: Arc<WorkloadSpec>,
    allocator: Arc<SequenceAllocator>,
    records_per_request: usize,
    pacer: Arc<Pacer>,
    ledger: Arc<Ledger>,
    latencies: Arc<LatencyHistogram>,
    counters: Arc<ClientCounters>,
    request_timeout: Duration,
    transport_failures: TransportFailurePolicy,
    deadline: TokioInstant,
) -> anyhow::Result<()> {
    let mut client = LoadClient::connect(
        &endpoint,
        spec,
        allocator,
        records_per_request,
        request_timeout,
        latencies,
        counters,
    )
    .await?;

    let mut consecutive_failures = 0u32;
    while TokioInstant::now() < deadline {
        pacer.acquire().await;
        if TokioInstant::now() >= deadline {
            break;
        }

        let result = client.send_one().await;
        match result.kind {
            // Only transport-classified outcomes indicate a lost endpoint;
            // any answer from the server (accepted, partially rejected or
            // definitively rejected) resets the counter.
            OutcomeKind::Ambiguous => {
                if matches!(transport_failures, TransportFailurePolicy::Abort) {
                    consecutive_failures += 1;
                    if consecutive_failures >= WORKER_FAILURE_ABORT {
                        anyhow::bail!(
                            "worker aborted after {consecutive_failures} consecutive \
                             transport-failed/ambiguous requests"
                        );
                    }
                }
            }
            OutcomeKind::AllAccepted
            | OutcomeKind::PartiallyRejected { .. }
            | OutcomeKind::Rejected => {
                consecutive_failures = 0;
            }
        }
        ledger.record(&result);
    }
    Ok(())
}

/// Single load driver for benchmark steps and crash recovery.
///
/// Every spawned worker is joined before this returns, so no task can mutate
/// the counters or ledger after the accounting value is frozen. The sampler
/// is stopped and joined the same way before its samples are read.
pub(crate) async fn run_load(request: LoadRequest) -> anyhow::Result<LoadRun> {
    let LoadRequest {
        endpoint,
        spec,
        clients,
        records_per_request,
        offered_records_per_second,
        duration,
        request_timeout,
        transport_failures,
        sampling,
    } = request;

    let allocator = Arc::new(SequenceAllocator::new(0));
    let ledger = Arc::new(Ledger::new());
    let latencies = Arc::new(LatencyHistogram::new());
    let counters = Arc::new(ClientCounters::default());

    let offered_request_rate = offered_records_per_second
        .map(|records_per_sec| records_per_sec as f64 / records_per_request as f64);
    let pacer = Arc::new(Pacer::new(offered_request_rate));

    let deadline = TokioInstant::now() + duration;
    let samples = Arc::new(Mutex::new(Vec::new()));
    let sampler_handle = sampling.map(|sampling| {
        tokio::spawn(sampler_task(
            sampling.telemetry,
            sampling.db_path,
            deadline,
            Arc::clone(&samples),
        ))
    });

    let started = Instant::now();
    let mut workers = Vec::with_capacity(clients);
    for _ in 0..clients {
        workers.push(tokio::spawn(worker_loop(
            endpoint.clone(),
            Arc::clone(&spec),
            Arc::clone(&allocator),
            records_per_request,
            Arc::clone(&pacer),
            Arc::clone(&ledger),
            Arc::clone(&latencies),
            Arc::clone(&counters),
            request_timeout,
            transport_failures,
            deadline,
        )));
    }

    let mut first_worker_error: Option<anyhow::Error> = None;
    for worker in workers {
        match worker.await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                if first_worker_error.is_none() {
                    first_worker_error = Some(error);
                }
            }
            Err(join_error) if join_error.is_cancelled() => {}
            Err(join_error) => {
                if first_worker_error.is_none() {
                    first_worker_error = Some(anyhow::Error::new(join_error));
                }
            }
        }
    }
    let measured_seconds = started.elapsed().as_secs_f64().max(f64::EPSILON);

    if let Some(handle) = sampler_handle {
        handle.abort();
        let _ = handle.await;
    }
    let samples = Arc::into_inner(samples)
        .map_or_default(|sink| sink.into_inner().expect("sample sink lock"));

    if let Some(error) = first_worker_error {
        return Err(error.context(format!("load against '{endpoint}' lost a producer")));
    }

    let snapshot = counters.snapshot();
    let (rejected, ambiguous, positions_exact) = ledger.freeze();
    let accounting = RunAccounting::new(snapshot, rejected, ambiguous, positions_exact)?;

    Ok(LoadRun {
        accounting,
        latency_us: latencies.snapshot().await,
        samples,
        measured_seconds,
    })
}

pub(crate) async fn execute_load(
    context: &RunContext,
    params: &StepParams,
    run_id: &str,
) -> anyhow::Result<LoadRun> {
    let spec = Arc::new(WorkloadSpec {
        run_id: run_id.to_owned(),
        seed: context.seed,
        body_size: context.body_size,
        attributes_per_record: context.attributes_per_record,
        resource_count: params.resource_count.unwrap_or(context.resource_count),
    });

    run_load(LoadRequest {
        endpoint: context.endpoint.clone(),
        spec,
        clients: params.clients,
        records_per_request: params.records_per_request,
        offered_records_per_second: params.offered_records_per_second,
        duration: Duration::from_secs(params.duration_secs),
        request_timeout: context.request_timeout,
        transport_failures: TransportFailurePolicy::Abort,
        sampling: Some(SamplingConfig {
            telemetry: context.telemetry.clone(),
            db_path: context.db_path.clone(),
        }),
    })
    .await
}

/// Order barrier plus stability poll: everything accepted must be persisted
/// before validation opens the database.
///
/// The whole phase shares ONE budget (`RunContext::drain_timeout`): the flush
/// barrier consumes whatever it needs from the front of it, the stability poll
/// gets what remains. Both halves are bounded and error-propagating — a dead
/// or wedged pipeline fails the step here with a named cause instead of
/// hanging forever or degrading into downstream validation noise.
pub(crate) async fn drain_storage(
    context: &RunContext,
    run_id: &str,
    expected_rows: Option<u64>,
) -> anyhow::Result<u64> {
    use anyhow::Context as _;

    let started = std::time::Instant::now();
    let total_budget = context.drain_timeout;
    let elapsed = || started.elapsed();

    if let Some(server) = &context.server {
        let barrier_budget = total_budget
            .saturating_sub(elapsed())
            .max(std::time::Duration::from_secs(1));
        let barrier_server = Arc::clone(server);
        // Blocking channel send: must not sit on a tokio worker thread.
        tokio::task::spawn_blocking(move || barrier_server.flush(barrier_budget))
            .await
            .map_err(|error| anyhow::anyhow!("flush task panicked: {error}"))?
            .with_context(|| {
                format!(
                    "step '{run_id}': flush barrier failed ({:.1}s into the {:.1}s drain budget)",
                    elapsed().as_secs_f64(),
                    total_budget.as_secs_f64(),
                )
            })?;

        verify_pipeline_healthy(server).map_err(|error| {
            anyhow::anyhow!(
                "step '{run_id}': storage pipeline unhealthy after flush barrier: {error:#}"
            )
        })?;
    }

    let db_path = context.db_path.clone();
    let run_id_owned = run_id.to_owned();
    let remaining = total_budget.saturating_sub(elapsed());
    let drained = tokio::task::spawn_blocking(move || {
        db::wait_until_drained(&db_path, &run_id_owned, expected_rows, remaining)
    })
    .await?
    .with_context(|| {
        format!(
            "step '{run_id}': drain poll failed ({:.1}s into the {:.1}s drain budget)",
            elapsed().as_secs_f64(),
            total_budget.as_secs_f64(),
        )
    })?;

    if let Some(server) = &context.server {
        verify_pipeline_healthy(server).map_err(|error| {
            anyhow::anyhow!("step '{run_id}': storage pipeline unhealthy after drain: {error:#}")
        })?;
    }
    Ok(drained)
}

/// Fails when a storage worker exited prematurely or recorded losses.
///
/// During a live session both threads must be running; either flag being
/// clear means the thread died mid-run (SQL error, panic) and every record
/// still upstream of it is stranded. Data loss is `quarantined_records` +
/// `dropped_records`; `errors` also counts retryable `BUSY`/`LOCKED`
/// failures that later succeeded, so it is reported as advisory evidence
/// rather than a failure.
fn verify_pipeline_healthy(server: &crate::server::EmbeddedServer) -> anyhow::Result<()> {
    if let Some(sample) = server.health_sample()
        && (!sample.writer_running || !sample.batcher_running)
    {
        let stats = server.stats();
        anyhow::bail!(
            "storage worker exited prematurely (writer_running={}, batcher_running={}, \
             records_written={}, batches_emitted={}, dropped_records={}, errors={})",
            sample.writer_running,
            sample.batcher_running,
            stats.map_or(0, |s| s.records_written),
            stats.map_or(0, |s| s.write_batches_emitted),
            stats.map_or(0, |s| s.dropped_records),
            stats.map_or(0, |s| s.errors),
        );
    }
    if let Some(stats) = server.stats() {
        if stats.quarantined_records > 0 || stats.dropped_records > 0 {
            anyhow::bail!(
                "storage pipeline lost data (quarantined_records={}, dropped_records={}, errors={})",
                stats.quarantined_records,
                stats.dropped_records,
                stats.errors,
            );
        }
        if stats.errors > 0 {
            tracing::warn!(
                errors = stats.errors,
                transient_retries = stats.transient_retries,
                "storage reported errors (retried transient failures included); \
                 loss counters are clean"
            );
        }
    }
    Ok(())
}
