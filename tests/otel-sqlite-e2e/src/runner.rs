//! Load execution mechanics: pacing, worker loops, sampling and drain.
//!
//! One scenario step runs `clients` concurrent [`worker_loop`]s against the
//! shared [`Pacer`], while a one-second [`sampler_task`] records gauge and
//! file-size samples. After producers stop, [`drain_storage`] flushes the
//! pipeline (embedded mode) and polls SQLite until row counts stabilize.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde::Serialize;
use tokio::time::Instant as TokioInstant;

use crate::client::{
    ClientCounters, ClientCountersSnapshot, LatencyHistogram, LoadClient, OutcomeKind,
    SequenceAllocator,
};
use crate::db;
use crate::generator::WorkloadSpec;
use crate::interval_set::IntervalSet;
use crate::ledger::Ledger;
use crate::metrics::TelemetryHandle;
use crate::pacer::Pacer;
use crate::params::{RunContext, StepParams};

/// Maximum consecutive failed/ambiguous requests tolerated before a worker
/// concludes the endpoint is gone. Legitimate backpressure outcomes
/// (`partial_success`) do not count towards this.
const WORKER_FAILURE_ABORT: u32 = 25;

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
            database_bytes: file_len(&db_path),
            wal_bytes: file_len(&db_path.with_extension("db-wal")),
        });
    }
}

fn file_len(path: &std::path::Path) -> u64 {
    std::fs::metadata(path).map_or(0, |meta| meta.len())
}

#[derive(Debug)]
pub(crate) struct StepRaw {
    pub counters: ClientCountersSnapshot,
    pub rejected: IntervalSet,
    pub ambiguous: IntervalSet,
    pub positions_exact: bool,
    pub latency_us: hdrhistogram::Histogram<u64>,
    pub samples: Arc<Mutex<Vec<SamplePoint>>>,
    pub measured_seconds: f64,
}

#[allow(clippy::too_many_arguments)]
async fn worker_loop(
    context: RunContext,
    spec: Arc<WorkloadSpec>,
    allocator: Arc<SequenceAllocator>,
    records_per_request: usize,
    pacer: Arc<Pacer>,
    ledger: Arc<Ledger>,
    latencies: Arc<LatencyHistogram>,
    counters: Arc<ClientCounters>,
    deadline: TokioInstant,
) -> anyhow::Result<()> {
    let mut client = LoadClient::connect(
        &context.endpoint,
        spec,
        allocator,
        records_per_request,
        context.request_timeout,
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
            OutcomeKind::AllAccepted | OutcomeKind::PartiallyRejected { .. } => {
                consecutive_failures = 0;
            }
            _ => {
                consecutive_failures += 1;
                if consecutive_failures >= WORKER_FAILURE_ABORT {
                    anyhow::bail!(
                        "worker aborted after {consecutive_failures} consecutive failed/ambiguous requests"
                    );
                }
            }
        }
        ledger.record(&result);
    }
    Ok(())
}

pub(crate) async fn execute_load(
    context: &RunContext,
    params: &StepParams,
    run_id: &str,
) -> anyhow::Result<StepRaw> {
    let spec = Arc::new(WorkloadSpec {
        run_id: run_id.to_owned(),
        seed: context.seed,
        body_size: context.body_size,
        attributes_per_record: context.attributes_per_record,
        resource_count: params.resource_count.unwrap_or(context.resource_count),
    });
    let allocator = Arc::new(SequenceAllocator::new(0));
    let ledger = Arc::new(Ledger::new());
    let latencies = Arc::new(LatencyHistogram::new());
    let counters = Arc::new(ClientCounters::default());

    let offered_request_rate = params
        .offered_records_per_second
        .map(|records_per_sec| records_per_sec as f64 / params.records_per_request as f64);
    let pacer = Arc::new(Pacer::new(offered_request_rate));

    let duration = Duration::from_secs(params.duration_secs);
    let deadline = TokioInstant::now() + duration;
    let samples = Arc::new(Mutex::new(Vec::new()));
    let sampler_handle = tokio::spawn(sampler_task(
        context.telemetry.clone(),
        context.db_path.clone(),
        deadline,
        Arc::clone(&samples),
    ));

    let started = Instant::now();
    let mut workers = Vec::with_capacity(params.clients);
    for _ in 0..params.clients {
        workers.push(tokio::spawn(worker_loop(
            context.clone(),
            Arc::clone(&spec),
            Arc::clone(&allocator),
            params.records_per_request,
            Arc::clone(&pacer),
            Arc::clone(&ledger),
            Arc::clone(&latencies),
            Arc::clone(&counters),
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
    sampler_handle.abort();

    if let Some(error) = first_worker_error {
        return Err(error.context(format!("step '{run_id}' lost a producer")));
    }

    let (rejected, ambiguous, positions_exact) = ledger.freeze();
    Ok(StepRaw {
        counters: counters.snapshot(),
        rejected,
        ambiguous,
        positions_exact,
        latency_us: latencies.snapshot().await,
        samples,
        measured_seconds,
    })
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
/// still upstream of it is stranded. `dropped_records`/`errors` are
/// cumulative process-wide counters that must stay zero in any valid run.
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
    if let Some(stats) = server.stats()
        && (stats.errors > 0 || stats.dropped_records > 0)
    {
        anyhow::bail!(
            "storage pipeline lost data (errors={}, dropped_records={})",
            stats.errors,
            stats.dropped_records,
        );
    }
    Ok(())
}
