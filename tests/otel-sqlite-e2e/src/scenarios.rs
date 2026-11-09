//! Benchmark scenarios and the scenario runner.
//!
//! Every scenario follows the same lifecycle:
//!
//! ```text
//! warm-up (own run_id, excluded from validation)
//!     -> drain queue so measurement starts clean
//!     -> measurement window (telemetry window open, sampler running)
//!     -> stop producers
//!     -> drain storage (Flush command + poll until stable)
//!     -> validate SQLite contents against generated sequences
//! ```
//!
//! Supporting mechanics live in sibling modules: parameters in `params`,
//! pacing in `pacer`, outcome accounting in `ledger`, load execution in
//! `runner`, report types in `report`.

// Binaries report progress and results on stdout by design.
#![allow(clippy::print_stdout, clippy::print_stderr)]

use anyhow::bail;
use clap::ValueEnum;

use crate::db;
use crate::params::{Overrides, RunContext, StepParams, fresh_run_id};
use crate::report::{
    LatencyPercentiles, QueueObservations, ResultsSummary, ScenarioReport, SizeTrajectory,
    StepReport, StorageDeltas, WorkloadSummary, print_step_summary,
};
use crate::runner::{drain_storage, execute_load};
use crate::validation;

// ---------------------------------------------------------------------------
// Scenario registry
// ---------------------------------------------------------------------------

/// One stop of a scenario plan: the canonical scenario plus the step list its
/// builder produced.
#[derive(Debug, Clone)]
pub struct ScenarioPlan {
    pub scenario: Scenario,
    pub name: &'static str,
    pub steps: Vec<StepParams>,
    pub stop_on_saturation: bool,
}

/// The complete registry: every scenario's canonical CLI/report name, step
/// builder and saturation policy lives in this one enum.
#[derive(Copy, Clone, Debug, PartialEq, Eq, ValueEnum)]
pub enum Scenario {
    /// A - small controlled workload.
    #[value(name = "baseline")]
    Baseline,
    /// B - progressively higher offered load.
    #[value(name = "throughput-ramp", alias = "throughput")]
    ThroughputRamp,
    /// C - records-per-request comparison.
    #[value(name = "batch-sizes", alias = "batch")]
    BatchSizes,
    /// D - client-count comparison at constant offered load.
    #[value(name = "concurrency")]
    Concurrency,
    /// E - long-running sustained load (default 10 minutes).
    #[value(name = "sustained")]
    Sustained,
    /// F - intentional overload / backpressure verification.
    #[value(name = "backpressure")]
    Backpressure,
    /// G - storage insert-batcher behaviour across OTLP batch shapes.
    #[value(name = "insert-batching", alias = "insertbatching")]
    InsertBatching,
    /// Everything except `sustained`.
    #[value(name = "all")]
    All,
}

impl Scenario {
    /// `all` expands to these in order; `sustained` is intentionally absent.
    pub const ALL: &'static [Scenario] = &[
        Scenario::Baseline,
        Scenario::ThroughputRamp,
        Scenario::BatchSizes,
        Scenario::Concurrency,
        Scenario::Backpressure,
        Scenario::InsertBatching,
    ];

    pub const fn name(self) -> &'static str {
        match self {
            Self::Baseline => "baseline",
            Self::ThroughputRamp => "throughput-ramp",
            Self::BatchSizes => "batch-sizes",
            Self::Concurrency => "concurrency",
            Self::Sustained => "sustained",
            Self::Backpressure => "backpressure",
            Self::InsertBatching => "insert-batching",
            Self::All => "all",
        }
    }

    /// Whether this scenario stops its own steps at saturation. The runtime
    /// `--keep-going` flag can still override it.
    pub const fn stop_on_saturation(self) -> bool {
        matches!(self, Self::ThroughputRamp)
    }

    pub fn steps(self, overrides: &Overrides) -> Vec<StepParams> {
        match self {
            Self::Baseline => baseline(overrides),
            Self::ThroughputRamp => throughput_ramp(overrides),
            Self::BatchSizes => batch_sizes(overrides),
            Self::Concurrency => concurrency(overrides),
            Self::Sustained => sustained(overrides),
            Self::Backpressure => backpressure(overrides),
            Self::InsertBatching => insert_batching(overrides),
            Self::All => Self::ALL
                .iter()
                .flat_map(|scenario| scenario.steps(overrides))
                .collect(),
        }
    }

    /// Expands `all` into the scenarios to run, preserving canonical names.
    pub fn plan(self, overrides: &Overrides, keep_going: bool) -> Vec<ScenarioPlan> {
        let scenarios: &[Scenario] = if self == Self::All {
            Self::ALL
        } else {
            std::slice::from_ref(&self)
        };
        scenarios
            .iter()
            .copied()
            .map(|scenario| ScenarioPlan {
                scenario,
                name: scenario.name(),
                steps: scenario.steps(overrides),
                stop_on_saturation: scenario.stop_on_saturation() && !keep_going,
            })
            .collect()
    }
}

impl std::fmt::Display for Scenario {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

// ---------------------------------------------------------------------------
// Scenario definitions (A-G)
// ---------------------------------------------------------------------------

/// Scenario A - small controlled workload for correctness and instrumentation
/// verification.
pub fn baseline(overrides: &Overrides) -> Vec<StepParams> {
    vec![StepParams {
        label: "baseline".to_owned(),
        clients: overrides.clients.unwrap_or(1),
        records_per_request: overrides.records_per_request.unwrap_or(100),
        offered_records_per_second: Some(overrides.offered_records_per_second.unwrap_or(1_000)),
        duration_secs: overrides.duration_secs.unwrap_or(60),
        resource_count: None,
    }]
}

/// Scenario B - progressively higher offered load. The runner stops at
/// saturation unless configured otherwise.
pub fn throughput_ramp(overrides: &Overrides) -> Vec<StepParams> {
    let rates = [1_000u64, 5_000, 10_000, 25_000, 50_000, 100_000];
    let default_clients = overrides.clients.unwrap_or(8);
    let default_duration = overrides.duration_secs.unwrap_or(10);
    let default_rpr = overrides.records_per_request.unwrap_or(500);
    let forced_rate = overrides.offered_records_per_second;

    rates
        .into_iter()
        .enumerate()
        .map(|(index, rate)| StepParams {
            label: format!("throughput-{index}-at-{rate}"),
            clients: default_clients,
            records_per_request: default_rpr,
            offered_records_per_second: Some(forced_rate.unwrap_or(rate)),
            duration_secs: default_duration,
            resource_count: None,
        })
        .collect()
}

/// Scenario C - fixed offered load, varying records-per-request.
pub fn batch_sizes(overrides: &Overrides) -> Vec<StepParams> {
    [1usize, 10, 100, 500, 1000]
        .into_iter()
        .map(|records_per_request| StepParams {
            label: format!("batch-at-{records_per_request}"),
            clients: overrides.clients.unwrap_or(16),
            records_per_request: overrides.records_per_request.unwrap_or(records_per_request),
            offered_records_per_second: Some(
                overrides.offered_records_per_second.unwrap_or(10_000),
            ),
            duration_secs: overrides.duration_secs.unwrap_or(15),
            resource_count: None,
        })
        .collect()
}

/// Scenario D - constant total offered load split over varying client counts
/// (a single shared pacer keeps the offered rate independent of concurrency).
pub fn concurrency(overrides: &Overrides) -> Vec<StepParams> {
    let client_counts: Vec<usize> = overrides
        .clients
        .map_or_else(|| vec![1, 2, 4, 8, 16, 32], |single| vec![single]);
    client_counts
        .into_iter()
        .map(|clients| StepParams {
            label: format!("concurrency-at-{clients}"),
            clients,
            records_per_request: overrides.records_per_request.unwrap_or(100),
            offered_records_per_second: Some(
                overrides.offered_records_per_second.unwrap_or(10_000),
            ),
            duration_secs: overrides.duration_secs.unwrap_or(15),
            resource_count: None,
        })
        .collect()
}

/// Scenario E - long-running load exposing WAL growth, degradation and leaks.
pub fn sustained(overrides: &Overrides) -> Vec<StepParams> {
    vec![StepParams {
        label: "sustained".to_owned(),
        clients: overrides.clients.unwrap_or(4),
        records_per_request: overrides.records_per_request.unwrap_or(500),
        offered_records_per_second: Some(overrides.offered_records_per_second.unwrap_or(20_000)),
        duration_secs: overrides.duration_secs.unwrap_or(600),
        resource_count: None,
    }]
}

/// Scenario F - intentionally overloaded closed-loop run exercising the
/// documented backpressure policy (`UNAVAILABLE` rejections, queue-full
/// events, bounded queues).
pub fn backpressure(overrides: &Overrides) -> Vec<StepParams> {
    vec![StepParams {
        label: "backpressure".to_owned(),
        clients: overrides.clients.unwrap_or(32),
        records_per_request: overrides.records_per_request.unwrap_or(100),
        offered_records_per_second: None,
        duration_secs: overrides.duration_secs.unwrap_or(30),
        resource_count: Some(1),
    }]
}

/// Scenario G - storage insert-batcher behaviour across OTLP batch shapes.
///
/// Exercises the separation between transport batching (OTLP request size)
/// and storage batching (`max_batch_records` / `max_batch_age`):
///
/// * `insert-small`       - OTLP batches far below storage capacity;
/// * `insert-at-capacity` - request record count equal to `max_batch_records`
///   (the request boundary is independent of the storage batch boundary);
/// * `insert-oversized`   - OTLP batches several times over capacity (splits);
/// * `insert-sustained`   - sustained high-volume ingestion (capacity-driven
///   emission dominates);
/// * `insert-low-volume`  - trickle traffic where timer flushes dominate and
///   maximum batching latency is observable.
pub fn insert_batching(overrides: &Overrides) -> Vec<StepParams> {
    [
        ("insert-small", 8usize, 37usize, Some(10_000u64), 15u64),
        (
            "insert-at-capacity",
            8,
            crate::server::benchmark_profile::MAX_BATCH_RECORDS,
            Some(40_000),
            15,
        ),
        ("insert-oversized", 8, 2_500, Some(50_000), 15),
        ("insert-sustained", 8, 700, Some(50_000), 30),
        ("insert-low-volume", 2, 5, Some(20), 20),
    ]
    .into_iter()
    .map(
        |(label, clients, records_per_request, rate, duration)| StepParams {
            label: label.to_owned(),
            clients: overrides.clients.unwrap_or(clients),
            records_per_request: overrides.records_per_request.unwrap_or(records_per_request),
            offered_records_per_second: overrides.offered_records_per_second.or(rate),
            duration_secs: overrides.duration_secs.unwrap_or(duration),
            resource_count: None,
        },
    )
    .collect()
}

// ---------------------------------------------------------------------------
// Scenario runner
// ---------------------------------------------------------------------------

/// Run one full scenario: optional warm-up, then all steps with validation.
///
/// `stop_on_saturation` halts multi-step scenarios once a step shows more
/// than 5% rejected/ambiguous records (used by the throughput ramp).
pub async fn run_scenario(
    context: &RunContext,
    name: &str,
    steps: Vec<StepParams>,
    stop_on_saturation: bool,
) -> anyhow::Result<ScenarioReport> {
    println!("\n=== Scenario: {name} ===");

    if context.warmup_secs > 0 {
        let warmup_params = StepParams {
            label: format!("{name}-warmup"),
            clients: 2,
            records_per_request: 100,
            offered_records_per_second: Some(1_000),
            duration_secs: context.warmup_secs,
            resource_count: None,
        };
        let run_id = fresh_run_id(&warmup_params.label, context.seed);
        println!(
            "warm-up: {}s at 1,000 rec/s (run_id={run_id})",
            warmup_params.duration_secs
        );
        let raw = execute_load(context, &warmup_params, &run_id).await?;
        let accepted = raw.accounting.counters.records_accepted;
        drain_storage(context, &run_id, Some(accepted)).await?;
        println!("warm-up drained ({accepted} rows)");
    }

    let mut reports = Vec::with_capacity(steps.len());
    let mut saturated = false;

    for (index, params) in steps.into_iter().enumerate() {
        if saturated {
            continue;
        }

        let stats_before = context.server.as_ref().and_then(|server| server.stats());
        let run_id = fresh_run_id(&format!("{name}-{index}"), context.seed);
        println!(
            "\n--- step {}: {} (clients={}, rec/req={}, offered={:?}, {}s) ---",
            index + 1,
            params.label,
            params.clients,
            params.records_per_request,
            params.offered_records_per_second,
            params.duration_secs
        );

        context.telemetry.begin_window();
        let raw = match execute_load(context, &params, &run_id).await {
            Ok(raw) => raw,
            Err(error) => {
                // A step that loses its producer cannot produce trustworthy
                // numbers; fail the whole scenario (and the process) instead
                // of reporting partial data.
                context.telemetry.end_window();
                return Err(error.context(format!(
                    "scenario '{name}' step '{}' lost its producer",
                    params.label
                )));
            }
        };
        let window = context.telemetry.end_window();

        let counters = raw.accounting.counters;
        let persisted = drain_storage(context, &run_id, Some(counters.records_accepted)).await?;

        let correctness = {
            let db_path = context.db_path.clone();
            let accounting = raw.accounting.clone();
            let run_id = run_id.clone();
            tokio::task::spawn_blocking(move || {
                validation::validate(&db_path, &run_id, &accounting)
            })
            .await??
        };

        let record_id_mismatches = {
            let db_path = context.db_path.clone();
            let run_id = run_id.clone();
            tokio::task::spawn_blocking(move || validation::verify_record_ids(&db_path, &run_id))
                .await??
        };

        let storage_info = {
            let db_path = context.db_path.clone();
            tokio::task::spawn_blocking(move || db::inspect(&db_path)).await??
        };

        let samples = raw.samples;
        let stats_after = context.server.as_ref().and_then(|server| server.stats());
        let storage_deltas = stats_before
            .zip(stats_after)
            .map(|(before, after)| StorageDeltas {
                records_written: after.records_written - before.records_written,
                transactions_committed: after.transactions_committed
                    - before.transactions_committed,
                batches_received: after.batches_received - before.batches_received,
                maintenance_runs: after.maintenance_runs - before.maintenance_runs,
                errors: after.errors - before.errors,
            });

        let results = ResultsSummary {
            measured_seconds: raw.measured_seconds,
            requests: counters.requests,
            records_generated: counters.records_generated,
            records_accepted: counters.records_accepted,
            records_rejected: counters.records_rejected,
            records_ambiguous: counters.records_ambiguous,
            requests_failed: counters.requests_failed,
            requests_timed_out: counters.requests_timed_out,
            achieved_generated_records_per_second: counters.records_generated as f64
                / raw.measured_seconds,
            achieved_accepted_records_per_second: counters.records_accepted as f64
                / raw.measured_seconds,
        };

        let report = StepReport {
            label: params.label.clone(),
            run_id: run_id.clone(),
            workload: WorkloadSummary {
                duration_seconds: params.duration_secs as f64,
                clients: params.clients,
                records_per_request: params.records_per_request,
                offered_records_per_second: params.offered_records_per_second,
            },
            results,
            latency_ms: Some(LatencyPercentiles::from_histogram(&raw.latency_us)),
            queue: QueueObservations::from_window(&window),
            pipeline_metrics: window,
            drained_rows: persisted,
            correctness,
            record_id_mismatches,
            rejection_positions_exact: raw.accounting.positions_exact,
            storage: storage_info,
            size_trajectory: SizeTrajectory::from_samples(&samples),
            storage_deltas,
        };

        if !report.correctness.passed || report.record_id_mismatches > 0 {
            print_step_summary(&report);
            bail!(
                "correctness validation failed for step '{}': {:?} (record-id mismatches: {})",
                params.label,
                report.correctness.failure_reasons,
                report.record_id_mismatches
            );
        }

        let loss_ratio = (counters.records_rejected + counters.records_ambiguous) as f64
            / counters.records_generated.max(1) as f64;
        if stop_on_saturation && loss_ratio > 0.05 {
            saturated = true;
            println!(
                "saturation detected at '{}' ({:.1}% rejected/ambiguous); stopping ramp",
                params.label,
                loss_ratio * 100.0
            );
        }

        print_step_summary(&report);
        reports.push(report);
    }

    Ok(ScenarioReport {
        name: name.to_owned(),
        steps: reports,
    })
}
