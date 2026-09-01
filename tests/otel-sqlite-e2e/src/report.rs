//! Report structures and human-readable summaries for scenario steps.
//!
//! All types here serialize into the benchmark's JSON output; the
//! `QueueObservations` extractor pulls the pipeline-relevant subset out of a
//! telemetry [`WindowReport`] for quick reading.

// Binaries report progress and results on stdout by design.
#![allow(clippy::print_stdout, clippy::print_stderr)]

use serde::Serialize;

use crate::db::StorageInfo;
use crate::metrics::{HistogramSummary, WindowReport};
use crate::runner::SamplePoint;
use crate::validation::CorrectnessReport;

#[derive(Debug, Clone, Serialize)]
pub struct WorkloadSummary {
    pub duration_seconds: f64,
    pub clients: usize,
    pub records_per_request: usize,
    pub offered_records_per_second: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ResultsSummary {
    pub measured_seconds: f64,
    pub requests: u64,
    pub records_generated: u64,
    pub records_accepted: u64,
    pub records_rejected: u64,
    pub records_ambiguous: u64,
    pub requests_failed: u64,
    pub requests_timed_out: u64,
    pub achieved_generated_records_per_second: f64,
    pub achieved_accepted_records_per_second: f64,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct LatencyPercentiles {
    pub min_ms: f64,
    pub p50_ms: f64,
    pub p90_ms: f64,
    pub p95_ms: f64,
    pub p99_ms: f64,
    pub p999_ms: f64,
    pub max_ms: f64,
}

impl LatencyPercentiles {
    pub(crate) fn from_histogram(histogram: &hdrhistogram::Histogram<u64>) -> Self {
        let ms = |micros: u64| micros as f64 / 1_000.0;
        Self {
            min_ms: ms(histogram.min()),
            p50_ms: ms(histogram.value_at_quantile(0.50)),
            p90_ms: ms(histogram.value_at_quantile(0.90)),
            p95_ms: ms(histogram.value_at_quantile(0.95)),
            p99_ms: ms(histogram.value_at_quantile(0.99)),
            p999_ms: ms(histogram.value_at_quantile(0.999)),
            max_ms: ms(histogram.max()),
        }
    }
}

/// Pipeline observability pulled out of the telemetry window for quick reading.
#[derive(Debug, Clone, Default, Serialize)]
pub struct QueueObservations {
    pub ingress_queue_depth_peak: f64,
    pub ingress_queue_depth_last: f64,
    pub storage_queue_depth_peak: f64,
    pub storage_queue_depth_last: f64,
    pub queue_full_events: u64,
    pub enqueue_duration_p95_ms: Option<f64>,
    pub enqueue_duration_p99_ms: Option<f64>,
    pub transaction_duration_p50_ms: Option<f64>,
    pub transaction_duration_p95_ms: Option<f64>,
    pub transaction_duration_p99_ms: Option<f64>,
    pub storage_errors_total: u64,
}

fn gauges_peak(window: &WindowReport, prefix: &str) -> f64 {
    window
        .gauges_peak
        .iter()
        .filter(|(key, _)| key.starts_with(prefix))
        .map(|(_, value)| *value)
        .fold(f64::NEG_INFINITY, f64::max)
        .max(0.0)
}

fn gauges_last(window: &WindowReport, prefix: &str) -> f64 {
    window
        .gauges_last
        .iter()
        .filter(|(key, _)| key.starts_with(prefix))
        .map(|(_, value)| *value)
        .fold(f64::NEG_INFINITY, f64::max)
        .max(0.0)
}

fn counter_delta(window: &WindowReport, prefix: &str) -> u64 {
    window
        .counters
        .iter()
        .filter(|(key, _)| key.starts_with(prefix))
        .map(|(_, value)| *value)
        .sum()
}

fn histogram_summary(window: &WindowReport, prefix: &str) -> Option<HistogramSummary> {
    window
        .histograms
        .iter()
        .find(|(key, _)| key.starts_with(prefix))
        .map(|(_, summary)| summary.clone())
}

impl QueueObservations {
    pub(crate) fn from_window(window: &WindowReport) -> Self {
        let enqueue = histogram_summary(window, "ingress_enqueue_duration");
        let transaction = histogram_summary(window, "storage_transaction_duration");
        Self {
            ingress_queue_depth_peak: gauges_peak(window, "ingress_queue_depth"),
            ingress_queue_depth_last: gauges_last(window, "ingress_queue_depth"),
            storage_queue_depth_peak: gauges_peak(window, "storage_queue_depth"),
            storage_queue_depth_last: gauges_last(window, "storage_queue_depth"),
            queue_full_events: counter_delta(window, "ingress_queue_full_total"),
            enqueue_duration_p95_ms: enqueue.as_ref().map(|summary| summary.p95_ms),
            enqueue_duration_p99_ms: enqueue.as_ref().map(|summary| summary.p99_ms),
            transaction_duration_p50_ms: transaction.as_ref().map(|summary| summary.p50_ms),
            transaction_duration_p95_ms: transaction.as_ref().map(|summary| summary.p95_ms),
            transaction_duration_p99_ms: transaction.as_ref().map(|summary| summary.p99_ms),
            storage_errors_total: counter_delta(window, "storage_errors_total"),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, Serialize)]
pub struct SizeTrajectory {
    pub samples: usize,
    pub first_database_bytes: u64,
    pub last_database_bytes: u64,
    pub peak_database_bytes: u64,
    pub first_wal_bytes: u64,
    pub last_wal_bytes: u64,
    pub peak_wal_bytes: u64,
}

impl SizeTrajectory {
    pub(crate) fn from_samples(samples: &[SamplePoint]) -> Self {
        let mut trajectory = Self {
            samples: samples.len(),
            ..Self::default()
        };
        for (index, sample) in samples.iter().enumerate() {
            if index == 0 {
                trajectory.first_database_bytes = sample.database_bytes;
                trajectory.first_wal_bytes = sample.wal_bytes;
            }
            trajectory.last_database_bytes = sample.database_bytes;
            trajectory.last_wal_bytes = sample.wal_bytes;
            trajectory.peak_database_bytes =
                trajectory.peak_database_bytes.max(sample.database_bytes);
            trajectory.peak_wal_bytes = trajectory.peak_wal_bytes.max(sample.wal_bytes);
        }
        trajectory
    }
}

/// Storage-writer counter deltas captured around one step (embedded mode).
#[derive(Debug, Clone, Copy, Default, Serialize)]
pub struct StorageDeltas {
    pub records_written: u64,
    pub transactions_committed: u64,
    pub batches_received: u64,
    pub maintenance_runs: u64,
    pub errors: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct StepReport {
    pub label: String,
    pub run_id: String,
    pub workload: WorkloadSummary,
    pub results: ResultsSummary,
    pub latency_ms: Option<LatencyPercentiles>,
    pub pipeline_metrics: WindowReport,
    pub queue: QueueObservations,
    /// Rows observed for this run id when the drain poll finished.
    pub drained_rows: u64,
    pub correctness: CorrectnessReport,
    pub record_id_mismatches: u64,
    pub rejection_positions_exact: bool,
    pub storage: StorageInfo,
    pub size_trajectory: SizeTrajectory,
    pub storage_deltas: Option<StorageDeltas>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ScenarioReport {
    pub name: String,
    pub steps: Vec<StepReport>,
}

/// Concise human-readable summary for one completed step.
pub fn print_step_summary(report: &StepReport) {
    let results = &report.results;
    println!();
    println!("Step:                  {}", report.label);
    println!("Duration:              {:.1}s", results.measured_seconds);
    println!("Clients:               {}", report.workload.clients);
    println!(
        "Records/request:       {}",
        report.workload.records_per_request
    );
    if let Some(offered) = report.workload.offered_records_per_second {
        println!("Offered:               {offered} records/s");
    }
    println!(
        "Generated:             {} ({:.0} records/s)",
        results.records_generated, results.achieved_generated_records_per_second
    );
    println!(
        "Accepted:              {} ({:.0} records/s)",
        results.records_accepted, results.achieved_accepted_records_per_second
    );
    println!("Rejected:              {}", results.records_rejected);
    println!("Ambiguous:             {}", results.records_ambiguous);

    if let Some(latency) = &report.latency_ms {
        println!("Latency:");
        println!("  p50:                 {:.2} ms", latency.p50_ms);
        println!("  p95:                 {:.2} ms", latency.p95_ms);
        println!("  p99:                 {:.2} ms", latency.p99_ms);
        println!("  p99.9:               {:.2} ms", latency.p999_ms);
        println!("  max:                 {:.2} ms", latency.max_ms);
    }

    println!("Queue:");
    println!(
        "  ingress depth peak:   {}",
        report.queue.ingress_queue_depth_peak
    );
    println!(
        "  storage depth peak:   {}",
        report.queue.storage_queue_depth_peak
    );
    println!("  queue-full events:    {}", report.queue.queue_full_events);

    println!("Storage:");
    println!(
        "  DB size:              {} bytes",
        report.storage.database_bytes
    );
    println!("  WAL size:             {} bytes", report.storage.wal_bytes);
    if let Some(deltas) = &report.storage_deltas {
        println!("  transactions:         {}", deltas.transactions_committed);
        println!("  records written:      {}", deltas.records_written);
        println!("  writer errors:        {}", deltas.errors);
    }

    println!("Correctness:");
    println!("  generated:            {}", report.correctness.generated);
    println!("  persisted rows:       {}", report.correctness.rows_found);
    println!(
        "  missing sequences:    {}",
        report.correctness.missing_sequences
    );
    println!(
        "  duplicates:           {}",
        report.correctness.duplicate_sequences
    );
    println!("  id mismatches:        {}", report.record_id_mismatches);
    println!(
        "  status:               {}",
        if report.correctness.passed {
            "PASS"
        } else {
            "FAIL"
        }
    );
}
