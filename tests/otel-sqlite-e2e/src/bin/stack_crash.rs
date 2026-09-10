//! otel-sqlite-stack-crash: container-level crash/restart durability driver.
//!
//! Orchestrated by `tests/docker/crash-e2e.sh` against `tests/docker/compose.crash-e2e.yml`:
//!
//! ```text
//!   1. `load`     -- drives OTLP load against the sidecar for a fixed
//!                   duration and writes an outcome manifest;
//!   2. the host   -- `docker kill`s the sidecar mid-load (SIGKILL, no
//!                   cleanup, exercising the WAL recovery path);
//!   3. the host   -- restarts the sidecar on the same volume;
//!   4. `load`     -- runs again to prove live traffic after recovery;
//!   5. `validate` -- asserts each manifest against the shared database.
//! ```
//!
//! Load execution and validation reuse `otel_sqlite_e2e::crash` — the exact
//! same code the native crash test (`crates/otel-sqlite/tests/crash_durability.rs`)
//! uses — so the native and container crash paths cannot drift.

// Verifier reports progress and results on stdout by design; assertions fail
// via bail!() with context instead of panicking.
#![allow(clippy::print_stdout, clippy::print_stderr)]

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context as _, Result};
use clap::{Parser, Subcommand};
use otel_sqlite_e2e::client::ClientCountersSnapshot;
use otel_sqlite_e2e::crash::{LoadOutcome, LoadParams, assert_valid, run_load};
use otel_sqlite_e2e::generator::WorkloadSpec;
use otel_sqlite_e2e::interval_set::IntervalSet;
use serde::{Deserialize, Serialize};

#[derive(Parser, Debug)]
#[command(
    name = "otel-sqlite-stack-crash",
    about = "Container crash/restart durability driver",
    version
)]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Drive OTLP load against the sidecar and write an outcome manifest.
    Load {
        #[arg(long, default_value = "http://sidecar:4317")]
        endpoint: String,
        #[arg(long)]
        run_id: String,
        #[arg(long, default_value_t = 10)]
        duration_secs: u64,
        #[arg(long, default_value_t = 4)]
        clients: usize,
        #[arg(long, default_value_t = 500)]
        records_per_request: usize,
        #[arg(long, default_value_t = 7)]
        seed: u64,
        /// Manifest output path (shared volume so later runs can read it).
        #[arg(long)]
        output: PathBuf,
    },
    /// Validate a manifest against the shared database.
    Validate {
        #[arg(long)]
        db_path: PathBuf,
        #[arg(long)]
        manifest: PathBuf,
    },
}

/// Outcome accounting serialized between load and validate runs.
#[derive(Debug, Serialize, Deserialize)]
struct Manifest {
    run_id: String,
    generated: u64,
    accepted: u64,
    rejected: u64,
    ambiguous: u64,
    rejected_ranges: Vec<(u64, u64)>,
    ambiguous_ranges: Vec<(u64, u64)>,
}

impl Manifest {
    fn from_outcome(outcome: &LoadOutcome) -> Self {
        let counters = &outcome.counters;
        Self {
            run_id: String::new(),
            generated: counters.records_generated,
            accepted: counters.records_accepted,
            rejected: counters.records_rejected,
            ambiguous: counters.records_ambiguous,
            rejected_ranges: outcome.rejected.ranges().to_vec(),
            ambiguous_ranges: outcome.ambiguous.ranges().to_vec(),
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    match args.command {
        Command::Load {
            endpoint,
            run_id,
            duration_secs,
            clients,
            records_per_request,
            seed,
            output,
        } => {
            run_load_phase(
                &endpoint,
                &run_id,
                duration_secs,
                clients,
                records_per_request,
                seed,
                &output,
            )
            .await
        }
        Command::Validate { db_path, manifest } => run_validate_phase(&db_path, &manifest),
    }
}

async fn run_load_phase(
    endpoint: &str,
    run_id: &str,
    duration_secs: u64,
    clients: usize,
    records_per_request: usize,
    seed: u64,
    output: &PathBuf,
) -> Result<()> {
    let spec = WorkloadSpec {
        run_id: run_id.to_owned(),
        seed,
        body_size: 64,
        attributes_per_record: 2,
        resource_count: 2,
    };
    let params = LoadParams {
        run_id: run_id.to_owned(),
        clients,
        records_per_request,
        // Closed loop: the pipeline stays saturated, so requests are in
        // flight (durable-ack waits) at whatever moment the host kills the
        // sidecar.
        offered_records_per_second: None,
        duration: Duration::from_secs(duration_secs),
        request_timeout: Duration::from_secs(15),
    };

    println!(
        "load '{run_id}': {duration_secs}s closed-loop, {clients} clients x {records_per_request} \
         rec/req against {endpoint}"
    );
    let outcome = run_load(endpoint, spec, &params)
        .await
        .context("load phase failed")?;
    println!(
        "load '{run_id}': generated={} accepted={} rejected={} ambiguous={}",
        outcome.counters.records_generated,
        outcome.counters.records_accepted,
        outcome.counters.records_rejected,
        outcome.counters.records_ambiguous,
    );

    let mut manifest = Manifest::from_outcome(&outcome);
    run_id.clone_into(&mut manifest.run_id);
    if let Some(parent) = output
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    }
    std::fs::write(output, serde_json::to_vec_pretty(&manifest)?)
        .with_context(|| format!("write manifest {}", output.display()))?;
    println!("manifest written to {}", output.display());
    Ok(())
}

fn run_validate_phase(db_path: &std::path::Path, manifest_path: &PathBuf) -> Result<()> {
    let text = std::fs::read_to_string(manifest_path)
        .with_context(|| format!("read manifest {}", manifest_path.display()))?;
    let manifest: Manifest = serde_json::from_str(&text)
        .with_context(|| format!("parse manifest {}", manifest_path.display()))?;

    // Round-trip the accounting exactly as `crash::run_load` produced it.
    let outcome = LoadOutcome {
        counters: ClientCountersSnapshot {
            requests: 0,
            records_generated: manifest.generated,
            records_accepted: manifest.accepted,
            records_rejected: manifest.rejected,
            records_ambiguous: manifest.ambiguous,
            requests_failed: 0,
            requests_timed_out: 0,
        },
        rejected: IntervalSet::from_ranges(manifest.rejected_ranges),
        ambiguous: IntervalSet::from_ranges(manifest.ambiguous_ranges),
    };

    println!(
        "validate '{}': db {} (generated={} accepted={} rejected={} ambiguous={})",
        manifest.run_id,
        db_path.display(),
        outcome.counters.records_generated,
        outcome.counters.records_accepted,
        outcome.counters.records_rejected,
        outcome.counters.records_ambiguous,
    );
    assert_valid(db_path, &manifest.run_id, &outcome)
        .context("crash-recovery validation failed")?;
    println!(
        "PASS: '{}' durable after restart (no acknowledged loss)",
        manifest.run_id
    );
    Ok(())
}
