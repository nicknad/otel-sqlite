//! Crash/restart durability harness pieces shared by the native process test
//! (`crates/otel-sqlite/tests/crash_durability.rs`) and the container crash
//! driver (`otel-sqlite-stack-crash`).
//!
//! Everything here is process-agnostic: server config generation, load
//! execution with outcome accounting, and post-restart SQLite validation.
//! Keeping load + validate in ONE place means the two crash paths cannot
//! drift apart, and both stay consistent with the benchmark runner's
//! accounting (`generated = accepted + rejected + ambiguous`).

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use otel_sqlite_core::storage::SyncMode;
use serde::Serialize;
use tokio::time::Instant as TokioInstant;

use crate::client::{
    ClientCounters, ClientCountersSnapshot, LatencyHistogram, LoadClient, SequenceAllocator,
};
use crate::generator::WorkloadSpec;
use crate::interval_set::IntervalSet;
use crate::ledger::Ledger;
use crate::pacer::Pacer;
use crate::validation;

// ---------------------------------------------------------------------------
// Server configuration
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct CrashServerConfig {
    /// Data directory: the database and its WAL sidecars live here and are
    /// reused across the crash/restart cycle.
    pub dir: PathBuf,
    pub port: u16,
    pub synchronous: SyncMode,
    /// Fast watchdog thresholds so a deliberately-killed pipeline thread is
    /// detected and halted within seconds (writer/batcher death tests).
    pub fast_watchdog: bool,
    /// Graceful-drain deadline also used to bound the fault-death shutdown.
    pub shutdown_timeout: Duration,
}

impl CrashServerConfig {
    pub fn new(dir: PathBuf, port: u16, synchronous: SyncMode) -> Self {
        Self {
            dir,
            port,
            synchronous,
            fast_watchdog: false,
            shutdown_timeout: Duration::from_secs(30),
        }
    }
}

#[derive(Serialize)]
struct ServerConfigToml {
    listen_address: String,
    sqlite_path: String,
    metrics_address: String,
    shutdown_timeout: String,
    durability: TomlDurability,
    maintenance: TomlMaintenance,
    watchdog: Option<TomlWatchdog>,
}

#[derive(Serialize)]
struct TomlDurability {
    mode: &'static str,
    synchronous: &'static str,
}

#[derive(Serialize)]
struct TomlMaintenance {
    retention: &'static str,
    purge_interval: &'static str,
    checkpoint_interval: &'static str,
    optimize_interval: &'static str,
    vacuum_interval: &'static str,
    rebuild_fts_interval: &'static str,
}

#[derive(Serialize)]
struct TomlWatchdog {
    tick_interval: &'static str,
    degraded_after: &'static str,
    unhealthy_after: &'static str,
    ack_stall_after: &'static str,
    halt_on_unhealthy: bool,
}

/// Writes the server config used by every crash scenario: `durability.mode =
/// "commit"`, periodic maintenance disabled so the WAL accumulates
/// un-checkpointed frames (WAL recovery is what a restart must replay), and
/// metrics off so parallel tests never fight over the admin port.
///
/// Returns the config file path; point `OTEL_SQLITE_CFG_PATH` at it.
pub fn write_server_config(config: &CrashServerConfig) -> PathBuf {
    // Forward slashes keep the TOML file valid on Windows too (SQLite accepts
    // them everywhere), avoiding backslash-escaping pitfalls in the config.
    let sqlite_path = config
        .dir
        .join("otel-logs.db")
        .display()
        .to_string()
        .replace('\\', "/");
    let toml = ServerConfigToml {
        listen_address: format!("127.0.0.1:{}", config.port),
        sqlite_path,
        metrics_address: "off".to_owned(),
        shutdown_timeout: format!("{}s", config.shutdown_timeout.as_secs()),
        durability: TomlDurability {
            mode: "commit",
            synchronous: config.synchronous.as_str(),
        },
        maintenance: TomlMaintenance {
            retention: "off",
            purge_interval: "off",
            checkpoint_interval: "off",
            optimize_interval: "off",
            vacuum_interval: "off",
            rebuild_fts_interval: "off",
        },
        watchdog: config.fast_watchdog.then_some(TomlWatchdog {
            tick_interval: "1s",
            degraded_after: "1s",
            unhealthy_after: "2s",
            ack_stall_after: "5s",
            halt_on_unhealthy: true,
        }),
    };
    let text = toml::to_string_pretty(&toml).expect("serialize crash server config");
    let path = config.dir.join("otel-sqlite-crash.toml");
    std::fs::write(&path, text).expect("write crash server config");
    path
}

// ---------------------------------------------------------------------------
// Load execution and outcome accounting
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct LoadOutcome {
    pub counters: ClientCountersSnapshot,
    /// Sequences definitively rejected (`UNAVAILABLE`/`InvalidArgument`).
    pub rejected: IntervalSet,
    /// Sequences whose fate is unknown client-side (timeout/transport break).
    pub ambiguous: IntervalSet,
}

#[derive(Debug, Clone)]
pub struct LoadParams {
    pub run_id: String,
    pub clients: usize,
    pub records_per_request: usize,
    /// `Some(rate)` paces an open-loop offered load; `None` runs closed-loop
    /// (workers send as fast as the pipeline accepts, keeping the pipeline
    /// busy so requests are genuinely in flight at any kill point).
    pub offered_records_per_second: Option<u64>,
    pub duration: Duration,
    pub request_timeout: Duration,
}

impl LoadParams {
    pub fn new(run_id: impl Into<String>) -> Self {
        Self {
            run_id: run_id.into(),
            clients: 4,
            records_per_request: 500,
            offered_records_per_second: None,
            duration: Duration::from_secs(5),
            request_timeout: Duration::from_secs(10),
        }
    }
}

/// Executes a bounded load and returns the outcome accounting. Unlike the
/// benchmark runner there is deliberately NO consecutive-failure abort: a
/// crash test EXPECTS the endpoint to die mid-run, and every request after
/// the kill must still be classified (rejected or ambiguous), never silently
/// dropped from the accounting.
///
/// Every worker is awaited, including any request still in flight past the
/// deadline (bounded by `request_timeout`), so
/// `generated = accepted + rejected + ambiguous` holds exactly and
/// validation never sees unclassified sequences.
pub async fn run_load(
    endpoint: &str,
    spec: WorkloadSpec,
    params: &LoadParams,
) -> anyhow::Result<LoadOutcome> {
    let spec = Arc::new(spec);
    let allocator = Arc::new(SequenceAllocator::new(0));
    let ledger = Arc::new(Ledger::new());
    let latencies = Arc::new(LatencyHistogram::new());
    let counters = Arc::new(ClientCounters::default());

    let offered_request_rate = params
        .offered_records_per_second
        .map(|records_per_sec| records_per_sec as f64 / params.records_per_request as f64);
    let pacer = Arc::new(Pacer::new(offered_request_rate));

    let deadline = TokioInstant::now() + params.duration;
    let records_per_request = params.records_per_request;
    let request_timeout = params.request_timeout;

    let mut workers = Vec::with_capacity(params.clients);
    for _ in 0..params.clients {
        let client_endpoint = endpoint.to_owned();
        let spec = Arc::clone(&spec);
        let allocator = Arc::clone(&allocator);
        let pacer = Arc::clone(&pacer);
        let ledger = Arc::clone(&ledger);
        let latencies = Arc::clone(&latencies);
        let counters = Arc::clone(&counters);
        workers.push(tokio::spawn(async move {
            load_worker(
                &client_endpoint,
                spec,
                allocator,
                records_per_request,
                request_timeout,
                pacer,
                ledger,
                latencies,
                counters,
                deadline,
            )
            .await
        }));
    }

    for worker in workers {
        worker.await??;
    }

    let (rejected, ambiguous, _positions_exact) = ledger.freeze();
    Ok(LoadOutcome {
        counters: counters.snapshot(),
        rejected,
        ambiguous,
    })
}

#[allow(clippy::too_many_arguments)]
async fn load_worker(
    endpoint: &str,
    spec: Arc<WorkloadSpec>,
    allocator: Arc<SequenceAllocator>,
    records_per_request: usize,
    request_timeout: Duration,
    pacer: Arc<Pacer>,
    ledger: Arc<Ledger>,
    latencies: Arc<LatencyHistogram>,
    counters: Arc<ClientCounters>,
    deadline: TokioInstant,
) -> anyhow::Result<()> {
    let mut client = LoadClient::connect(
        endpoint,
        spec,
        allocator,
        records_per_request,
        request_timeout,
        latencies,
        counters,
    )
    .await?;

    while TokioInstant::now() < deadline {
        pacer.acquire().await;
        if TokioInstant::now() >= deadline {
            break;
        }
        let result = client.send_one().await;
        ledger.record(&result);
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Post-restart validation
// ---------------------------------------------------------------------------

/// Runs the standard correctness validation for one run: every
/// non-rejected/non-ambiguous sequence must be persisted exactly once.
pub fn validate_run(
    db_path: &Path,
    run_id: &str,
    outcome: &LoadOutcome,
) -> anyhow::Result<validation::CorrectnessReport> {
    validation::validate(
        db_path,
        run_id,
        outcome.counters.records_generated,
        &outcome.rejected,
        &outcome.ambiguous,
    )
}

/// Full crash-recovery assertion: correctness passes (no acknowledged loss,
/// no duplicates, no rejected-present), the database survives
/// `PRAGMA integrity_check`, and it is still a WAL database after replay.
///
/// Strict mode: every `Rejected` sequence must be absent. This holds for a
/// SIGKILL crash because the process dies before sending any `UNAVAILABLE`,
/// so rejected requests never reached the server. Use
/// [`assert_valid_recovery`] when a pipeline *component* dies instead.
pub fn assert_valid(db_path: &Path, run_id: &str, outcome: &LoadOutcome) -> anyhow::Result<()> {
    let report = validate_run(db_path, run_id, outcome)?;
    if !report.passed {
        anyhow::bail!(
            "crash recovery validation failed for run '{run_id}': {:?}",
            report.failure_reasons
        );
    }
    let conn = crate::db::open_readonly(db_path)?;
    let integrity: String = conn.query_row("PRAGMA integrity_check", [], |row| row.get(0))?;
    if integrity != "ok" {
        anyhow::bail!("integrity_check failed after crash: {integrity}");
    }
    let journal_mode: String = conn.query_row("PRAGMA journal_mode", [], |row| row.get(0))?;
    if journal_mode != "wal" {
        anyhow::bail!("expected journal_mode=wal after recovery, got {journal_mode}");
    }
    Ok(())
}

/// Recovery assertion for the component-death (fault) tests.
///
/// When a writer or batcher dies mid-commit, a request that was *fully
/// admitted* can be `UNAVAILABLE`-rejected after only part of its records
/// committed (the ledger closes before its last ticket settles). That partial
/// application is expected and NOT a failure here: the acknowledged portion
/// is durable, the rest is lost-but-counted as rejected. This variant
/// therefore checks the durable-ack contract — no acknowledged record
/// missing, no duplicates, exact `generated = accepted + rejected +
/// ambiguous` accounting, database intact — without requiring rejected
/// sequences to be absent.
pub fn assert_valid_recovery(
    db_path: &Path,
    run_id: &str,
    outcome: &LoadOutcome,
) -> anyhow::Result<()> {
    let report = validate_run(db_path, run_id, outcome)?;
    if report.missing_sequences > 0 {
        anyhow::bail!(
            "crash recovery validation failed for run '{run_id}': {} acknowledged sequences lost",
            report.missing_sequences
        );
    }
    if report.duplicate_sequences > 0 {
        anyhow::bail!(
            "crash recovery validation failed for run '{run_id}': {} duplicate rows",
            report.duplicate_sequences
        );
    }
    let classified = outcome.counters.records_accepted
        + outcome.counters.records_rejected
        + outcome.counters.records_ambiguous;
    if classified != outcome.counters.records_generated {
        anyhow::bail!(
            "crash recovery validation failed for run '{run_id}': accounting mismatch, \
             generated {} != accepted {} + rejected {} + ambiguous {}",
            outcome.counters.records_generated,
            outcome.counters.records_accepted,
            outcome.counters.records_rejected,
            outcome.counters.records_ambiguous,
        );
    }
    let conn = crate::db::open_readonly(db_path)?;
    let integrity: String = conn.query_row("PRAGMA integrity_check", [], |row| row.get(0))?;
    if integrity != "ok" {
        anyhow::bail!("integrity_check failed after component death: {integrity}");
    }
    Ok(())
}
