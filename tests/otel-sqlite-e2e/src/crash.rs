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
use std::time::Duration;

use otel_sqlite_core::storage::SyncMode;
use serde::Serialize;

use crate::accounting::RunAccounting;
use crate::generator::WorkloadSpec;
use crate::runner::{LoadRequest, TransportFailurePolicy};
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
        .join(otel_sqlite_storage::DEFAULT_DB_FILENAME)
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

pub type LoadOutcome = RunAccounting;

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

/// Executes a bounded load through the shared driver, tolerating transport
/// failures: a crash test EXPECTS the endpoint to die mid-run, and every
/// request after the kill must still be classified (rejected or ambiguous),
/// never silently dropped from the accounting.
pub async fn run_load(
    endpoint: &str,
    spec: WorkloadSpec,
    params: &LoadParams,
) -> anyhow::Result<LoadOutcome> {
    let run = crate::runner::run_load(LoadRequest {
        endpoint: endpoint.to_owned(),
        spec: std::sync::Arc::new(spec),
        clients: params.clients,
        records_per_request: params.records_per_request,
        offered_records_per_second: params.offered_records_per_second,
        duration: params.duration,
        request_timeout: params.request_timeout,
        transport_failures: TransportFailurePolicy::Tolerate,
        sampling: None,
    })
    .await?;
    Ok(run.accounting)
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
    validation::validate(db_path, run_id, outcome)
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
    if !outcome.positions_exact {
        anyhow::bail!(
            "strict crash validation for run '{run_id}' requires exact rejected positions;              a mixed partial rejection made them unknowable"
        );
    }
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
    let conn = crate::db::open_readonly(db_path)?;
    let integrity: String = conn.query_row("PRAGMA integrity_check", [], |row| row.get(0))?;
    if integrity != "ok" {
        anyhow::bail!("integrity_check failed after component death: {integrity}");
    }
    Ok(())
}
