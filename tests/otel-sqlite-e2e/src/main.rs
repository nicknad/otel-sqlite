//! otel-sqlite-e2e: end-to-end benchmark harness for the
//! OTLP -> ingress -> queue -> single SQLite writer -> SQLite pipeline.
//!
//! Modes:
//!
//! * embedded (default) - boots the real pipeline in-process, wired exactly
//!   like the production binary (see `server.rs`). Programmatic shutdown
//!   replaces the OS-signal wait; every other component is production code.
//! * external           - loads a running server via `--endpoint`; validation
//!   reads the database through `--db-path` (local path or Docker volume).

// Binaries report progress and results on stdout by design.
#![allow(clippy::print_stdout, clippy::print_stderr)]

use otel_sqlite_e2e::{db, metrics, params, report, scenarios, server};

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context as AnyhowContext, Result, bail};
use clap::{Parser, ValueEnum};
use serde::Serialize;

#[derive(Copy, Clone, PartialEq, Eq, ValueEnum, Debug)]
enum ScenarioChoice {
    /// A - small controlled workload.
    Baseline,
    /// B - progressively higher offered load.
    Throughput,
    /// C - records-per-request comparison.
    Batch,
    /// D - client-count comparison at constant offered load.
    Concurrency,
    /// E - long-running sustained load (default 10 minutes).
    Sustained,
    /// F - intentional overload / backpressure verification.
    Backpressure,
    /// G - storage insert-batcher behaviour across OTLP batch shapes.
    InsertBatching,
    /// Everything except `sustained`.
    All,
}

#[derive(Parser, Debug)]
#[command(
    name = "otel-sqlite-e2e",
    about = "End-to-end performance and correctness harness for otel-sqlite",
    version
)]
struct Args {
    /// Scenario to execute.
    #[arg(long, value_enum, default_value_t = ScenarioChoice::Baseline)]
    scenario: ScenarioChoice,

    /// Load an already-running server instead of booting one in-process
    /// (e.g. <http://127.0.0.1:4317> or a Docker service name).
    #[arg(long)]
    endpoint: Option<String>,

    /// SQLite database to validate against in external mode. Must be readable
    /// from this process (shared volume for Docker runs). Required together
    /// with --endpoint.
    #[arg(long)]
    db_path: Option<PathBuf>,

    /// Listen address used by the embedded pipeline.
    #[arg(long, default_value = "127.0.0.1:4317")]
    listen: String,

    /// Data directory for the embedded-mode database. Defaults to a fresh
    /// temporary directory that is removed after the run.
    #[arg(long)]
    data_dir: Option<PathBuf>,

    /// Deterministic seed for workload generation.
    #[arg(long, default_value_t = 42)]
    seed: u64,

    /// Warm-up seconds before each scenario's measurement steps.
    #[arg(long, default_value_t = 5)]
    warmup_secs: u64,

    /// Per-request gRPC timeout in milliseconds.
    #[arg(long, default_value_t = 30_000)]
    request_timeout_ms: u64,

    /// Approximate log body payload size in bytes.
    #[arg(long, default_value_t = 120)]
    body_size: usize,

    /// Extra attributes per record beyond benchmark bookkeeping attributes.
    #[arg(long, default_value_t = 4)]
    attributes: usize,

    /// Distinct OTLP resources spread round-robin across records.
    #[arg(long, default_value_t = 4)]
    resources: usize,

    /// Drain timeout after producers stop.
    #[arg(long, default_value_t = 120)]
    drain_timeout_secs: u64,

    /// Override step duration for the selected scenario.
    #[arg(long)]
    duration_secs: Option<u64>,

    /// Override client count for the selected scenario.
    #[arg(long)]
    clients: Option<usize>,

    /// Override records-per-request for the selected scenario.
    #[arg(long)]
    records_per_request: Option<usize>,

    /// Override offered records/second where the scenario supports it.
    #[arg(long)]
    rate: Option<u64>,

    /// Write machine-readable results as JSON to this path.
    #[arg(short, long)]
    output: Option<PathBuf>,

    /// Keep the throughput ramp running past detected saturation.
    #[arg(long, default_value_t = false)]
    keep_going: bool,

    /// Keep the embedded-mode data directory after exit (debugging aid).
    #[arg(long, default_value_t = false)]
    keep_data: bool,

    /// Proceed even when running a non-release build.
    #[arg(long, default_value_t = false)]
    allow_dev: bool,
}

// ---------------------------------------------------------------------------
// Environment capture
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize)]
struct EnvironmentInfo {
    benchmark_version: String,
    git_revision: String,
    rustc_version: String,
    cargo_version: String,
    os: String,
    arch: String,
    cpu_model: Option<String>,
    ram_bytes: Option<u64>,
    sqlite_version: String,
    sqlite_synchronous: String,
    execution_mode: String,
    docker: bool,
    database_state: String,
    profile: String,
}

fn run_command(program: &str, args: &[&str]) -> Option<String> {
    let output = Command::new(program).args(args).output().ok()?.stdout;
    let text = String::from_utf8_lossy(&output).trim().to_owned();
    (!text.is_empty()).then_some(text)
}

fn git_revision() -> String {
    if let Some(rev) = std::env::var("OTEL_SQLITE_BENCH_GIT_REV")
        .ok()
        .filter(|rev| !rev.is_empty())
    {
        return rev;
    }
    run_command("git", &["rev-parse", "HEAD"]).unwrap_or_else(|| "unknown".to_owned())
}

fn cpu_model() -> Option<String> {
    if let Some(identifier) = std::env::var("PROCESSOR_IDENTIFIER")
        .ok()
        .filter(|identifier| !identifier.is_empty())
    {
        return Some(identifier);
    }
    if let Ok(cpuinfo) = std::fs::read_to_string("/proc/cpuinfo") {
        for line in cpuinfo.lines() {
            if let Some(rest) = line.strip_prefix("model name") {
                let model = rest.trim_start_matches([':', ' ']);
                if !model.is_empty() {
                    return Some(model.trim_end_matches('\u{0}').to_owned());
                }
            }
        }
    }
    run_command("sysctl", &["-n", "machdep.cpu.brand_string"])
}

fn ram_bytes() -> Option<u64> {
    std::fs::read_to_string("/proc/meminfo")
        .ok()
        .and_then(|meminfo| {
            meminfo.lines().find_map(|line| {
                line.strip_prefix("MemTotal:")
                    .and_then(|rest| rest.trim().strip_suffix(" kB"))
                    .and_then(|kb| kb.trim().parse::<u64>().ok())
                    .map(|kb| kb * 1024)
            })
        })
}

fn is_docker() -> bool {
    Path::new("/.dockerenv").exists()
        || std::env::var("OTEL_SQLITE_BENCH_DOCKER").is_ok_and(|value| value == "1")
}

// ---------------------------------------------------------------------------
// Report envelope
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
struct ConfigurationSummary {
    scenario: String,
    endpoint: String,
    database_path: String,
    seed: u64,
    warmup_secs: u64,
    request_timeout_ms: u64,
    body_size_bytes: usize,
    attributes_per_record: usize,
    resources: usize,
    drain_timeout_secs: u64,
    overrides_applied: params::Overrides,
    stop_on_saturation_ramp: bool,
}

#[derive(Debug, Serialize)]
struct FinalStorage {
    note: String,
    storage: db::StorageInfo,
}

#[derive(Debug, Serialize)]
struct BenchmarkOutput {
    schema_version: u32,
    environment: EnvironmentInfo,
    configuration: ConfigurationSummary,
    pre_existing_log_rows: u64,
    scenarios: Vec<report::ScenarioReport>,
    post_shutdown_storage: Option<FinalStorage>,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn init_tracing() {
    use tracing_subscriber::EnvFilter;
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .compact()
        .init();
}

async fn wait_for_endpoint(endpoint: &str, timeout: Duration) -> Result<()> {
    let host_port = endpoint
        .trim_start_matches("http://")
        .trim_start_matches("https://")
        .to_owned();
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if tokio::net::TcpStream::connect(host_port.as_str())
            .await
            .is_ok()
        {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            bail!("endpoint {endpoint} not reachable within {timeout:?}");
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// Owns the temporary data directory for embedded runs; deletes it on drop
/// unless retention was requested.
struct TempDirGuard {
    path: PathBuf,
    retain: bool,
}

impl TempDirGuard {
    fn create(explicit: Option<PathBuf>, retain: bool) -> Result<Self> {
        let path = if let Some(path) = explicit {
            path
        } else {
            let unique = format!(
                "otel-sqlite-e2e-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_millis())
                    .unwrap_or_default()
            );
            std::env::temp_dir().join(unique)
        };
        std::fs::create_dir_all(&path)
            .with_context(|| format!("failed to create {}", path.display()))?;
        Ok(Self { path, retain })
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempDirGuard {
    fn drop(&mut self) {
        if self.retain {
            println!("data dir retained at {}", self.path.display());
            return;
        }
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    init_tracing();
    let telemetry = metrics::TelemetryHandle::install();

    // ----- execution mode ---------------------------------------------------
    let mut embedded_server: Option<Arc<server::EmbeddedServer>> = None;
    let mut _temp_guard: Option<TempDirGuard> = None;

    let (endpoint, db_path, execution_mode) = if let Some(url) = &args.endpoint {
        let db_path = args
            .db_path
            .clone()
            .context("--db-path is required with --endpoint (validation needs the database)")?;
        wait_for_endpoint(url, Duration::from_secs(30)).await?;
        (url.clone(), db_path, "native-external")
    } else {
        let guard = TempDirGuard::create(
            args.data_dir.clone(),
            args.keep_data || args.data_dir.is_some(),
        )?;
        println!("embedded data dir: {}", guard.path().display());
        let started = server::EmbeddedServer::start(&args.listen, guard.path()).await?;
        let endpoint = started.endpoint().to_owned();
        let db_path = started.db_path().to_path_buf();
        embedded_server = Some(Arc::new(started));
        _temp_guard = Some(guard);
        (endpoint, db_path, "native-embedded")
    };

    let database_state = if db_path.exists() {
        "pre-populated"
    } else {
        "fresh"
    };
    let pre_existing_rows = if db_path.exists() {
        let probe_path = db_path.clone();
        tokio::task::spawn_blocking(move || db::count_all_log_events(&probe_path)).await??
    } else {
        0
    };

    let environment = EnvironmentInfo {
        benchmark_version: env!("CARGO_PKG_VERSION").to_owned(),
        git_revision: git_revision(),
        rustc_version: run_command("rustc", &["--version"]).unwrap_or_else(|| "unknown".to_owned()),
        cargo_version: run_command("cargo", &["--version"]).unwrap_or_else(|| "unknown".to_owned()),
        os: std::env::consts::OS.to_owned(),
        arch: std::env::consts::ARCH.to_owned(),
        cpu_model: cpu_model(),
        ram_bytes: ram_bytes(),
        sqlite_version: rusqlite::version().to_owned(),
        sqlite_synchronous: server::production_defaults::sync_mode().as_str().to_owned(),
        execution_mode: execution_mode.to_owned(),
        docker: is_docker(),
        database_state: database_state.to_owned(),
        profile: if cfg!(debug_assertions) {
            "dev"
        } else {
            "release"
        }
        .to_owned(),
    };

    if environment.profile != "release" && !args.allow_dev {
        eprintln!(
            "warning: running a dev-profile build - results are not authoritative.\n         \
             use `cargo run --release -p otel-sqlite-e2e -- ...` or pass --allow-dev."
        );
    }

    println!(
        "\nOTEL SQLite E2E Benchmark\n=========================\nmode: {} | endpoint: {} | db: {}\nscenario: {}",
        execution_mode,
        endpoint,
        db_path.display(),
        format!("{:?}", args.scenario).to_lowercase()
    );

    let overrides = params::Overrides {
        duration_secs: args.duration_secs,
        clients: args.clients,
        records_per_request: args.records_per_request,
        offered_records_per_second: args.rate,
    };

    let context = params::RunContext {
        endpoint: endpoint.clone(),
        db_path: db_path.clone(),
        seed: args.seed,
        body_size: args.body_size,
        attributes_per_record: args.attributes,
        resource_count: args.resources,
        warmup_secs: args.warmup_secs,
        request_timeout: Duration::from_millis(args.request_timeout_ms),
        drain_timeout: Duration::from_secs(args.drain_timeout_secs),
        telemetry,
        server: embedded_server.clone(),
    };

    // ----- scenario dispatch -------------------------------------------------
    let ramp_stop = !args.keep_going;
    let plan: Vec<(&'static str, Vec<params::StepParams>, bool)> = match args.scenario {
        ScenarioChoice::Baseline => vec![("baseline", scenarios::baseline(&overrides), false)],
        ScenarioChoice::Throughput => vec![(
            "throughput-ramp",
            scenarios::throughput_ramp(&overrides),
            ramp_stop,
        )],
        ScenarioChoice::Batch => vec![("batch-sizes", scenarios::batch_sizes(&overrides), false)],
        ScenarioChoice::Concurrency => {
            vec![("concurrency", scenarios::concurrency(&overrides), false)]
        }
        ScenarioChoice::Sustained => vec![("sustained", scenarios::sustained(&overrides), false)],
        ScenarioChoice::Backpressure => {
            vec![("backpressure", scenarios::backpressure(&overrides), false)]
        }
        ScenarioChoice::InsertBatching => {
            vec![(
                "insert-batching",
                scenarios::insert_batching(&overrides),
                false,
            )]
        }
        ScenarioChoice::All => vec![
            ("baseline", scenarios::baseline(&overrides), false),
            (
                "throughput-ramp",
                scenarios::throughput_ramp(&overrides),
                ramp_stop,
            ),
            ("batch-sizes", scenarios::batch_sizes(&overrides), false),
            ("concurrency", scenarios::concurrency(&overrides), false),
            ("backpressure", scenarios::backpressure(&overrides), false),
        ],
    };

    let mut scenario_reports = Vec::new();
    for (name, steps, stop_on_saturation) in plan {
        scenario_reports
            .push(scenarios::run_scenario(&context, name, steps, stop_on_saturation).await?);
    }

    // ----- graceful shutdown -------------------------------------------------
    drop(context);

    let mut post_shutdown_storage = None;
    if let Some(server_arc) = embedded_server.take() {
        match Arc::try_unwrap(server_arc) {
            Ok(server) => {
                println!("\nshutting down embedded pipeline (drain + WAL checkpoint)...");
                server.shutdown().await?;
                let db_path = db_path.clone();
                let info = tokio::task::spawn_blocking(move || db::inspect(&db_path)).await??;
                println!(
                    "final DB size: {} bytes | WAL size: {} bytes",
                    info.database_bytes, info.wal_bytes
                );
                post_shutdown_storage = Some(FinalStorage {
                    note: "captured after graceful shutdown and TRUNCATE checkpoint".to_owned(),
                    storage: info,
                });
            }
            Err(_) => bail!("internal error: embedded server still referenced at shutdown"),
        }
    }

    // ----- results -----------------------------------------------------------
    let output = BenchmarkOutput {
        schema_version: 1,
        environment,
        configuration: ConfigurationSummary {
            scenario: format!("{:?}", args.scenario).to_lowercase(),
            endpoint,
            database_path: db_path.display().to_string(),
            seed: args.seed,
            warmup_secs: args.warmup_secs,
            request_timeout_ms: args.request_timeout_ms,
            body_size_bytes: args.body_size,
            attributes_per_record: args.attributes,
            resources: args.resources,
            drain_timeout_secs: args.drain_timeout_secs,
            overrides_applied: overrides,
            stop_on_saturation_ramp: ramp_stop,
        },
        pre_existing_log_rows: pre_existing_rows,
        scenarios: scenario_reports,
        post_shutdown_storage,
    };

    if let Some(path) = &args.output {
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(path, serde_json::to_vec_pretty(&output)?)?;
        println!("results written to {}", path.display());
    } else {
        println!("{}", serde_json::to_string_pretty(&output)?);
    }

    println!("\nbenchmark complete.");
    Ok(())
}
