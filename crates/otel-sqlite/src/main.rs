// Binaries report progress and results on stdout by design.
#![allow(clippy::print_stdout, clippy::print_stderr)]

mod backup_cli;
mod config;
mod gencerts;
mod healthcheck;

use anyhow::{Context, Result};
use otel_sqlite_ingress::ServingCheck;
use otel_sqlite_storage::{Storage, StorageHealth};
use tokio::sync::watch;

use config::Config;
use otel_sqlite_runtime::{HealthSampleSource, MaintenanceWorker, PipelineSample, Watchdog};

/// Adapts the storage health probe to the watchdog's evidence contract.
///
/// The watchdog stays decoupled from the storage crate; this mapping is the
/// only place that knows both sides. Reads are lock-free atomic loads, so the
/// watchdog can sample even while the pipeline is wedged.
struct StorageHealthSource(StorageHealth);

impl HealthSampleSource for StorageHealthSource {
    fn sample(&self) -> PipelineSample {
        let sample = self.0.sample();
        PipelineSample {
            writer_running: sample.writer_running,
            batcher_running: sample.batcher_running,
            writer_idle_ms: sample.writer_idle_ms,
            batcher_idle_ms: sample.batcher_idle_ms,
            queue_depth: sample.queue_depth,
            queue_capacity: sample.queue_capacity,
            pending_records: sample.buffered_records,
            outstanding_tickets: sample.outstanding_commit_tickets,
            watermark_idle_ms: sample.watermark_idle_ms,
        }
    }
}

/// Answers gRPC health from live pipeline evidence: serving only while both
/// pipeline threads run.
struct StorageServingCheck(StorageHealth);

impl ServingCheck for StorageServingCheck {
    fn is_serving(&self) -> bool {
        let sample = self.0.sample();
        sample.writer_running && sample.batcher_running
    }
}

fn install_metrics_exporter(address: &str) -> Result<()> {
    use metrics_exporter_prometheus::PrometheusBuilder;

    let socket_addr: std::net::SocketAddr = config::expand_bind_address(address)
        .parse()
        .with_context(|| format!("invalid metrics_address `{address}`"))?;
    // `install` both registers the global recorder and spawns the scrape
    // endpoint on the current tokio runtime (`install_recorder` alone would
    // never bind the port).
    PrometheusBuilder::new()
        .with_http_listener(socket_addr)
        .install()
        .context("failed to start the prometheus metrics endpoint")?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    // Subcommands: `healthcheck` is the Docker HEALTHCHECK probe,
    // `gen-certs` bootstraps the in-house PKI, and `backup`/`restore`/`verify`
    // provide the online backup contract (the slim runtime image ships no
    // curl/bash/openssl/sqlite3, so the binary must answer for itself).
    // Anything else starts the server.
    let mut cli_args = std::env::args().skip(1);
    match cli_args.next().as_deref() {
        Some("healthcheck") => return healthcheck::run_healthcheck().await,
        Some("gen-certs") => {
            let rest = cli_args.collect::<Vec<_>>();
            let args = gencerts::parse_args(&rest)?;
            return gencerts::run(&args);
        }
        Some("backup") => {
            let rest = cli_args.collect::<Vec<_>>();
            let args = backup_cli::parse_backup_args(&rest)?;
            return backup_cli::run_backup(&args);
        }
        Some("restore") => {
            let rest = cli_args.collect::<Vec<_>>();
            let args = backup_cli::parse_restore_args(&rest)?;
            return backup_cli::run_restore(&args);
        }
        Some("verify") => {
            let rest = cli_args.collect::<Vec<_>>();
            let args = backup_cli::parse_verify_args(&rest)?;
            return backup_cli::run_verify(&args);
        }
        Some(unknown) => {
            eprintln!(
                "unknown argument `{unknown}`; usage: otel-sqlite [healthcheck | gen-certs | backup | restore | verify]"
            );
            std::process::exit(2);
        }
        None => {}
    }

    // Defaults first, then the TOML file named by OTEL_SQLITE_CFG_PATH (when
    // set) overlays only the keys it defines.
    let config = Config::from_env().context("failed to load configuration")?;
    match Config::env_config_path() {
        Some(path) => println!("loaded config overrides from {}", path.display()),
        None => println!(
            "{} not set; using built-in defaults",
            config::CONFIG_PATH_ENV_VAR
        ),
    }
    println!(
        "effective configuration: {}",
        config.dump_json().context("failed to dump configuration")?
    );
    let ingress_config = config.ingress_config();

    match &config.metrics_address {
        Some(address) => {
            install_metrics_exporter(address)
                .context("failed to start the prometheus metrics endpoint")?;
            println!("prometheus metrics on http://{address}/metrics");
        }
        None => println!("metrics endpoint disabled (metrics_address = \"off\")"),
    }

    let (queue, receiver) = otel_sqlite_ingress::channel(config.ingest_queue_capacity);
    let storage_config = config.storage_config();
    // Kept out of `storage_config`: it is moved into `Storage::open`, but the
    // shutdown budget is needed again when joining the pipeline below.
    let storage_shutdown_timeout = storage_config.shutdown_timeout;
    let mut storage =
        Storage::open(receiver, storage_config).context("failed to start sqlite storage")?;

    // Durable acks: ingress holds export responses until the writer publishes
    // the matching commit tickets on this shared ledger.
    let commit_ledger = storage.commit_ledger();

    // The maintenance worker is another producer of the same command queue;
    // the single SQLite writer keeps executing everything.
    let maintenance =
        MaintenanceWorker::new(storage.producer(), config.maintenance.clone()).spawn();

    // The watchdog observes pipeline evidence and may halt ingestion on an
    // unhealthy verdict; graceful drain and joins stay in main's hands.
    let (halt_tx, halt_rx) = watch::channel(false);
    let watchdog = Watchdog::new(
        StorageHealthSource(storage.health()),
        config.watchdog.clone(),
        halt_tx,
    )
    .spawn();

    println!("otel-sqlite starting with config: {config:?}");
    println!(
        "ingress listening on {} (sqlite: {})",
        ingress_config.listen_address,
        config.sqlite_path.display()
    );

    let serve_result = otel_sqlite_ingress::serve_with_shutdown(
        ingress_config,
        queue,
        halt_rx,
        commit_ledger,
        std::sync::Arc::new(StorageServingCheck(storage.health())),
    )
    .await;

    // Shutdown order: ingress stopped (serve returned) -> stop observation ->
    // stop maintenance scheduling -> let the writer drain and exit.
    if watchdog.requested_halt() {
        eprintln!("watchdog halted ingestion: unrecoverable pipeline fault detected");
    }
    watchdog
        .stop()
        .context("watchdog terminated with failure")?;
    maintenance
        .stop()
        .context("maintenance worker terminated with failure")?;
    // Bounded drain: a wedged SQLite (stalled I/O, oversized final
    // checkpoint) must not hang process shutdown forever. On timeout the
    // error aborts main, the OS reaps the stuck threads, and the container
    // supervisor restarts a clean process.
    storage
        .join_timeout(storage_shutdown_timeout)
        .context("sqlite storage writer terminated with failure")?;

    let stats = storage.stats();
    println!(
        "shutdown complete: records_written={}, transactions={}, batches_received={}",
        stats.records_written, stats.transactions_committed, stats.batches_received
    );

    serve_result.context("ingress server terminated unexpectedly")?;
    Ok(())
}
