// Binaries report progress and results on stdout by design.
#![allow(clippy::print_stdout, clippy::print_stderr)]

mod backup_cli;
mod check_cli;
mod config;
mod gencerts;
mod healthcheck;

use anyhow::{Context, Result};
use otel_sqlite::{Pipeline, PipelineSpecs};
use otel_sqlite_ingress::expand_bind_address;

use config::Config;

fn install_metrics_exporter(address: &str) -> Result<()> {
    use metrics_exporter_prometheus::PrometheusBuilder;

    let socket_addr: std::net::SocketAddr = expand_bind_address(address)
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

fn print_usage() {
    println!(
        "otel-sqlite - OTLP/gRPC to SQLite sink

Usage:
  otel-sqlite [COMMAND] [OPTIONS]

With no command, starts the server. Configuration is read from the TOML
file named by OTEL_SQLITE_CFG_PATH; without it the built-in defaults apply.

Commands:
  healthcheck   probe the local gRPC health service (Docker HEALTHCHECK)
  gen-certs     generate the in-house CA, server and client certificates
  backup        take an online SQLite snapshot (optionally encrypted)
  restore       restore a backup into a clean directory
  verify        run integrity/foreign-key/row-count checks on a database
  check         non-mutating preflight: config, TLS, tokens and DB schema

Options:
  -h, --help    print this help

Each command has its own flags (for example `otel-sqlite backup --db PATH`)."
    );
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    // Subcommands: `healthcheck` is the Docker HEALTHCHECK probe,
    // `gen-certs` bootstraps the in-house PKI, `backup`/`restore`/`verify`
    // provide the online backup contract (the slim runtime image ships no
    // curl/bash/openssl/sqlite3, so the binary must answer for itself), and
    // `check` validates a rollout without mutating anything. Anything else
    // starts the server.
    let mut cli_args = std::env::args().skip(1);
    match cli_args.next().as_deref() {
        Some("--help" | "-h" | "help") => {
            print_usage();
            return Ok(());
        }
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
        Some("check") => {
            let rest = cli_args.collect::<Vec<_>>();
            let args = match check_cli::parse_args(&rest) {
                Ok(args) => args,
                Err(error) => {
                    eprintln!("{error:#}");
                    eprintln!("run `otel-sqlite check --help` for usage");
                    std::process::exit(2);
                }
            };
            if args.help {
                check_cli::print_usage();
                return Ok(());
            }
            if !check_cli::run(&args)? {
                std::process::exit(1);
            }
            return Ok(());
        }
        Some(unknown) => {
            eprintln!("unknown argument `{unknown}`; run `otel-sqlite --help` for usage");
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
            println!(
                "prometheus metrics on {}",
                config::metrics_display_url(address)
            );
        }
        None => println!("prometheus metrics disabled (set metrics_address to enable)"),
    }

    // The one pipeline assembly. Boot covers readiness: Storage::open inside
    // it blocks on migrations, the startup checkpoint and file permissions,
    // so the returned handle is storage-ready the moment boot returns. The
    // handle owns storage, the maintenance worker, the watchdog and the
    // ingest producer from here on.
    let pipeline = Pipeline::boot(PipelineSpecs {
        ingest_queue_capacity: config.ingest_queue_capacity,
        ingress: ingress_config.clone(),
        storage: config.storage_config(),
        maintenance: config.maintenance.clone(),
        watchdog: config.watchdog.clone(),
    })
    .context("failed to boot the pipeline")?;

    println!(
        "ingress listening on {} (sqlite: {})",
        ingress_config.listen_address,
        config.sqlite_path.display()
    );

    let serve_result = pipeline.serve().await;

    // Shutdown order lives behind Pipeline::drain: ingress stopped (serve
    // returned) -> flush barrier -> stop observation -> stop maintenance
    // scheduling -> let the writer drain and exit. Every step runs even if an
    // earlier one failed; see the pipeline module docs for why partial
    // teardown would lose log records buffered in the pipeline.
    let report = pipeline.drain();

    if report.halt_requested {
        eprintln!("watchdog halted ingestion: unrecoverable pipeline fault detected");
    }
    if let Some(refusal) = &report.barrier {
        eprintln!("flush barrier refused during drain: {refusal}");
    }

    println!(
        "shutdown complete: records_written={}, transactions={}, batches_received={}",
        report.stats.records_written,
        report.stats.transactions_committed,
        report.stats.batches_received
    );

    serve_result.context("ingress server terminated unexpectedly")?;
    if let Some(error) = report.watchdog_error {
        return Err(anyhow::Error::new(error).context("watchdog terminated with failure"));
    }
    if let Some(error) = report.maintenance_error {
        return Err(anyhow::Error::new(error).context("maintenance worker terminated with failure"));
    }
    if let Some(error) = report.storage_error {
        return Err(
            anyhow::Error::new(error).context("sqlite storage writer terminated with failure")
        );
    }
    Ok(())
}
