//! Embedded server: boots the REAL pipeline assembly through
//! `otel_sqlite::Pipeline` — the same boot and drain the production binary
//! uses, including the maintenance worker and the watchdog:
//!
//! ```text
//! tonic gRPC (real TCP) -> IngressLogsService from `Pipeline::logs_service`
//!     (gzip, decode-size/auth/stream limits) -> storage insert batcher thread
//!     -> bounded WriteCommand queue -> single Storage writer thread -> SQLite WAL
//! ```
//!
//! The request contract, the workers and the drain are the production
//! implementations. Only the transport lifecycle and the benchmark profile
//! differ:
//!
//! 1. The gRPC server is driven by a programmatic shutdown channel instead of
//!    waiting for Ctrl+C/SIGTERM (`Pipeline::serve` blocks on OS signals).
//!    It also stops when the real watchdog halts ingestion, mirroring the
//!    production behavior.
//! 2. [`benchmark_profile`] tightens per-record caps and raises stream
//!    concurrency for short benchmark runs; every other value comes from the
//!    crates' public defaults.
//! 3. [`RuntimePolicy`] selects the worker schedules: the default is the
//!    production policy, and tests may override the purge cadence to observe
//!    maintenance work (see `tests/e2e.rs`).

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use otel_sqlite::{Pipeline, PipelineSpecs};
use otel_sqlite_core::storage::InsertBatcherConfig;
use otel_sqlite_ingress::IngressConfig;
use otel_sqlite_runtime::{MaintenanceConfig, WatchdogConfig};
use otel_sqlite_storage::StorageConfig;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tonic::transport::Server;

/// Benchmark pipeline profile.
///
/// The handlers, workers and pipeline wiring are the production
/// implementations, but this profile is deliberately NOT a verbatim copy of
/// the production defaults in `crates/otel-sqlite/src/config.rs`: the
/// per-record caps below are tighter (harness payloads are tiny, and tighter
/// caps make the validation and backpressure paths cheap to reach in short
/// runs) and the stream count is higher (so the concurrency ramp can hold
/// more simultaneous clients). Every value that matches production comes from
/// the crates' own defaults, so it cannot drift.
pub mod benchmark_profile {
    use super::{Duration, IngressConfig, InsertBatcherConfig, PathBuf, StorageConfig};
    use otel_sqlite_core::storage::SyncMode;
    use otel_sqlite_ingress::MAX_MAPPING_CONCURRENCY;
    use otel_sqlite_storage::FtsMode;

    pub const INGEST_QUEUE_CAPACITY: usize = 50_000;
    pub const COMMAND_QUEUE_CAPACITY: usize = otel_sqlite_storage::DEFAULT_COMMAND_QUEUE_CAPACITY;
    pub const MAX_BATCH_RECORDS: usize =
        otel_sqlite_core::storage::DEFAULT_MAX_INSERT_BATCH_RECORDS;
    pub const MAX_BATCH_AGE: Duration = otel_sqlite_core::storage::DEFAULT_MAX_INSERT_BATCH_AGE;

    /// Wall-clock bound for each phase of `EmbeddedServer::shutdown`. The
    /// production binary relies on OS process teardown; an embedded harness
    /// must fail loudly instead of hanging when a phase never completes.
    pub(crate) const SHUTDOWN_PHASE_TIMEOUT: Duration = Duration::from_secs(60);

    /// OTLP ingress for benchmark runs: the crate defaults plus exactly the
    /// overrides that differ from production.
    pub fn ingress_config(listen_address: String) -> IngressConfig {
        IngressConfig {
            listen_address,
            // Production default is 64; raised so ramp scenarios can hold
            // more simultaneous clients.
            max_concurrent_streams: 256,
            // 2 × the raised stream cap, clamped to the accepted maximum.
            mapping_concurrency: MAX_MAPPING_CONCURRENCY,
            // Tighter than the production caps (1_000 / 512 / 64 KiB / 1 MiB)
            // so the validation and backpressure paths are cheap to reach
            // with tiny harness payloads.
            max_attributes_per_record: 128,
            max_attribute_key_bytes: 256,
            max_attribute_value_bytes: 4096,
            max_body_bytes: 65536,
            // Everything else — decode size, shutdown timeout, per-request
            // cap, scope-metadata cap, durability mode, TLS, auth — is the
            // crate default, i.e. the production value.
            ..IngressConfig::default()
        }
    }

    /// SQLite `synchronous` pragma for benchmark runs, overridable with
    /// `OTEL_SQLITE_E2E_SYNC=normal|full` (default `normal`) so A/B
    /// durability comparisons use identical commands.
    pub fn sync_mode() -> SyncMode {
        match std::env::var("OTEL_SQLITE_E2E_SYNC")
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase()
            .as_str()
        {
            "full" => SyncMode::Full,
            _ => SyncMode::Normal,
        }
    }

    /// Distinct insert-command record budget for one writer transaction,
    /// overridable with `OTEL_SQLITE_E2E_GROUP_COMMIT` (default: production
    /// default). `0` restores one transaction per insert command.
    pub fn group_commit_max_records() -> usize {
        std::env::var("OTEL_SQLITE_E2E_GROUP_COMMIT")
            .ok()
            .and_then(|value| value.trim().parse().ok())
            .unwrap_or(otel_sqlite_storage::DEFAULT_GROUP_COMMIT_RECORDS)
    }

    /// Installed log search-index mode, overridable with
    /// `OTEL_SQLITE_E2E_SEARCH_MODE=off|body|full` (default `body`).
    pub fn search_mode() -> FtsMode {
        std::env::var("OTEL_SQLITE_E2E_SEARCH_MODE")
            .ok()
            .and_then(|value| FtsMode::parse(&value))
            .unwrap_or_default()
    }

    pub fn storage_config(sqlite_path: PathBuf) -> StorageConfig {
        StorageConfig {
            sqlite_path,
            insert_batcher: InsertBatcherConfig::new(MAX_BATCH_RECORDS, MAX_BATCH_AGE),
            command_queue_capacity: COMMAND_QUEUE_CAPACITY,
            group_commit_max_records: group_commit_max_records(),
            fts_mode: search_mode(),
            max_db_bytes: None,
            synchronous: sync_mode(),
            startup_timeout: otel_sqlite_storage::DEFAULT_STARTUP_TIMEOUT,
            shutdown_timeout: otel_sqlite_storage::DEFAULT_SHUTDOWN_TIMEOUT,
        }
    }
}

/// Runtime worker policy for the embedded assembly: the maintenance schedule
/// and the watchdog observation policy the production binary runs.
///
/// `EmbeddedServer::start` uses `Default`, which is the production policy
/// (nothing fires within a benchmark run); tests override the purge cadence
/// through `EmbeddedServer::start_with` to observe scheduled maintenance
/// through the real worker.
#[derive(Debug, Clone, Default)]
pub struct RuntimePolicy {
    pub maintenance: MaintenanceConfig,
    pub watchdog: WatchdogConfig,
}

pub struct EmbeddedServer {
    endpoint: String,
    db_path: PathBuf,
    pipeline: Option<Pipeline>,
    shutdown_tx: watch::Sender<bool>,
    serve_handle: JoinHandle<anyhow::Result<()>>,
}

impl EmbeddedServer {
    /// Start the real pipeline with the production worker policy.
    pub async fn start(listen_addr: &str, data_dir: &Path) -> anyhow::Result<Self> {
        Self::start_with(listen_addr, data_dir, RuntimePolicy::default()).await
    }

    /// Start the real pipeline assembly with an explicit worker policy.
    ///
    /// Boots through `otel_sqlite::Pipeline::boot` — the same assembly
    /// `crates/otel-sqlite/src/main.rs` boots — so storage readiness
    /// (migrations, startup checkpoint, permissions) gates the start, and
    /// the maintenance worker and the watchdog run for real.
    pub async fn start_with(
        listen_addr: &str,
        data_dir: &Path,
        policy: RuntimePolicy,
    ) -> anyhow::Result<Self> {
        let ingress_config = benchmark_profile::ingress_config(listen_addr.to_owned());
        let storage_config = benchmark_profile::storage_config(
            data_dir.join(otel_sqlite_storage::DEFAULT_DB_FILENAME),
        );

        let pipeline = Pipeline::boot(PipelineSpecs {
            ingest_queue_capacity: benchmark_profile::INGEST_QUEUE_CAPACITY,
            ingress: ingress_config.clone(),
            storage: storage_config,
            maintenance: policy.maintenance,
            watchdog: policy.watchdog,
        })
        .map_err(|error| anyhow::anyhow!("failed to boot the pipeline assembly: {error}"))?;

        let addr: SocketAddr = ingress_config.socket_addr()?;

        // The harness owns its transport lifecycle: it drives the same
        // production logs service on its own listener and stops it on its own
        // signal — or when the real watchdog halts ingestion.
        let logs = pipeline
            .logs_service()
            .map_err(|error| anyhow::anyhow!("failed to build the logs service: {error}"))?;
        let mut halt_rx = pipeline.halt_signal();
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let max_concurrent_streams = ingress_config.max_concurrent_streams;
        let serve_handle = tokio::spawn(async move {
            Server::builder()
                .max_concurrent_streams(max_concurrent_streams)
                .add_service(logs)
                .serve_with_shutdown(addr, async move {
                    tokio::select! {
                        _ = shutdown_rx.changed() => {},
                        _ = halt_rx.changed() => {},
                    }
                })
                .await
                .map_err(|error| anyhow::anyhow!("gRPC serve failed: {error}"))
        });

        wait_until_listening(addr).await?;
        let endpoint = format!("http://{addr}");

        Ok(Self {
            endpoint,
            db_path: data_dir.join(otel_sqlite_storage::DEFAULT_DB_FILENAME),
            pipeline: Some(pipeline),
            shutdown_tx,
            serve_handle,
        })
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    pub fn db_path(&self) -> &Path {
        &self.db_path
    }

    /// Order barrier: everything ingested before this reaches SQLite before
    /// anything ingested afterwards is observed.
    ///
    /// The barrier protocol — refuse-when-dead, bounded retry,
    /// stranded-chunk reporting — lives in `otel_sqlite::Pipeline::flush`.
    /// Callers must give this the same deadline they give the subsequent
    /// drain poll so one overall budget covers the whole drain phase.
    pub fn flush(&self, budget: Duration) -> anyhow::Result<()> {
        let Some(pipeline) = &self.pipeline else {
            anyhow::bail!("embedded server already shut down");
        };
        pipeline.flush(budget)?;
        Ok(())
    }

    pub fn stats(&self) -> Option<otel_sqlite_storage::StorageStatsSnapshot> {
        self.pipeline.as_ref().map(Pipeline::stats)
    }

    /// Liveness/progress evidence for the storage threads, or `None` after
    /// shutdown. Cheap lock-free read; safe to poll from anywhere.
    pub fn health_sample(&self) -> Option<otel_sqlite_core::PipelineSample> {
        self.pipeline.as_ref().map(Pipeline::health_sample)
    }

    /// Whether the real watchdog halted ingestion during this run; `None`
    /// after shutdown.
    pub fn watchdog_halted(&self) -> Option<bool> {
        self.pipeline.as_ref().map(Pipeline::watchdog_halted)
    }

    /// Graceful stop mirroring the production Ctrl+C path: stop the harness
    /// transport -> drain the real assembly (flush barrier -> stop watchdog ->
    /// stop maintenance -> drop producers -> bounded writer join).
    ///
    /// Every await is bounded: a wedged gRPC drain or a pipeline that never
    /// finishes must surface as an error naming the phase instead of hanging
    /// this process forever. (The underlying threads are leaked on such an
    /// error; the harness exits non-zero immediately afterwards.)
    pub async fn shutdown(mut self) -> anyhow::Result<()> {
        use benchmark_profile::SHUTDOWN_PHASE_TIMEOUT;

        let _ = self.shutdown_tx.send(true);
        let serve_handle = self.serve_handle;
        let served = tokio::time::timeout(SHUTDOWN_PHASE_TIMEOUT, serve_handle)
            .await
            .map_err(|_| {
                anyhow::anyhow!("gRPC server did not stop within {SHUTDOWN_PHASE_TIMEOUT:?}")
            })?;
        if let Err(error) = served {
            return Err(anyhow::anyhow!("gRPC serve failed: {error}"));
        }

        if let Some(pipeline) = self.pipeline.take() {
            let drain = tokio::task::spawn_blocking(move || pipeline.drain());
            let report = tokio::time::timeout(SHUTDOWN_PHASE_TIMEOUT, drain)
                .await
                .map_err(|_| {
                    anyhow::anyhow!(
                        "pipeline drain did not finish within {SHUTDOWN_PHASE_TIMEOUT:?}; writer or batcher is wedged"
                    )
                })?
                .map_err(|error| anyhow::anyhow!("pipeline drain task panicked: {error}"))?;
            if let Some(error) = report.storage_error {
                return Err(anyhow::anyhow!(
                    "sqlite storage writer terminated with failure: {error}"
                ));
            }
            if let Some(error) = report.watchdog_error {
                return Err(anyhow::anyhow!("watchdog terminated with failure: {error}"));
            }
            if let Some(error) = report.maintenance_error {
                return Err(anyhow::anyhow!(
                    "maintenance worker terminated with failure: {error}"
                ));
            }
            if let Some(refusal) = report.barrier {
                return Err(anyhow::anyhow!("drain flush barrier refused: {refusal}"));
            }
        }
        Ok(())
    }
}

async fn wait_until_listening(addr: SocketAddr) -> anyhow::Result<()> {
    let started = tokio::task::spawn_blocking(move || {
        otel_sqlite_test_support::wait_for_tcp(&addr.to_string(), Duration::from_secs(15))
    })
    .await
    .map_err(|error| anyhow::anyhow!("listener readiness task panicked: {error}"))?;
    if !started {
        anyhow::bail!("embedded server did not start listening within 15s");
    }
    Ok(())
}

// `Pipeline::boot` blocks until the writer finished bootstrapping (database
// open, schema migrated), so the embedded server is storage-ready the moment
// `start_with` returns; no extra schema-readiness poll is needed.
