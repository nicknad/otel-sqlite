//! Embedded server: boots the real pipeline exactly as the production
//! `otel-sqlite` binary does (see `crates/otel-sqlite/src/main.rs`):
//!
//! ```text
//! tonic gRPC (real TCP) -> real LogsIngress/MetricsIngress handlers
//!     -> bounded ingest channel -> storage insert batcher thread
//!     -> bounded WriteCommand queue -> single Storage writer thread -> SQLite WAL
//! ```
//!
//! Only two things differ from the production process:
//!
//! 1. The gRPC server is driven by a programmatic shutdown channel instead of
//!    waiting for Ctrl+C/SIGTERM (`ingress::serve` blocks on OS signals).
//! 2. The listen address defaults to loopback instead of all interfaces.
//!
//! All request handling, mapping, insert batching, queue backpressure and
//! SQLite persistence code paths are byte-for-byte the production implementations.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use otel_sqlite_core::storage::{IngestMessage, InsertBatcherConfig};
use otel_sqlite_ingress::mapping::pb::collector::logs::v1::logs_service_server::LogsServiceServer;
use otel_sqlite_ingress::mapping::pb::collector::metrics::v1::metrics_service_server::MetricsServiceServer;
use otel_sqlite_ingress::{IngressConfig, LogsIngress, MetricsIngress};
use otel_sqlite_storage::{Storage, StorageConfig};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tonic::transport::Server;

/// Production defaults mirrored from `crates/otel-sqlite/src/config.rs`.
pub mod production_defaults {
    use super::{Duration, IngressConfig, InsertBatcherConfig, PathBuf, StorageConfig};
    use otel_sqlite_core::storage::{DurabilityMode, SyncMode};

    pub const INGEST_QUEUE_CAPACITY: usize = 50_000;
    pub const COMMAND_QUEUE_CAPACITY: usize = 50_000;
    pub const MAX_BATCH_RECORDS: usize =
        otel_sqlite_core::storage::DEFAULT_MAX_INSERT_BATCH_RECORDS;
    pub const MAX_BATCH_AGE: Duration = otel_sqlite_core::storage::DEFAULT_MAX_INSERT_BATCH_AGE;

    /// Wall-clock bound for each phase of `EmbeddedServer::shutdown`. The
    /// production binary relies on OS process teardown; an embedded harness
    /// must fail loudly instead of hanging when a phase never completes.
    pub(crate) const SHUTDOWN_PHASE_TIMEOUT: Duration = Duration::from_secs(60);

    pub fn ingress_config(listen_address: String) -> IngressConfig {
        IngressConfig {
            listen_address,
            max_recv_msg_size: 16 * 1024 * 1024,
            max_concurrent_streams: 256,
            shutdown_timeout: Duration::from_secs(30),
            max_records_per_request: 100_000,
            durability_mode: DurabilityMode::Commit,
            tls: None,
            auth: None,
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

    pub fn storage_config(sqlite_path: PathBuf) -> StorageConfig {
        StorageConfig {
            sqlite_path,
            insert_batcher: InsertBatcherConfig::new(MAX_BATCH_RECORDS, MAX_BATCH_AGE),
            command_queue_capacity: COMMAND_QUEUE_CAPACITY,
            retention: None,
            synchronous: sync_mode(),
            startup_timeout: otel_sqlite_storage::DEFAULT_STARTUP_TIMEOUT,
            shutdown_timeout: otel_sqlite_storage::DEFAULT_SHUTDOWN_TIMEOUT,
        }
    }
}

pub struct EmbeddedServer {
    endpoint: String,
    db_path: PathBuf,
    ingest_tx: Option<otel_sqlite_ingress::IngestSender>,
    storage: Option<Storage>,
    shutdown_tx: watch::Sender<bool>,
    serve_handle: JoinHandle<anyhow::Result<()>>,
}

impl EmbeddedServer {
    /// Start the full pipeline. Mirrors `crates/otel-sqlite/src/main.rs`.
    pub async fn start(listen_addr: &str, data_dir: &Path) -> anyhow::Result<Self> {
        let config = production_defaults::ingress_config(listen_addr.to_owned());
        let storage_config = production_defaults::storage_config(data_dir.join("otel-logs.db"));

        let addr: SocketAddr = config.socket_addr()?;
        let config = Arc::new(config);

        let (ingest_tx, receiver) =
            otel_sqlite_ingress::channel(production_defaults::INGEST_QUEUE_CAPACITY);
        let storage = Storage::open(receiver, storage_config)
            .map_err(|error| anyhow::anyhow!("failed to start sqlite storage: {error}"))?;
        let commit_ledger = storage.commit_ledger();

        let logs_service = LogsServiceServer::new(LogsIngress::new(
            ingest_tx.clone(),
            Arc::clone(&config),
            Arc::clone(&commit_ledger),
        ))
        .max_decoding_message_size(config.max_recv_msg_size);
        let metrics_service = MetricsServiceServer::new(MetricsIngress::new(
            ingest_tx.clone(),
            Arc::clone(&config),
            commit_ledger,
        ))
        .max_decoding_message_size(config.max_recv_msg_size);

        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let max_concurrent_streams = config.max_concurrent_streams;
        let serve_handle = tokio::spawn(async move {
            Server::builder()
                .max_concurrent_streams(max_concurrent_streams)
                .add_service(logs_service)
                .add_service(metrics_service)
                .serve_with_shutdown(addr, async move {
                    let _ = shutdown_rx.changed().await;
                })
                .await
                .map_err(|error| anyhow::anyhow!("gRPC serve failed: {error}"))
        });

        wait_until_listening(addr).await?;
        // `Storage::open` blocks until the writer finished bootstrapping
        // (database open, schema migrated), so no extra readiness wait is
        // needed here.
        let endpoint = format!("http://{addr}");

        Ok(Self {
            endpoint,
            db_path: data_dir.join("otel-logs.db"),
            ingest_tx: Some(ingest_tx),
            storage: Some(storage),
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
    /// Bounded by design. The barrier is a plain command on the same bounded
    /// ingest queue as record chunks, so injecting it can block when the
    /// pipeline is saturated — and blocks *forever* if the batcher died while
    /// the queue stays open. The wrapper's control send is serialized with
    /// request reservations, so the barrier can never steal a slot a concurrent
    /// all-or-nothing enqueue is relying on. This method therefore:
    ///
    /// 1. refuses up front when a pipeline thread is already dead (the
    ///    stranded-record count is reported, not silently ignored),
    /// 2. sends with `send_timeout` in a retry loop until `budget` expires,
    /// 3. reports a disconnected queue instead of discarding the error.
    ///
    /// Callers must give this the same deadline they give the subsequent
    /// drain poll so one overall budget covers the whole drain phase.
    pub fn flush(&self, budget: Duration) -> anyhow::Result<()> {
        let Some(tx) = &self.ingest_tx else {
            anyhow::bail!("embedded server already shut down");
        };
        let started = std::time::Instant::now();

        if let Some(sample) = self.health_sample()
            && (!sample.writer_running || !sample.batcher_running)
        {
            let stats = self.stats();
            anyhow::bail!(
                "cannot flush: storage worker dead before barrier (writer_running={}, \
                 batcher_running={}, records_written={}, dropped_records={}, errors={})",
                sample.writer_running,
                sample.batcher_running,
                stats.map_or(0, |s| s.records_written),
                stats.map_or(0, |s| s.dropped_records),
                stats.map_or(0, |s| s.errors),
            );
        }

        loop {
            match tx.send_timeout(IngestMessage::Flush, Duration::from_millis(250)) {
                Ok(()) => return Ok(()),
                Err(otel_sqlite_ingress::SendTimeoutFailure::Timeout) => {
                    if started.elapsed() >= budget {
                        anyhow::bail!(
                            "flush barrier not accepted within {:.1}s (ingest queue depth {}); \
                             the batcher stopped consuming without disconnecting",
                            budget.as_secs_f64(),
                            tx.len(),
                        );
                    }
                }
                Err(otel_sqlite_ingress::SendTimeoutFailure::Disconnected) => {
                    anyhow::bail!(
                        "flush barrier rejected: ingest queue disconnected (batcher gone) with \
                         {} chunks stranded inside",
                        tx.len(),
                    );
                }
            }
        }
    }

    /// Storage-side counters snapshot.
    pub fn stats(&self) -> Option<otel_sqlite_storage::StorageStatsSnapshot> {
        self.storage.as_ref().map(Storage::stats)
    }

    /// Liveness/progress evidence for the storage threads, or `None` after
    /// shutdown. Cheap lock-free read; safe to poll from anywhere.
    pub fn health_sample(&self) -> Option<otel_sqlite_storage::StorageHealthSample> {
        self.storage
            .as_ref()
            .map(|storage| storage.health().sample())
    }

    /// Graceful stop mirroring the production Ctrl+C path:
    /// stop accepting requests -> drop ingest senders (batcher drains its
    /// input, flushes partial batches and closes the command queue)
    /// -> join both threads (writer drains queued commands, truncates WAL).
    ///
    /// Every await is bounded: a wedged gRPC drain or a storage thread that
    /// never finishes must surface as an error naming the phase instead of
    /// hanging this process forever. (The underlying threads are leaked on
    /// such an error; the harness exits non-zero immediately afterwards.)
    pub async fn shutdown(mut self) -> anyhow::Result<()> {
        use production_defaults::SHUTDOWN_PHASE_TIMEOUT;

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

        drop(self.ingest_tx.take());
        if let Some(mut storage) = self.storage.take() {
            let join = tokio::task::spawn_blocking(move || storage.join());
            tokio::time::timeout(SHUTDOWN_PHASE_TIMEOUT, join)
                .await
                .map_err(|_| {
                    anyhow::anyhow!(
                        "storage threads did not finish draining within \
                         {SHUTDOWN_PHASE_TIMEOUT:?}; writer or batcher is wedged"
                    )
                })?
                .map_err(|error| anyhow::anyhow!("storage join task panicked: {error}"))?
                .map_err(|error| anyhow::anyhow!("storage failed during shutdown: {error}"))?;
        }
        Ok(())
    }
}

async fn wait_until_listening(addr: SocketAddr) -> anyhow::Result<()> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        if tokio::time::Instant::now() >= deadline {
            anyhow::bail!("embedded server did not start listening within 15s");
        }
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

// NOTE: no schema-readiness polling is needed here. `Storage::open` blocks
// until the writer finished bootstrapping (database open, schema migrated),
// so the embedded server is storage-ready the moment `start()` returns.
