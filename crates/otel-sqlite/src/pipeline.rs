//! One home for booting the assembled pipeline and draining it to
//! completion.
//!
//! Three call sites used to re-assemble this pipeline by hand: the production
//! binary (`crates/otel-sqlite/src/main.rs`), the embedded end-to-end harness
//! (`tests/otel-sqlite-e2e/src/server.rs`, which silently omitted the
//! watchdog and the maintenance worker, so no e2e scenario could exercise a
//! halt or a purge cadence), and assorted test wiring. Each re-learned the
//! same two protocols, and each could drift. [`Pipeline::boot`] and
//! [`Pipeline::drain`] are those two protocols, written once.
//!
//! ```text
//! OTLP ──▶ ingress handlers ──▶ ingest queue ──▶ insert batcher ─┐
//!                                 (the handle keeps one producer  ├─▶ command queue ──▶ SQLite writer ──▶ SQLite
//!                                 clone for barriers and drain)  │
//! maintenance worker (runtime) ─────────────────────────────────┘
//! watchdog (runtime) ──(atomic evidence)──▶ halt ──▶ serve / harness transport
//! ```
//!
//! # Ownership
//!
//! [`Pipeline`] owns every long-running piece started at boot: the storage
//! handle (insert batcher + single SQLite writer), the maintenance worker's
//! and the watchdog's control handles, the watchdog's halt signal, the commit
//! ledger, and one producer clone of the ingest queue used for flush barriers
//! and closed by the drain. Dropping the handle is not a drain — call
//! [`Pipeline::drain`], which is bounded and reports every failure phase.
//!
//! # Boot and readiness
//!
//! [`Pipeline::boot`] runs the assembly in the production order: ingest
//! channel, storage, commit ledger, maintenance worker, watchdog. Booting
//! blocks inside `Storage::open` until the writer finished bootstrapping
//! (database open, migrations, startup checkpoint, file hardening), so a
//! successful boot means storage is ready for traffic. A boot that fails
//! partway stops and joins whatever had already started before returning the
//! error — no thread is left holding a queue open.
//!
//! # The drain contract
//!
//! [`Pipeline::drain`], called once the transport stopped, performs in
//! order:
//!
//! 1. a bounded flush barrier ([`Pipeline::flush`]),
//! 2. stopping the watchdog (halt evidence captured first),
//! 3. stopping the maintenance worker,
//! 4. dropping the last ingest producer so the batcher drains its input and
//!    closes the command queue,
//! 5. a bounded join of the storage threads within the shutdown budget
//!    captured from `StorageConfig::shutdown_timeout`, then a final stats
//!    snapshot.
//!
//! Every step runs even if an earlier one failed: aborting mid-drain would
//! leave the maintenance worker holding the command queue open and the writer
//! undrained, so log records already buffered in the pipeline would be lost
//! when the process exits. All evidence lands in the returned
//! [`DrainReport`]. The barrier refuses instead of blocking when the
//! pipeline is already dead ([`FlushRefused`]) — a dead batcher can never
//! consume a barrier — and reports the stranded chunks; the drain still
//! joins, because a failed pipeline must be reaped, not abandoned.
//!
//! # Two transports, one assembly
//!
//! [`Pipeline::serve`] is the production transport: it blocks until an OS
//! shutdown signal or the watchdog's halt, with the same graceful drain and
//! deadline the binary answers to Ctrl+C. Harnesses that must control their
//! own transport lifecycle — the embedded e2e server, which stops serving on
//! its own signal — instead take [`Pipeline::logs_service`] (the production
//! request contract, including bearer auth) and [`Pipeline::halt_signal`],
//! and stop serving when either they or the watchdog say so. Both paths boot
//! and drain through this module.
//!
//! # Coverage through this interface
//!
//! The embedded e2e harness boots the real assembly here, so its scenarios
//! run the real maintenance worker and watchdog; the previously unreachable
//! purge cadence is asserted in `tests/otel-sqlite-e2e/tests/e2e.rs` through
//! a runtime-policy knob, alongside watchdog non-halt evidence. Tests that
//! drive `Storage` directly with raw `Flush` messages (the storage crate's
//! own tests) are deliberately left below this seam: they exercise
//! sub-assemblies, not the assembly.

use std::sync::Arc;
use std::time::{Duration, Instant};

use otel_sqlite_core::PipelineSample;
use otel_sqlite_core::storage::{CommitLedger, IngestMessage};
use otel_sqlite_ingress::{
    AuthError, BearerInterceptor, IngestSender, IngressConfig, IngressError, IngressLogsService,
    SendTimeoutFailure, ServingCheck, logs_service, serve_with_shutdown,
};
use otel_sqlite_runtime::{
    HealthSampleSource, MaintenanceConfig, MaintenanceHandle, MaintenanceWorker,
    MaintenanceWorkerError, Watchdog, WatchdogConfig, WatchdogError, WatchdogHandle,
};
use otel_sqlite_storage::{
    Storage, StorageConfig, StorageError, StorageHealth, StorageStatsSnapshot,
};
use thiserror::Error;
use tokio::sync::watch;

/// Inputs to [`Pipeline::boot`]: the four crate configs plus the ingest
/// queue capacity.
///
/// The binary builds this from its `Config`; harnesses build it from the
/// crates' public defaults plus explicit test overrides. No value is
/// defaulted or re-derived here — what the caller passes is what boots, so a
/// harness can never silently diverge from the values it means to test.
pub struct PipelineSpecs {
    /// Capacity of the bounded ingest queue, in messages.
    pub ingest_queue_capacity: usize,
    /// OTLP ingress: transport, limits, durability mode, TLS, auth.
    pub ingress: IngressConfig,
    /// Storage pipeline: batcher, command queue, writer, database path.
    pub storage: StorageConfig,
    /// Maintenance worker schedule (purge/checkpoint/optimize/vacuum).
    pub maintenance: MaintenanceConfig,
    /// Watchdog observation policy and halt behaviour.
    pub watchdog: WatchdogConfig,
}

/// Why the pipeline could not be booted.
#[derive(Debug, Error)]
pub enum PipelineError {
    /// `Storage::open` gates on migrations, the startup checkpoint and file
    /// permissions; it failed before the pipeline was ready.
    #[error("failed to start sqlite storage: {0}")]
    Storage(#[source] StorageError),
    /// The maintenance worker thread could not be spawned.
    #[error("failed to start the maintenance worker: {0}")]
    Maintenance(#[source] MaintenanceWorkerError),
    /// The watchdog configuration cannot produce meaningful verdicts.
    #[error("invalid watchdog configuration: {0}")]
    WatchdogConfig(#[source] WatchdogError),
    /// The watchdog thread could not be spawned.
    #[error("failed to start the watchdog: {0}")]
    WatchdogSpawn(#[source] WatchdogError),
}

/// Why a flush barrier could not be placed.
///
/// Every variant is a refusal, not a silent loss: the caller learns whether
/// the pipeline was already dead, wedged, or disconnected, and how many
/// chunks stayed stranded inside.
#[derive(Debug, Error)]
pub enum FlushRefused {
    /// A storage thread was already dead, so the barrier could never be
    /// consumed. The stranded-chunk count and the final counters are
    /// reported instead of silently ignored.
    #[error(
        "cannot flush: storage worker dead before barrier (writer_running={writer_running}, \
         batcher_running={batcher_running}, records_written={stats.records_written}, \
         dropped_records={stats.dropped_records}, errors={stats.errors})"
    )]
    DeadPipeline {
        writer_running: bool,
        batcher_running: bool,
        stranded_chunks: usize,
        stats: StorageStatsSnapshot,
    },
    /// The batcher stopped consuming without disconnecting: the queue stayed
    /// full until the budget expired.
    #[error(
        "flush barrier not accepted within {:.1}s (ingest queue depth {queue_depth}); \
         the batcher stopped consuming without disconnecting",
        budget.as_secs_f64()
    )]
    BudgetExhausted {
        /// The wall-clock budget that expired.
        budget: Duration,
        queue_depth: usize,
    },
    /// The ingest queue disconnected while the barrier was in flight: the
    /// batcher is gone and everything still queued is stranded.
    #[error(
        "flush barrier rejected: ingest queue disconnected (batcher gone) with \
         {stranded_chunks} chunks stranded inside"
    )]
    Disconnected { stranded_chunks: usize },
}

/// Everything the drain learned, in the order its phases ran.
///
/// A clean drain carries no errors and no refusal. Callers decide how to
/// surface each field; the binary prints the counters, warns on the halt, and
/// propagates the component errors with its own contexts.
#[derive(Debug)]
pub struct DrainReport {
    /// The flush barrier's refusal detail, `None` when it landed.
    pub barrier: Option<FlushRefused>,
    /// The watchdog sent the one-shot ingestion halt during this run.
    pub halt_requested: bool,
    /// The watchdog thread failed to stop cleanly.
    pub watchdog_error: Option<WatchdogError>,
    /// The maintenance worker thread failed to stop cleanly.
    pub maintenance_error: Option<MaintenanceWorkerError>,
    /// The storage threads failed or did not finish within the shutdown
    /// budget.
    pub storage_error: Option<StorageError>,
    /// Final pipeline counters, captured after the join.
    pub stats: StorageStatsSnapshot,
}

/// Adapts the storage health probe to both runtime evidence contracts: the
/// watchdog's [`HealthSampleSource`] and the gRPC [`ServingCheck`].
///
/// The runtime and ingress crates stay decoupled from storage; this adapter
/// is the only place that knows both sides. Storage already produces the
/// shared [`PipelineSample`] from lock-free atomic loads, so both consumers
/// can sample even while the pipeline is wedged.
struct StoragePipelineHealth(StorageHealth);

impl HealthSampleSource for StoragePipelineHealth {
    fn sample(&self) -> PipelineSample {
        self.0.sample()
    }
}

impl ServingCheck for StoragePipelineHealth {
    fn is_serving(&self) -> bool {
        let sample = self.0.sample();
        sample.writer_running && sample.batcher_running
    }
}

/// The assembled pipeline handle: everything boot started, in one value.
///
/// The interface is deliberately the test surface — boot, observe, drain:
/// callers never touch raw channels, spawn workers, or join threads. See the
/// module docs for the ownership, readiness and drain contracts.
pub struct Pipeline {
    storage: Storage,
    maintenance: MaintenanceHandle,
    watchdog: WatchdogHandle,
    /// Cheap, cloneable probe; the watchdog and the serving checks each hold
    /// an adapter over the same atomics, so all evidence agrees.
    health: StorageHealth,
    /// The ingest producer kept for flush barriers and closed by the drain.
    ingest: IngestSender,
    ledger: Arc<CommitLedger>,
    ingress: IngressConfig,
    halt: watch::Receiver<bool>,
    drain_budget: Duration,
}

impl Pipeline {
    /// Boots the assembled pipeline in the production order: ingest channel,
    /// `Storage::open` (readiness gate), commit ledger, maintenance worker,
    /// watchdog.
    ///
    /// A boot that fails partway is undone: pieces already started are
    /// stopped and joined within the configured shutdown budget before the
    /// error returns.
    pub fn boot(specs: PipelineSpecs) -> Result<Self, PipelineError> {
        let PipelineSpecs {
            ingest_queue_capacity,
            ingress,
            storage: storage_config,
            maintenance,
            watchdog: watchdog_config,
        } = specs;

        let (ingest, receiver) = otel_sqlite_ingress::channel(ingest_queue_capacity);
        // Kept out of `storage_config`: it is moved into `Storage::open`,
        // but the budget is needed again when draining the pipeline below.
        let drain_budget = storage_config.shutdown_timeout;
        let mut storage =
            Storage::open(receiver, storage_config).map_err(PipelineError::Storage)?;

        // Durable acks: ingress holds export responses until the writer
        // publishes the matching commit tickets on this shared ledger.
        let ledger = storage.commit_ledger();

        // The maintenance worker is another producer of the same command
        // queue; the single SQLite writer keeps executing everything.
        let maintenance = match MaintenanceWorker::new(storage.producer(), maintenance).spawn() {
            Ok(handle) => handle,
            Err(error) => {
                abort_boot(&mut storage, ingest, drain_budget);
                return Err(PipelineError::Maintenance(error));
            }
        };

        // The watchdog observes pipeline evidence and may halt ingestion on
        // an unhealthy verdict; graceful drain and joins stay here.
        let (halt_tx, halt) = watch::channel(false);
        let health = storage.health();
        let watchdog = match Watchdog::new(
            StoragePipelineHealth(health.clone()),
            watchdog_config,
            halt_tx,
        ) {
            Ok(watchdog) => match watchdog.spawn() {
                Ok(handle) => handle,
                Err(error) => {
                    let _ = maintenance.stop();
                    abort_boot(&mut storage, ingest, drain_budget);
                    return Err(PipelineError::WatchdogSpawn(error));
                }
            },
            Err(error) => {
                let _ = maintenance.stop();
                abort_boot(&mut storage, ingest, drain_budget);
                return Err(PipelineError::WatchdogConfig(error));
            }
        };

        Ok(Self {
            storage,
            maintenance,
            watchdog,
            health,
            ingest,
            ledger,
            ingress,
            halt,
            drain_budget,
        })
    }

    /// Production transport: binds the configured listener (TLS when
    /// configured) and blocks until an OS shutdown signal or the watchdog's
    /// halt, then drains in-flight exports within the ingress shutdown
    /// deadline.
    ///
    /// The queue handed to the server is a producer clone; the handle keeps
    /// its own for [`Pipeline::flush`] and [`Pipeline::drain`].
    pub async fn serve(&self) -> Result<(), IngressError> {
        serve_with_shutdown(
            self.ingress.clone(),
            self.ingest.clone(),
            self.halt.clone(),
            Arc::clone(&self.ledger),
            Arc::new(StoragePipelineHealth(self.health.clone())),
        )
        .await
    }

    /// Builds a fresh production logs service (gzip acceptance, decode-size
    /// cap, bearer auth, mapping budget) for callers that drive their own
    /// transport, such as the embedded e2e harness.
    ///
    /// Fallible for the same reason the production serve path is: the
    /// configured bearer-token file is read when the service is built.
    /// [`Pipeline::serve`] constructs its own equivalent internally.
    pub fn logs_service(&self) -> Result<IngressLogsService, IngressError> {
        let interceptor = BearerInterceptor::new(
            self.ingress
                .auth
                .as_ref()
                .map(|auth| auth.token_file.as_path()),
        )
        .map_err(|error: AuthError| IngressError::Auth(error.to_string()))?;
        Ok(logs_service(
            &self.ingress,
            self.ingest.clone(),
            Arc::clone(&self.ledger),
            interceptor,
        ))
    }

    /// The watchdog's one-shot halt signal, for harnesses that own the
    /// transport and must stop serving when the watchdog halts ingestion.
    pub fn halt_signal(&self) -> watch::Receiver<bool> {
        self.halt.clone()
    }

    /// Storage-side counters snapshot.
    pub fn stats(&self) -> StorageStatsSnapshot {
        self.storage.stats()
    }

    /// Liveness/progress evidence for the storage threads. Cheap lock-free
    /// read; safe to poll from anywhere, even a wedged pipeline.
    pub fn health_sample(&self) -> PipelineSample {
        self.health.sample()
    }

    /// Whether the watchdog sent the halt signal during this run.
    pub fn watchdog_halted(&self) -> bool {
        self.watchdog.requested_halt()
    }

    /// Bounded flush barrier: everything ingested before this call reaches
    /// SQLite before anything ingested afterwards is observed.
    ///
    /// The barrier is a plain command on the same bounded ingest queue as
    /// record chunks, so injecting it can block when the pipeline is
    /// saturated — and blocks forever if the batcher died while the queue
    /// stays open. The barrier's control send is serialized with request
    /// reservations, so it can never steal a slot a concurrent all-or-nothing
    /// enqueue is relying on. This call therefore:
    ///
    /// 1. refuses up front when a pipeline thread is already dead (the
    ///    stranded-chunk count is reported, not silently ignored),
    /// 2. sends with `send_timeout` in a retry loop until `budget`
    ///    expires,
    /// 3. reports a disconnected queue instead of discarding the error.
    ///
    /// Callers must give this the same deadline they give the subsequent
    /// drain poll so one overall budget covers the whole drain phase.
    pub fn flush(&self, budget: Duration) -> Result<(), FlushRefused> {
        let started = Instant::now();

        let sample = self.health.sample();
        if !sample.writer_running || !sample.batcher_running {
            return Err(FlushRefused::DeadPipeline {
                writer_running: sample.writer_running,
                batcher_running: sample.batcher_running,
                stranded_chunks: self.ingest.len(),
                stats: self.storage.stats(),
            });
        }

        loop {
            match self
                .ingest
                .send_timeout(IngestMessage::Flush, Duration::from_millis(250))
            {
                Ok(()) => return Ok(()),
                Err(SendTimeoutFailure::Timeout) => {
                    if started.elapsed() >= budget {
                        return Err(FlushRefused::BudgetExhausted {
                            budget,
                            queue_depth: self.ingest.len(),
                        });
                    }
                }
                Err(SendTimeoutFailure::Disconnected) => {
                    return Err(FlushRefused::Disconnected {
                        stranded_chunks: self.ingest.len(),
                    });
                }
            }
        }
    }

    /// Bounded drain to completion, after the caller stopped the transport.
    ///
    /// Runs every phase even if an earlier one failed — see the module docs
    /// for the ordering argument — and returns the [`DrainReport`] so the
    /// caller can print counters and propagate each failure with its own
    /// context. The join is bounded by the storage shutdown budget captured
    /// at boot, so a wedged SQLite cannot hang process shutdown forever; the
    /// timeout is reported after teardown so the OS reaps the stuck threads
    /// and the supervisor restarts a clean process.
    pub fn drain(mut self) -> DrainReport {
        // Barrier first: the last ordering point while both halves of the
        // pipeline still exist. After the transport stopped it is a cheap
        // no-op command; if the pipeline died mid-flight it is refused with
        // evidence instead of blocking.
        let barrier = self.flush(self.drain_budget).err();

        // Shutdown order: ingress stopped (caller) -> stop observation ->
        // stop maintenance scheduling -> let the writer drain and exit.
        let halt_requested = self.watchdog.requested_halt();
        let watchdog_error = self.watchdog.stop().err();
        let maintenance_error = self.maintenance.stop().err();
        // Dropping the last ingest producer closes the ingest queue: the
        // batcher drains its input, flushes partial batches and closes the
        // command queue.
        drop(self.ingest);
        let storage_error = self.storage.join_timeout(self.drain_budget).err();
        let stats = self.storage.stats();

        DrainReport {
            barrier,
            halt_requested,
            watchdog_error,
            maintenance_error,
            storage_error,
            stats,
        }
    }
}

/// Undoes a boot that failed partway: closes the ingest channel so the
/// batcher can exit, then joins both storage threads within the shutdown
/// budget. Failures are swallowed on purpose — the caller only needs the
/// threads gone before the error propagates.
fn abort_boot(storage: &mut Storage, ingest: IngestSender, budget: Duration) {
    drop(ingest);
    let _ = storage.join_timeout(budget);
}
