//! Long-running execution units that orchestrate the storage pipeline.

// Unit tests may assert invariants with unwrap(); production code must not.
#![cfg_attr(test, allow(clippy::unwrap_used))]
//!
//! # Maintenance worker
//!
//! The crate currently hosts the [`MaintenanceWorker`]: a dedicated scheduler
//! thread that periodically enqueues maintenance commands onto the *existing*
//! command queue. It is a **producer**, exactly like OTLP ingestion — never a
//! second database worker:
//!
//! ```text
//!                     ┌───────────────────┐
//! OTLP ──▶ Ingress ──▶│                   │
//!                     │   Command Queue   │────▶ SQLite Writer ──▶ SQLite
//! Maintenance Worker ▶│  (bounded mpsc)   │
//!                     └───────────────────┘
//! ```
//!
//! The worker owns **scheduling only**:
//!
//! - it evaluates which maintenance operations are due,
//! - it enqueues them as ordinary [`WriteCommand`]s via a
//!   [`CommandSink`] (in production: the producer handle obtained from
//!   `otel_sqlite_storage::Storage::producer`),
//! - it contains **no SQL** and holds **no SQLite connection**,
//! - it sleeps until its next deadline or shutdown (never busy-loops).
//!
//! The SQLite writer remains the single component that executes every
//! mutation, including maintenance.
//!
//! # Architectural invariants
//!
//! 1. **Single SQLite writer.** Exactly one component (the storage writer)
//!    owns and mutates the SQLite connection.
//! 2. **Commands are the mutation boundary.** All database mutations —
//!    inserts, purges, checkpoints, vacuums — flow through the same bounded
//!    command queue into the writer.
//! 3. **Maintenance contains no SQL.** The worker only builds command values;
//!    statements live exclusively in the storage layer.
//! 4. **No database handles in the worker.** No `Connection`, transaction, or
//!    equivalent is passed to it; this is enforced by its API (`new` accepts a
//!    command sink, not a connection) and its tests run without a database.
//! 5. **One command queue.** Maintenance shares the ingestion queue. A
//!    priority-aware queue may be introduced later if backlogs ever starve
//!    critical maintenance; no second queue/writer is created preemptively.
//! 6. **The scheduler is not a writer.** The worker schedules work; the
//!    writer performs work.
//!
//! # Duplicate suppression and queue pressure
//!
//! Each operation's schedule state lives in the scheduler: an operation is
//! only re-enqueued once its interval has elapsed after a successful enqueue,
//! so at most one outstanding instance normally exists. If the queue is full
//! the operation stays due and is retried after `retry_delay` — maintenance is
//! best-effort and must never flood the queue or kill the worker.
//!
//! # Lifecycle
//!
//! ```text
//! MaintenanceWorker::new(sink, config).spawn()  ─▶ started thread
//!                                                ─▶ periodic enqueue loop
//! handle.stop()                                 ─▶ clean exit + join()
//! ```

//! # Watchdog
//!
//! The crate also hosts the [`Watchdog`]: an observer-only supervisor that
//! periodically samples pipeline evidence (thread liveness, progress ages,
//! pending work, queue depth) and derives a [`HealthState`] verdict. It is
//! deliberately **not** a component manager:
//!
//! - it reads shared atomics instead of asking components questions, so it
//!   cannot block on or deadlock against a wedged writer,
//! - it distinguishes `NO WORK` from `WORK BUT NO PROGRESS` by combining
//!   progress ages with pending-work counters,
//! - on an unhealthy verdict it sends a halt signal that stops OTLP
//!   ingestion so the process can drain and exit; restarting the process is
//!   the external supervisor's job, never the watchdog's.
//!
//! ```text
//! writer/batcher ──(atomic evidence)──▶ Watchdog ──▶ metrics / logs / halt
//! ```
//!
//! # Lifecycle
//!
//! ```text
//! Watchdog::new(source, config, halt).spawn()  ─▶ started thread
//!                                               ─▶ periodic sample/verdict loop
//! unhealthy verdict                             ─▶ one-shot halt, loop ends
//! handle.stop()                                 ─▶ clean exit + join()
//! ```

pub mod config;
mod scheduler;
pub mod sink;
mod watchdog;
mod worker;

pub use config::{MaintenanceConfig, WatchdogConfig};
pub use sink::{CommandSink, EnqueueError};
pub use watchdog::{
    HealthSampleSource, HealthState, PipelineSample, Watchdog, WatchdogError, WatchdogHandle,
};
pub use worker::{MaintenanceHandle, MaintenanceWorker, MaintenanceWorkerError};
