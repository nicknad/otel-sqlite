//! The maintenance worker: a dedicated scheduler thread.
//!
//! The worker is a producer of the existing command queue — never a second
//! SQLite worker. Its loop block-waits on exactly one of two events (the next
//! schedule deadline or a shutdown signal) via `crossbeam_channel::select!`;
//! there is no polling and no busy loop. When no operation is enabled it waits
//! on shutdown alone.

use std::io;
use std::time::Instant;

use crossbeam_channel::{Receiver, Sender, after, select};
use otel_sqlite_core::storage::WriteCommand;
use thiserror::Error;

use crate::config::MaintenanceConfig;
use crate::scheduler::{Scheduler, TickOutcome};
use crate::thread::{ActiveGauge, WorkerHandle, shutdown_signalled, spawn_named};

#[derive(Debug, Error)]
pub enum MaintenanceWorkerError {
    #[error("failed to spawn maintenance worker thread: {0}")]
    Spawn(#[source] io::Error),
    #[error("maintenance worker thread panicked")]
    Panicked,
}

/// Scheduler-only maintenance unit.
///
/// Constructed with the producer handle of the existing bounded command queue
/// (`Storage::producer()`), never a database connection, which makes bypassing
/// the single-writer model unrepresentable in this API.
pub struct MaintenanceWorker {
    sink: Sender<WriteCommand>,
    config: MaintenanceConfig,
}

impl MaintenanceWorker {
    pub fn new(sink: Sender<WriteCommand>, config: MaintenanceConfig) -> Self {
        Self { sink, config }
    }

    /// Starts the worker on its own thread and returns a handle controlling
    /// its lifecycle.
    pub fn spawn(self) -> Result<MaintenanceHandle, MaintenanceWorkerError> {
        let (shutdown_tx, shutdown_rx) = crossbeam_channel::bounded::<()>(1);
        let join_handle = spawn_named("otel-sqlite-maintenance", move || {
            run(self.sink, self.config, shutdown_rx);
        })
        .map_err(MaintenanceWorkerError::Spawn)?;

        Ok(MaintenanceHandle {
            worker: WorkerHandle::new(shutdown_tx, join_handle),
        })
    }
}

/// Control handle for a spawned [`MaintenanceWorker`].
#[derive(Debug)]
pub struct MaintenanceHandle {
    worker: WorkerHandle,
}

impl MaintenanceHandle {
    /// Signals the worker to stop without waiting for it. Idempotent: a
    /// second call (or one after the thread has exited) is a no-op.
    pub fn shutdown(&self) {
        self.worker.shutdown();
    }

    /// Signals shutdown and joins the thread. Returns an error if the worker
    /// panicked; scheduling already stopped either way.
    pub fn stop(self) -> Result<(), MaintenanceWorkerError> {
        self.worker
            .stop()
            .map_err(|_| MaintenanceWorkerError::Panicked)
    }

    /// Joins the worker thread. Dropping every shutdown sender (including
    /// this handle's) also releases a worker waiting for shutdown.
    pub fn join(self) -> Result<(), MaintenanceWorkerError> {
        self.worker
            .join()
            .map_err(|_| MaintenanceWorkerError::Panicked)
    }
}

fn run(sink: Sender<WriteCommand>, config: MaintenanceConfig, shutdown: Receiver<()>) {
    // The scheduler owns the schedule; derive the startup log count from it
    // instead of duplicating the enabled-operation rules here.
    let mut scheduler = Scheduler::new(config, Instant::now());
    let enabled = scheduler.enabled_task_count();

    tracing::info!(tasks = enabled, "maintenance worker started");
    let _active = ActiveGauge::new("maintenance_worker_active");

    loop {
        // Drain a shutdown requested while the previous cycle was running so
        // we never enqueue while shutdown is already pending.
        if shutdown_signalled(&shutdown) {
            break;
        }

        if scheduler.tick(Instant::now(), &sink) == TickOutcome::Disconnected {
            // The writer is gone; there is nowhere to schedule into anymore.
            break;
        }

        if let Some(deadline) = scheduler.next_deadline() {
            let timer = after(deadline.saturating_duration_since(Instant::now()));
            select! {
                recv(shutdown) -> _ => break,
                recv(timer) -> _ => {},
            }
        } else {
            // No periodic task enabled: sleep until shutdown is signalled
            // or every shutdown sender is dropped.
            let _ = shutdown.recv();
            break;
        }
    }

    tracing::info!("maintenance worker stopped");
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use otel_sqlite_core::storage::{CheckpointMode, MaintenanceOperation, WriteCommand};
    use otel_sqlite_test_support::wait_until;

    use super::*;
    use crate::config::PurgeSchedule;

    fn config(purge_interval: Option<Duration>) -> MaintenanceConfig {
        MaintenanceConfig {
            purge: purge_interval.map(|interval| PurgeSchedule {
                retention: Duration::from_secs(60),
                interval,
            }),
            checkpoint_interval: None,
            checkpoint_mode: CheckpointMode::Passive,
            optimize_interval: None,
            vacuum_interval: None,
            rebuild_fts_interval: None,
            retry_delay: Duration::from_millis(100),
        }
    }

    #[test]
    fn worker_enqueues_periodically_without_a_database() {
        // A plain channel is the only dependency: scheduling never needs
        // SQLite. This test enforces the architectural boundary.
        let (queue, receiver) = crossbeam_channel::bounded::<WriteCommand>(16);
        let handle = MaintenanceWorker::new(queue, config(Some(Duration::from_millis(20))))
            .spawn()
            .expect("worker thread spawns");

        assert!(
            wait_until(Duration::from_secs(5), || !receiver.is_empty()),
            "worker must enqueue a purge command once the interval elapses"
        );

        handle.stop().expect("clean stop");

        let prunes: Vec<_> = receiver
            .try_iter()
            .filter_map(|command| match command {
                WriteCommand::Maintenance(MaintenanceOperation::Prune(retention)) => {
                    Some(retention)
                }
                _ => None,
            })
            .collect();
        assert_eq!(prunes.first(), Some(&Duration::from_secs(60)));
    }

    #[test]
    fn shutdown_stops_the_worker_promptly() {
        let (queue, _receiver) = crossbeam_channel::bounded::<WriteCommand>(16);
        let handle = MaintenanceWorker::new(queue, config(Some(Duration::from_secs(3_600))))
            .spawn()
            .expect("worker thread spawns");
        std::thread::sleep(Duration::from_millis(50));

        let (done_tx, done_rx) = crossbeam_channel::bounded(1);
        let joiner = std::thread::spawn(move || {
            let result = handle.stop();
            let _ = done_tx.send(());
            result
        });
        assert!(
            done_rx.recv_timeout(Duration::from_secs(5)).is_ok(),
            "stop must not wait out the next schedule deadline"
        );
        joiner.join().expect("joiner").expect("clean stop");
    }
}
