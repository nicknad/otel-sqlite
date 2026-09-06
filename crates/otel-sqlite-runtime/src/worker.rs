//! The maintenance worker: a dedicated scheduler thread.
//!
//! The worker is a producer of the existing command queue — never a second
//! SQLite worker. Its loop block-waits on exactly one of two events (the next
//! schedule deadline or a shutdown signal) via `crossbeam_channel::select!`;
//! there is no polling and no busy loop. When no operation is enabled it waits
//! on shutdown alone.

use std::thread::JoinHandle;
use std::time::Instant;

use crossbeam_channel::{Receiver, Sender, TryRecvError, after, select};
use thiserror::Error;

use crate::config::MaintenanceConfig;
use crate::scheduler::Scheduler;
use crate::sink::CommandSink;

#[derive(Debug, Error)]
pub enum MaintenanceWorkerError {
    #[error("maintenance worker thread panicked")]
    Panicked,
}

/// Scheduler-only maintenance unit.
///
/// Constructed with a [`CommandSink`] — in production the producer handle of
/// the existing command queue (`Storage::producer()`), never a database
/// connection, which makes bypassing the single-writer model unrepresentable
/// in this API.
pub struct MaintenanceWorker {
    sink: Box<dyn CommandSink>,
    config: MaintenanceConfig,
}

impl MaintenanceWorker {
    pub fn new(sink: impl CommandSink, config: MaintenanceConfig) -> Self {
        Self {
            sink: Box::new(sink),
            config,
        }
    }

    /// Starts the worker on its own thread and returns a handle controlling
    /// its lifecycle.
    pub fn spawn(self) -> MaintenanceHandle {
        let (shutdown_tx, shutdown_rx) = crossbeam_channel::bounded::<()>(1);
        let join_handle = std::thread::Builder::new()
            .name("otel-sqlite-maintenance".to_owned())
            .spawn(move || run(self.sink, self.config, shutdown_rx))
            .expect("failed to spawn maintenance worker thread");

        MaintenanceHandle {
            shutdown: Some(shutdown_tx),
            join: Some(join_handle),
        }
    }
}

/// Control handle for a spawned [`MaintenanceWorker`].
#[derive(Debug)]
pub struct MaintenanceHandle {
    shutdown: Option<Sender<()>>,
    join: Option<JoinHandle<()>>,
}

impl MaintenanceHandle {
    /// Signals the worker to stop without waiting for it.
    pub fn shutdown(&self) {
        if let Some(sender) = &self.shutdown {
            let _ = sender.send(());
        }
    }

    /// Signals shutdown and joins the thread. Returns an error if the worker
    /// panicked; scheduling already stopped either way.
    pub fn stop(self) -> Result<(), MaintenanceWorkerError> {
        self.shutdown();
        self.join()
    }

    /// Joins the worker thread. Dropping every shutdown sender (including
    /// this handle's) also releases a worker waiting for shutdown.
    pub fn join(mut self) -> Result<(), MaintenanceWorkerError> {
        self.shutdown = None;
        if let Some(handle) = self.join.take() {
            handle
                .join()
                .map_err(|_| MaintenanceWorkerError::Panicked)?;
        }
        Ok(())
    }
}

impl Drop for MaintenanceHandle {
    fn drop(&mut self) {
        // Best-effort: releasing the sender wakes a block-waiting worker even
        // when nobody called `stop()`.
        self.shutdown = None;
    }
}

fn run(sink: Box<dyn CommandSink>, config: MaintenanceConfig, shutdown: Receiver<()>) {
    // The scheduler owns the schedule; derive the startup log count from it
    // instead of duplicating the enabled-operation rules here.
    let enabled = {
        let scheduler = Scheduler::new(config.clone(), Instant::now());
        scheduler.enabled_task_count()
    };

    tracing::info!(tasks = enabled, "maintenance worker started");
    ::metrics::gauge!("maintenance_worker_active").set(1.0);

    let mut scheduler = Scheduler::new(config, Instant::now());
    loop {
        // Drain a shutdown requested while the previous cycle was running so
        // we never enqueue while shutdown is already pending.
        if shutdown_signalled(&shutdown) {
            break;
        }

        let report = scheduler.tick(Instant::now(), sink.as_ref());
        if report.disconnected {
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

    ::metrics::gauge!("maintenance_worker_active").set(0.0);
    tracing::info!("maintenance worker stopped");
}

fn shutdown_signalled(shutdown: &Receiver<()>) -> bool {
    match shutdown.try_recv() {
        Ok(()) | Err(TryRecvError::Disconnected) => true,
        Err(TryRecvError::Empty) => false,
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use otel_sqlite_core::storage::{MaintenanceOperation, RetentionPolicy, WriteCommand};
    use otel_sqlite_test_support::wait_until;

    use super::*;
    use crate::config::MaintenanceConfig;

    fn config(purge_interval: Option<Duration>) -> MaintenanceConfig {
        MaintenanceConfig {
            retention: Some(Duration::from_secs(60)),
            metric_retention: None,
            purge_interval,
            checkpoint_interval: None,
            checkpoint_mode: otel_sqlite_core::storage::CheckpointMode::Passive,
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
        let handle = MaintenanceWorker::new(queue, config(Some(Duration::from_millis(20)))).spawn();

        assert!(
            wait_until(Duration::from_secs(5), || !receiver.is_empty()),
            "worker must enqueue a purge command once the interval elapses"
        );

        handle.stop().unwrap();

        let prunes: Vec<_> = receiver
            .try_iter()
            .filter_map(|command| match command {
                WriteCommand::Maintenance(MaintenanceOperation::Prune(policy)) => Some(policy),
                _ => None,
            })
            .collect();
        assert!(!prunes.is_empty());
        assert_eq!(
            prunes[0],
            RetentionPolicy::logs_only(Duration::from_secs(60))
        );
    }

    #[test]
    fn shutdown_stops_the_worker_promptly() {
        let (queue, _receiver) = crossbeam_channel::bounded::<WriteCommand>(16);
        let handle =
            MaintenanceWorker::new(queue, config(Some(Duration::from_secs(3_600)))).spawn();
        std::thread::sleep(Duration::from_millis(50));

        let started = Instant::now();
        handle.stop().expect("clean stop");
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "stop must not wait out the next schedule deadline"
        );
    }

    #[test]
    fn closed_command_queue_terminates_the_worker() {
        let (queue, receiver) = crossbeam_channel::bounded::<WriteCommand>(16);
        let handle = MaintenanceWorker::new(queue, config(Some(Duration::from_millis(10)))).spawn();
        drop(receiver); // simulate writer death

        let started = Instant::now();
        handle
            .stop()
            .expect("worker exits cleanly after disconnect");
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "disconnect must terminate the loop instead of spinning forever"
        );
    }
}
