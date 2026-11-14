//! Shared scaffolding for the runtime's long-running periodic threads.
//!
//! The maintenance worker and the watchdog have the same lifecycle shape: a
//! named thread, a shutdown channel, a join handle that maps a panic to a
//! typed error, and a liveness gauge that is reset on unwind. Keeping that
//! shape in one place means their public handles differ only where their
//! behaviour does.

use std::io;
use std::mem::DropGuard;
use std::thread::{self, JoinHandle};

use crossbeam_channel::{Receiver, Sender, TryRecvError};
use thiserror::Error;

/// Runs `body` on a thread with an operator-readable name.
pub(crate) fn spawn_named(
    name: &str,
    body: impl FnOnce() + Send + 'static,
) -> io::Result<JoinHandle<()>> {
    thread::Builder::new().name(name.to_owned()).spawn(body)
}

/// The joined thread panicked.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("periodic thread panicked")]
pub(crate) struct ThreadPanicked;

/// Owns a spawned thread's shutdown signal and join handle.
#[derive(Debug)]
pub(crate) struct WorkerHandle {
    shutdown: Option<Sender<()>>,
    join: Option<JoinHandle<()>>,
}

impl WorkerHandle {
    pub(crate) fn new(shutdown: Sender<()>, join: JoinHandle<()>) -> Self {
        Self {
            shutdown: Some(shutdown),
            join: Some(join),
        }
    }

    /// Signals the thread to stop without waiting for it. Idempotent: a
    /// second call (or one after the thread has exited) is a no-op.
    pub(crate) fn shutdown(&self) {
        if let Some(sender) = &self.shutdown {
            let _ = sender.try_send(());
        }
    }

    /// Signals shutdown and joins the thread.
    pub(crate) fn stop(self) -> Result<(), ThreadPanicked> {
        self.shutdown();
        self.join()
    }

    /// Joins the thread. Dropping every shutdown sender (including this
    /// handle's) also releases a thread waiting for shutdown.
    pub(crate) fn join(mut self) -> Result<(), ThreadPanicked> {
        self.shutdown = None;
        if let Some(handle) = self.join.take() {
            handle.join().map_err(|_| ThreadPanicked)?;
        }
        Ok(())
    }
}

impl Drop for WorkerHandle {
    fn drop(&mut self) {
        // Best-effort: releasing the sender wakes a block-waiting thread even
        // when nobody called `stop()`.
        self.shutdown = None;
    }
}

/// Whether a shutdown was requested or every shutdown sender is gone.
pub(crate) fn shutdown_signalled(shutdown: &Receiver<()>) -> bool {
    !matches!(shutdown.try_recv(), Err(TryRecvError::Empty))
}

/// Publishes a thread's liveness for as long as the guard lives, including
/// unwind paths (a panic must not leave the gauge stuck at 1).
///
/// The struct survives only to host [`ActiveGauge::new`]: a type alias cannot
/// carry an inherent constructor. The retraction is [`DropGuard`]'s, so the
/// publish and retract halves sit next to each other instead of in a separate
/// `Drop` impl. The field is `_`-named because nothing reads it; it exists to
/// be dropped.
pub(crate) struct ActiveGauge {
    _guard: DropGuard<&'static str, fn(&'static str)>,
}

impl ActiveGauge {
    pub(crate) fn new(metric: &'static str) -> Self {
        ::metrics::gauge!(metric).set(1.0);
        Self {
            _guard: DropGuard::new(metric, |metric: &'static str| {
                ::metrics::gauge!(metric).set(0.0);
            }),
        }
    }
}
