//! The only way the maintenance worker touches the pipeline: a command sink.
//!
//! The sink abstraction keeps the worker decoupled from both SQLite and the
//! concrete channel implementation. In production it wraps the crossbeam
//! producer handle of the existing bounded command queue; tests substitute
//! in-memory fakes, which enforces the architectural boundary (no database is
//! ever created to exercise scheduling logic).

use crossbeam_channel::{Sender, TrySendError};
use otel_sqlite_core::storage::WriteCommand;

/// Why a command could not be enqueued.
#[derive(Debug)]
pub enum EnqueueError {
    /// Queue at capacity: backpressure. The operation stays due and is
    /// retried later according to the configured retry policy.
    Full(Box<WriteCommand>),
    /// Every consumer is gone (writer terminated). Terminal for the worker.
    Disconnected(Box<WriteCommand>),
}

/// Destination for maintenance commands.
///
/// Implemented by the crossbeam producer handle of the real command queue;
/// fakes implement it in unit tests. Non-blocking by contract: schedulers use
/// this to tolerate temporary queue pressure instead of blocking ingestion.
pub trait CommandSink: Send + Sync + 'static {
    fn try_send(&self, command: WriteCommand) -> Result<(), EnqueueError>;
}

impl CommandSink for Sender<WriteCommand> {
    fn try_send(&self, command: WriteCommand) -> Result<(), EnqueueError> {
        match Sender::try_send(self, command) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(command)) => Err(EnqueueError::Full(Box::new(command))),
            Err(TrySendError::Disconnected(command)) => {
                Err(EnqueueError::Disconnected(Box::new(command)))
            }
        }
    }
}
