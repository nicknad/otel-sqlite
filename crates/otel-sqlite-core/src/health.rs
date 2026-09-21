//! Pipeline health evidence vocabulary.
//!
//! [`PipelineSample`] is the point-in-time evidence both sides of the
//! pipeline agree on: the storage pipeline publishes it (thread liveness,
//! progress ages, queue pressure, durability-ticket progress) and the runtime
//! watchdog evaluates it. Keeping the type in this shared crate lets storage
//! produce it directly without the watchdog depending on the storage crate.

/// One observation of pipeline health evidence.
///
/// Fields are individually atomic reads plus two short ledger lock
/// acquisitions; they are diagnostics for a watchdog evaluator, not a
/// transactional snapshot. No I/O and no blocking.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PipelineSample {
    /// Whether the SQLite writer thread is still running.
    pub writer_running: bool,
    /// Whether the insert batcher thread is still running.
    pub batcher_running: bool,
    /// Milliseconds since the writer last executed a command successfully.
    pub writer_idle_ms: u64,
    /// Milliseconds since the batcher last received input or submitted a batch.
    pub batcher_idle_ms: u64,
    /// Depth of the bounded write-command queue.
    pub queue_depth: usize,
    /// Capacity of that queue; `0` disables the saturation rule.
    pub queue_capacity: usize,
    /// Records buffered inside the insert batcher (pending, unsent).
    pub pending_records: usize,
    /// Issued durability tickets that have neither completed nor voided.
    /// In-flight records account for some of these; a ticket that stays
    /// outstanding while nothing is queued or buffered is a leak that will
    /// hold every durable ack behind it forever.
    pub outstanding_tickets: u64,
    /// Milliseconds since the contiguous commit watermark last advanced.
    /// Combined with `outstanding_tickets` this detects a stuck durability
    /// watermark.
    pub watermark_idle_ms: u64,
    /// Whether the writer is currently executing a maintenance-class command
    /// (checkpoint, prune, vacuum, FTS rebuild, quota eviction). Insert
    /// commits legitimately pause during that window.
    pub maintenance_in_progress: bool,
    /// Milliseconds since the current maintenance command started; `0` when
    /// none is in progress.
    pub maintenance_elapsed_ms: u64,
}

impl PipelineSample {
    /// Work accepted by the pipeline but not yet persisted: queued commands
    /// plus records still buffered in the insert batcher.
    pub fn pending_work(&self) -> usize {
        self.queue_depth.saturating_add(self.pending_records)
    }
}
