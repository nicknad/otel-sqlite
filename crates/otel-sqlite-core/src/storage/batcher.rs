//! Storage-owned insert batching.
//!
//! [`InsertBatcher`] converts arbitrary incoming record batches (e.g. OTLP
//! export batches) into storage-sized [`WriteBatch`] commands:
//!
//! * full batches are emitted immediately once `max_batch_records` is reached,
//! * oversized inputs are split into consecutive full batches,
//! * small leftovers are combined with later input,
//! * partial batches are emitted after `max_batch_age` so low-volume traffic still
//!   meets a bounded maximum batching latency.
//!
//! The batcher itself is synchronous and never spawns threads or polls. The
//! owner (the storage batcher driver) waits for either new input or
//! `deadline()` and calls [`InsertBatcher::flush`] when the deadline expires.

use std::time::{Duration, Instant};

use super::batch::WriteBatch;

/// Default storage batch capacity (`max_batch_records`).
///
/// Selected from the Criterion transaction-size sweep (`cargo bench -p
/// otel-sqlite-storage`, `write_batch` group): per-transaction cost fits
/// roughly `45 µs fixed + 5.7 µs/record`; throughput rises 2.5x from 10 to 100
/// records per transaction but only ~1.27x from 100 to 1_000 and flattens
/// completely beyond ~1_000 (~180k records/s single writer). 500 sits safely
/// on that plateau with identical throughput while halving buffered memory and
/// the worst-case flush backlog compared to 1_000.
pub const DEFAULT_MAX_INSERT_BATCH_RECORDS: usize = 500;

/// Default maximum batch age (`max_batch_age`).
pub const DEFAULT_MAX_INSERT_BATCH_AGE: Duration = Duration::from_millis(10);

/// Limits controlling how [`InsertBatcher`] groups records.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InsertBatcherConfig {
    /// Maximum number of records per emitted [`WriteBatch`]. Must be >= 1.
    pub max_batch_records: usize,
    /// Maximum age of the oldest buffered record before a partial batch is
    /// flushed. A zero duration flushes every non-empty buffer at the next
    /// deadline check.
    pub max_batch_age: Duration,
}

impl InsertBatcherConfig {
    /// Creates a configuration, panicking in debug builds when `max_batch_records`
    /// is zero (a batch could then never make progress).
    pub const fn new(max_batch_records: usize, max_batch_age: Duration) -> Self {
        debug_assert!(
            max_batch_records >= 1,
            "max_batch_records must be at least 1"
        );
        Self {
            max_batch_records,
            max_batch_age,
        }
    }
}

impl Default for InsertBatcherConfig {
    fn default() -> Self {
        Self {
            max_batch_records: DEFAULT_MAX_INSERT_BATCH_RECORDS,
            max_batch_age: DEFAULT_MAX_INSERT_BATCH_AGE,
        }
    }
}

/// Result of pushing an incoming batch into [`InsertBatcher`].
///
/// Full batches become ready immediately; ownership of their records is
/// transferred to the caller without cloning. The common cases allocate
/// nothing extra (`Buffered`) or exactly one batch (`Single`).
#[derive(Debug, Default, PartialEq)]
pub enum BatchOutput<T> {
    /// No full batch was produced; records stay buffered awaiting more input
    /// or the age deadline.
    #[default]
    Buffered,
    /// Exactly one full batch became ready.
    Single(WriteBatch<T>),
    /// An oversized input produced several full batches, in order.
    Multiple(Vec<WriteBatch<T>>),
}

impl<T> BatchOutput<T> {
    /// Calls `f` with every ready batch in emission order.
    pub fn for_each(self, mut f: impl FnMut(WriteBatch<T>)) {
        match self {
            Self::Buffered => {}
            Self::Single(batch) => f(batch),
            Self::Multiple(batches) => {
                for batch in batches {
                    f(batch);
                }
            }
        }
    }

    /// Number of ready batches.
    pub fn len(&self) -> usize {
        match self {
            Self::Buffered => 0,
            Self::Single(_) => 1,
            Self::Multiple(batches) => batches.len(),
        }
    }

    /// `true` when no batch became ready.
    pub fn is_empty(&self) -> bool {
        matches!(self, Self::Buffered)
    }
}

/// Accumulates incoming record batches into storage-sized [`WriteBatch`]es.
///
/// Invariant: `deadline` is `Some` if and only if the buffer holds records.
/// The deadline belongs to the oldest buffered record: it is set when the
/// first record enters an empty buffer and is *not* postponed by subsequent
/// pushes, so a continuously fed batch can never wait indefinitely.
pub struct InsertBatcher<T> {
    config: InsertBatcherConfig,
    buffer: Vec<T>,
    deadline: Option<Instant>,
}

impl<T> InsertBatcher<T> {
    /// Creates a batcher with the given limits.
    ///
    /// # Panics
    /// Panics when `config.max_batch_records == 0`.
    pub fn new(config: InsertBatcherConfig) -> Self {
        assert!(
            config.max_batch_records >= 1,
            "InsertBatcher requires max_batch_records >= 1"
        );
        Self {
            config,
            buffer: Vec::new(),
            deadline: None,
        }
    }

    /// Configuration this batcher enforces.
    pub const fn config(&self) -> &InsertBatcherConfig {
        &self.config
    }

    /// Number of records currently held back waiting for a full batch.
    pub fn buffered(&self) -> usize {
        self.buffer.len()
    }

    /// `true` when nothing is buffered.
    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    /// Instant when the current partial batch must be flushed, if any.
    pub fn deadline(&self) -> Option<Instant> {
        self.deadline
    }

    /// Pushes an incoming batch of records into the accumulator.
    ///
    /// Every complete group of `max_records` records is emitted immediately as
    /// a [`WriteBatch`]; any remainder stays buffered and keeps its place at
    /// the head of future batches (FIFO ordering across all emissions). Empty
    /// inputs are no-ops that never produce empty write batches.
    ///
    /// # Panics
    /// Panics if the internal invariant "a ready batch is always present when
    /// exactly one full batch was produced" is violated; unreachable with
    /// `max_records >= 1`.
    pub fn push(&mut self, records: Vec<T>) -> BatchOutput<T> {
        let mut ready = Vec::new();
        let mut incoming = records;
        if incoming.is_empty() {
            return BatchOutput::Buffered;
        }

        while !incoming.is_empty() {
            let space = self.config.max_batch_records - self.buffer.len();
            if incoming.len() < space {
                self.fill(&mut incoming);
                break;
            }

            // Fill the buffer to capacity, emit it as a full batch and keep
            // looping over the remainder. `split_off` moves the tail into a
            // fresh allocation instead of copying record payloads.
            let tail = incoming.split_off(space);
            self.fill(&mut incoming);
            let full = WriteBatch::from_vec(std::mem::take(&mut self.buffer));
            self.deadline = None;
            ready.push(full);
            incoming = tail;
        }

        match ready.len() {
            0 => BatchOutput::Buffered,
            1 => BatchOutput::Single(ready.pop().expect("single ready batch")),
            _ => BatchOutput::Multiple(ready),
        }
    }

    /// Emits the current partial batch regardless of its age (flush deadline,
    /// shutdown, ...). Returns `None` when nothing is buffered.
    pub fn flush(&mut self) -> Option<WriteBatch<T>> {
        if self.buffer.is_empty() {
            return None;
        }
        let records = std::mem::take(&mut self.buffer);
        self.deadline = None;
        Some(WriteBatch::from_vec(records))
    }

    /// Emits the partial batch when its age deadline has passed relative to
    /// `now`. Returns the emitted batch, or `None` when the buffer is empty or
    /// not yet expired.
    pub fn flush_if_expired(&mut self, now: Instant) -> Option<WriteBatch<T>> {
        match self.deadline {
            Some(deadline) if now >= deadline => self.flush(),
            _ => None,
        }
    }

    /// Moves every record from `records` into the buffer, transferring
    /// ownership without cloning payloads. Arms the age clock when the buffer
    /// transitions from empty to non-empty so the deadline always belongs to
    /// the oldest buffered record.
    fn fill(&mut self, records: &mut Vec<T>) {
        let was_empty = self.buffer.is_empty();
        self.buffer.append(records);
        debug_assert!(records.is_empty());
        if was_empty && !self.buffer.is_empty() {
            self.deadline = Some(Instant::now() + self.config.max_batch_age);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(max_batch_records: usize, max_batch_age_ms: u64) -> InsertBatcherConfig {
        InsertBatcherConfig::new(max_batch_records, Duration::from_millis(max_batch_age_ms))
    }

    fn records(count: usize) -> Vec<usize> {
        (0..count).collect()
    }

    fn sizes<T>(output: BatchOutput<T>) -> Vec<usize> {
        let mut collected = Vec::new();
        output.for_each(|batch| collected.push(batch.len()));
        collected
    }

    #[test]
    fn exact_capacity_is_emitted_immediately() {
        let mut batcher = InsertBatcher::new(config(1000, 10));

        let output = batcher.push(records(1000));

        assert_eq!(sizes(output), vec![1000]);
        assert_eq!(batcher.buffered(), 0);
        assert!(batcher.is_empty());
        assert_eq!(batcher.flush(), None);
    }

    #[test]
    fn below_capacity_stays_buffered() {
        let mut batcher = InsertBatcher::new(config(1000, 10));

        let output = batcher.push(records(700));

        assert!(output.is_empty());
        assert_eq!(output.len(), 0);
        assert_eq!(batcher.buffered(), 700);
        // Nothing may be force-emitted while under capacity...
        let forced = batcher.flush().expect("partial batch available");
        assert_eq!(forced.len(), 700);
        assert!(batcher.is_empty());
    }

    #[test]
    fn combines_smaller_batches_into_full_ones() {
        let mut batcher = InsertBatcher::new(config(1000, 10));

        let first = batcher.push(records(700));
        assert!(first.is_empty());

        let second = batcher.push(records(600));
        assert_eq!(sizes(second), vec![1000]);
        assert_eq!(batcher.buffered(), 300);

        let third = batcher.push(records(900));
        assert_eq!(sizes(third), vec![1000]);
        assert_eq!(batcher.buffered(), 200);
    }

    #[test]
    fn remainder_preserves_fifo_order_across_emissions() {
        let mut batcher = InsertBatcher::new(config(4, 10));

        let mut sequence = Vec::new();
        batcher
            .push(vec![0, 1])
            .for_each(|b| sequence.extend(b.into_records()));
        batcher
            .push(vec![2, 3, 4])
            .for_each(|b| sequence.extend(b.into_records()));
        let tail = batcher.flush().unwrap();
        sequence.extend(tail.into_records());

        assert_eq!(sequence, vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn oversized_batches_are_split() {
        let mut batcher = InsertBatcher::new(config(1000, 10));

        let output = batcher.push(records(2500));

        assert_eq!(sizes(output), vec![1000, 1000]);
        assert_eq!(batcher.buffered(), 500);

        // The leftover completes with the next input.
        let next = batcher.push(records(500));
        assert_eq!(sizes(next), vec![1000]);
        assert_eq!(batcher.buffered(), 0);
    }

    #[test]
    fn multiple_splits_with_remainder_combination() {
        let mut batcher = InsertBatcher::new(config(1000, 10));

        assert!(batcher.push(records(700)).is_empty());
        let second = batcher.push(records(600));
        assert_eq!(sizes(second), vec![1000]);
        let third = batcher.push(records(900));
        assert_eq!(sizes(third), vec![1000]);
        assert_eq!(batcher.buffered(), 200);

        let flushed = batcher.flush().unwrap();
        assert_eq!(flushed.len(), 200);
    }

    #[test]
    fn empty_input_never_creates_batches() {
        let mut batcher: InsertBatcher<usize> = InsertBatcher::new(config(8, 10));

        let output = batcher.push(Vec::new());

        assert!(output.is_empty());
        assert_eq!(batcher.buffered(), 0);
        assert_eq!(batcher.flush(), None);
    }

    #[test]
    fn empty_input_does_not_arm_deadline() {
        let mut batcher: InsertBatcher<usize> = InsertBatcher::new(config(8, 10));
        batcher.push(Vec::new());
        assert_eq!(batcher.deadline(), None);

        batcher.push(records(2));
        assert!(batcher.deadline().is_some());
    }

    #[test]
    fn deadline_is_anchored_to_first_record_not_postponed_by_later_pushes() {
        let max_batch_age = Duration::from_millis(50);
        let mut batcher = InsertBatcher::new(InsertBatcherConfig::new(100, max_batch_age));

        let started = Instant::now();
        batcher.push(records(1));
        let first_deadline = batcher.deadline().expect("armed after first record");
        // Deadline ≈ push time + max_batch_age; allow scheduling slack.
        let elapsed_to_deadline = first_deadline.saturating_duration_since(started);
        assert!(
            elapsed_to_deadline >= max_batch_age
                && elapsed_to_deadline <= max_batch_age + Duration::from_millis(20),
            "deadline not anchored to oldest record: {elapsed_to_deadline:?}"
        );

        std::thread::sleep(Duration::from_millis(20));
        // Unrelated incoming traffic must not endlessly postpone the deadline.
        batcher.push(records(2));
        let second_deadline = batcher.deadline().expect("still armed");
        assert_eq!(
            second_deadline, first_deadline,
            "age belongs to the oldest buffered record"
        );

        // Once flushed the clock resets for the next partial batch.
        std::thread::sleep(Duration::from_millis(40));
        let now = Instant::now();
        let expired = batcher.flush_if_expired(now).expect("expired");
        assert_eq!(expired.len(), 3);
        assert_eq!(batcher.deadline(), None);

        batcher.push(records(1));
        assert!(batcher.deadline().unwrap() > now);
    }

    #[test]
    fn flush_if_expired_respects_future_deadline() {
        let mut batcher = InsertBatcher::new(config(10, 60_000));
        batcher.push(records(3));

        let output = batcher.flush_if_expired(Instant::now());
        assert!(output.is_none());
        assert_eq!(batcher.buffered(), 3);
    }

    #[test]
    fn flush_emits_partial_remainder_after_split() {
        let mut batcher = InsertBatcher::new(config(4, 30));

        let output = batcher.push(records(6));
        assert_eq!(sizes(output), vec![4]);
        assert_eq!(batcher.buffered(), 2);

        // The remainder ages from its own arrival; once expired it is
        // flushed as a partial batch instead of waiting for capacity.
        std::thread::sleep(Duration::from_millis(40));
        let partial = batcher.flush_if_expired(Instant::now()).expect("expired");
        assert_eq!(partial.len(), 2);
        assert!(batcher.is_empty());
    }

    #[test]
    fn exact_fill_leaves_no_stale_deadline() {
        let mut batcher = InsertBatcher::new(config(2, 10));
        batcher.push(records(2));
        assert!(batcher.is_empty());
        // Deadline is irrelevant without buffered records, and flush is a
        // no-op rather than an empty batch.
        assert_eq!(batcher.flush(), None);
    }

    #[test]
    fn single_output_variant_carries_one_batch() {
        let mut batcher = InsertBatcher::new(config(3, 10));
        let output = batcher.push(records(3));
        assert!(matches!(output, BatchOutput::Single(_)));
        assert_eq!(output.len(), 1);

        let mut batcher = InsertBatcher::new(config(3, 10));
        let output = batcher.push(records(7));
        assert!(matches!(output, BatchOutput::Multiple(_)));
        assert_eq!(output.len(), 2);
        assert_eq!(batcher.buffered(), 1);
    }

    #[test]
    fn default_config_matches_documented_constants() {
        let defaults = InsertBatcherConfig::default();
        assert_eq!(defaults.max_batch_records, DEFAULT_MAX_INSERT_BATCH_RECORDS);
        assert_eq!(defaults.max_batch_age, DEFAULT_MAX_INSERT_BATCH_AGE);
    }

    #[test]
    #[should_panic(expected = "max_batch_records")]
    fn zero_capacity_is_rejected() {
        let _ = InsertBatcher::<u8>::new(config(0, 10));
    }
}
