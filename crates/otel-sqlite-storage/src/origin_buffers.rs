//! Per-origin accumulation of mapped records into storage-sized batches.
//!
//! [`OriginBuffers`] owns one independent [`InsertBatcher`] per distinct
//! [`BatchOrigin`]. Records from different origins are never merged into the
//! same write batch (batch-level resource attribution would be lost), but
//! origins accumulate concurrently: interleaved traffic from several services
//! still combines into full storage-sized batches instead of flushing on every
//! origin change. Buffer entries are removed as soon as they are drained, so
//! the list only ever holds the handful of origins active inside one batching
//! window.
//!
//! Each buffer also accumulates the durability tickets (`commit_seq`) of the
//! chunks folded into it. Emitted batches claim *all* buffered tickets — not
//! just their maximum — because a batch that merges tickets 1 and 3 commits
//! both, and leaving ticket 1 unsettled would stall the contiguous commit
//! watermark forever. Claiming is idempotent: a buffer keeps its ticket set
//! until it is drained, so later partial flushes re-claim already-settled
//! tickets harmlessly.
//!
//! This module is pure data-structure logic: it knows nothing about channels,
//! threads or metrics. The driver loop lives in `crate::batcher`.

use std::collections::BTreeSet;
use std::time::Instant;

use otel_sqlite_core::storage::{BatchOrigin, InsertBatcher, InsertBatcherConfig, WriteBatch};

/// Batches produced by one origin buffer, paired with the origin shared by
/// all records inside them and every durability ticket they carry. This is
/// the driver-side counterpart of the core [`WriteBatch`]: same records,
/// plus the metadata needed to build a `LogWriteBatch`/`MetricWriteBatch`
/// command.
#[derive(Debug, PartialEq)]
pub(crate) struct Submission<T> {
    pub(crate) origin: BatchOrigin,
    pub(crate) records: WriteBatch<T>,
    /// Sorted ascending; may repeat across successive submissions of the
    /// same buffer (settlement downstream is idempotent).
    pub(crate) commit_seqs: Vec<u64>,
}

impl<T> Submission<T> {
    pub(crate) fn new(origin: BatchOrigin, records: WriteBatch<T>, commit_seqs: Vec<u64>) -> Self {
        Self {
            origin,
            records,
            commit_seqs,
        }
    }
}

/// Result of pushing a mapped chunk into an [`OriginBuffers`].
///
/// Mirrors the generic `InsertBatcher::push` result (`BatchOutput`) one layer
/// up: same Buffered/Single/Multiple shape, but every ready batch is paired
/// with its origin so the driver can submit complete commands directly.
#[derive(Debug, PartialEq)]
pub(crate) enum Emitted<T> {
    /// Nothing ready; the records joined (or formed) the partial batch.
    Buffered,
    /// Exactly one batch is ready for submission.
    Single(Submission<T>),
    /// An oversized chunk produced several full batches, in order.
    Multiple(Vec<Submission<T>>),
}

impl<T> Emitted<T> {
    pub(crate) fn from_batches(mut batches: Vec<(BatchOrigin, WriteBatch<T>, Vec<u64>)>) -> Self {
        match batches.len() {
            0 => Self::Buffered,
            1 => {
                let (origin, records, commit_seqs) = batches.swap_remove(0);
                Self::Single(Submission::new(origin, records, commit_seqs))
            }
            _ => Self::Multiple(
                batches
                    .into_iter()
                    .map(|(origin, records, commit_seqs)| {
                        Submission::new(origin, records, commit_seqs)
                    })
                    .collect(),
            ),
        }
    }

    /// Calls `f` with every ready submission in order.
    pub(crate) fn for_each(self, mut f: impl FnMut(Submission<T>)) {
        match self {
            Self::Buffered => {}
            Self::Single(submission) => f(submission),
            Self::Multiple(submissions) => {
                for submission in submissions {
                    f(submission);
                }
            }
        }
    }
}

pub(crate) struct OriginBuffers<T> {
    config: InsertBatcherConfig,
    /// One buffer per active origin. The set holds every durability ticket
    /// absorbed since the buffer was created; emitted batches claim a sorted
    /// snapshot of it.
    buffers: Vec<(BatchOrigin, BTreeSet<u64>, InsertBatcher<T>)>,
}

impl<T> OriginBuffers<T> {
    pub(crate) fn new(config: InsertBatcherConfig) -> Self {
        Self {
            config,
            buffers: Vec::new(),
        }
    }

    pub(crate) fn deadline(&self) -> Option<Instant> {
        self.buffers
            .iter()
            .filter_map(|(_, _, batcher)| batcher.deadline())
            .min()
    }

    pub(crate) fn buffered(&self) -> usize {
        self.buffers
            .iter()
            .map(|(_, _, batcher)| batcher.buffered())
            .sum()
    }

    /// Pushes a mapped chunk, emitting any full batches that became ready.
    ///
    /// The chunk's ticket joins the buffer's accumulated set before any
    /// emission snapshot is taken, so a full batch emitted by this push
    /// already claims the incoming ticket.
    pub(crate) fn push(
        &mut self,
        origin: BatchOrigin,
        records: Vec<T>,
        commit_seq: u64,
    ) -> Emitted<T> {
        let index = if let Some(index) = self
            .buffers
            .iter()
            .position(|(known, _, _)| *known == origin)
        {
            self.buffers[index].1.insert(commit_seq);
            index
        } else {
            let mut tickets = BTreeSet::new();
            tickets.insert(commit_seq);
            self.buffers
                .push((origin.clone(), tickets, InsertBatcher::new(self.config)));
            self.buffers.len() - 1
        };

        let output = self.buffers[index].2.push(records);
        if output.is_empty() {
            if self.buffers[index].2.is_empty() {
                self.buffers.remove(index);
            }
            return Emitted::Buffered;
        }
        let claimed = self.buffers[index].1.iter().copied().collect::<Vec<_>>();
        let mut emitted = Vec::new();
        output.for_each(|batch| {
            emitted.push((origin.clone(), batch, claimed.clone()));
        });
        if self.buffers[index].2.is_empty() {
            self.buffers.remove(index);
        }

        Emitted::from_batches(emitted)
    }

    /// Emits every expired partial batch (oldest deadline first).
    pub(crate) fn flush_expired(&mut self, now: Instant) -> Vec<Submission<T>> {
        self.flush_with(|batcher| batcher.flush_if_expired(now))
    }

    /// Emits all partial batches unconditionally (barriers and shutdown), in
    /// first-seen origin order.
    pub(crate) fn flush_all(&mut self) -> Vec<Submission<T>> {
        self.flush_with(InsertBatcher::flush)
    }

    fn flush_with(
        &mut self,
        mut flush_one: impl FnMut(&mut InsertBatcher<T>) -> Option<WriteBatch<T>>,
    ) -> Vec<Submission<T>> {
        let mut submissions = Vec::new();
        for (origin, tickets, batcher) in &mut self.buffers {
            if let Some(records) = flush_one(batcher) {
                let claimed = tickets.iter().copied().collect::<Vec<_>>();
                submissions.push(Submission::new(origin.clone(), records, claimed));
            }
        }
        self.retain_active();
        submissions
    }

    fn retain_active(&mut self) {
        self.buffers.retain(|(_, _, batcher)| !batcher.is_empty());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use otel_sqlite_core::model::LogRecord;
    use std::time::Duration;

    fn config(max_records: usize, max_age_ms: u64) -> InsertBatcherConfig {
        InsertBatcherConfig::new(max_records, Duration::from_millis(max_age_ms))
    }

    #[test]
    fn signal_buffers_accumulate_per_origin_and_flush_in_order() {
        fn origin(schema: &str) -> BatchOrigin {
            BatchOrigin {
                resource: None,
                schema_url: schema.to_owned(),
            }
        }

        let mut buffers = OriginBuffers::<LogRecord>::new(config(4, 60_000));

        let pushed = buffers.push(origin("a"), vec![], 0);
        assert!(matches!(pushed, Emitted::Buffered));

        let pushed = buffers.push(
            origin("a"),
            (0..2).map(|_| LogRecord::default()).collect(),
            3,
        );
        assert!(matches!(pushed, Emitted::Buffered));
        assert_eq!(buffers.buffered(), 2);
        assert!(buffers.deadline().is_some());

        // Interleaved origins accumulate independently instead of forcing a
        // partial flush on every origin change.
        let pushed = buffers.push(
            origin("b"),
            (0..3).map(|_| LogRecord::default()).collect(),
            5,
        );
        assert!(matches!(pushed, Emitted::Buffered));

        let pushed = buffers.push(
            origin("a"),
            (0..9).map(|_| LogRecord::default()).collect(),
            7,
        );
        match pushed {
            Emitted::Multiple(submissions) => {
                assert_eq!(
                    submissions
                        .iter()
                        .map(|s| s.records.len())
                        .collect::<Vec<_>>(),
                    vec![4, 4]
                );
                assert!(submissions.iter().all(|s| s.origin.schema_url == "a"));
                // Both emissions claim the origin's whole accumulated ticket
                // set {3, 7}: settlement downstream must cover every folded
                // ticket, not just the maximum.
                assert!(submissions.iter().all(|s| s.commit_seqs == vec![3, 7]));
            }
            other => panic!("expected multiple emissions, got {other:?}"),
        }
        // 3 records of "a" plus the untouched 3 of "b" remain.
        assert_eq!(buffers.buffered(), 6);

        let submissions = buffers.flush_all();
        assert_eq!(
            submissions
                .iter()
                .map(|s| (
                    s.origin.schema_url.as_str(),
                    s.records.len(),
                    s.commit_seqs.as_slice()
                ))
                .collect::<Vec<_>>(),
            vec![("a", 3, [3, 7].as_slice()), ("b", 3, [5].as_slice())]
        );
        assert_eq!(buffers.buffered(), 0);
        assert!(buffers.flush_all().is_empty());
    }

    #[test]
    fn merged_tickets_are_all_claimed_not_just_the_maximum() {
        // Regression guard for the watermark-stall bug: chunks with tickets
        // 1 and 3 merge into one batch, which must claim BOTH — settling only
        // 3 would strand ticket 1 on the commit watermark forever.
        fn origin(schema: &str) -> BatchOrigin {
            BatchOrigin {
                resource: None,
                schema_url: schema.to_owned(),
            }
        }

        let mut buffers = OriginBuffers::<LogRecord>::new(config(100, 60_000));
        buffers.push(
            origin("a"),
            (0..2).map(|_| LogRecord::default()).collect(),
            1,
        );
        let pushed = buffers.push(
            origin("a"),
            (0..2).map(|_| LogRecord::default()).collect(),
            3,
        );

        let submissions = buffers.flush_all();
        assert!(matches!(pushed, Emitted::Buffered));
        assert_eq!(submissions.len(), 1);
        assert_eq!(submissions[0].commit_seqs, vec![1, 3]);
    }
}
