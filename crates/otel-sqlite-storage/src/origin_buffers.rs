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
//! Each buffer also tracks the durability tickets (`commit_seq`) of the
//! records still buffered, as an ordered queue of `(ticket, records)`
//! segments. An emitted batch consumes the segments covering its first
//! records; the segment that carries a ticket's last record is marked
//! complete. A split chunk therefore stays outstanding across batches, and a
//! batch that merges tickets 1 and 3 settles each on its own final segment
//! rather than stranding ticket 1 behind the watermark.
//!
//! This module is pure data-structure logic: it knows nothing about channels,
//! threads or metrics. The driver loop lives in `crate::batcher`.

use std::collections::VecDeque;
use std::time::Instant;

use otel_sqlite_core::storage::{
    BatchOrigin, InsertBatcher, InsertBatcherConfig, RecordSegment, WriteBatch,
};

/// Batches produced by one origin buffer, paired with the origin shared by
/// all records inside them and the ticket segments they carry. This is the
/// driver-side counterpart of the core [`WriteBatch`]: same records, plus the
/// metadata needed to build a `LogWriteBatch` command.
#[derive(Debug, PartialEq)]
pub(crate) struct Submission<T> {
    pub(crate) origin: BatchOrigin,
    pub(crate) records: WriteBatch<T>,
    /// Every ticket referenced by this submission, sorted ascending.
    pub(crate) commit_seqs: Vec<u64>,
    /// Ordered partition of `records`; `complete` marks the segment carrying
    /// the last record of its ticket.
    pub(crate) segments: Vec<RecordSegment>,
}

impl<T> Submission<T> {
    pub(crate) fn new(
        origin: BatchOrigin,
        records: WriteBatch<T>,
        segments: Vec<RecordSegment>,
    ) -> Self {
        let mut commit_seqs = segments
            .iter()
            .map(|segment| segment.commit_seq)
            .collect::<Vec<_>>();
        commit_seqs.sort_unstable();
        commit_seqs.dedup();
        Self {
            origin,
            records,
            commit_seqs,
            segments,
        }
    }
}

/// One origin's batcher plus the tickets of the records still buffered.
struct OriginBuffer<T> {
    /// FIFO `(ticket, records remaining in the buffer)` for every record
    /// currently buffered, in the same order as the batcher's buffer.
    segments: VecDeque<(u64, usize)>,
    batcher: InsertBatcher<T>,
}

impl<T> OriginBuffer<T> {
    fn new(config: InsertBatcherConfig) -> Self {
        Self {
            segments: VecDeque::new(),
            batcher: InsertBatcher::new(config),
        }
    }

    fn is_empty(&self) -> bool {
        self.batcher.is_empty() && self.segments.is_empty()
    }

    /// Maps the first `count` records of the batcher's FIFO buffer to their
    /// tickets, consuming the segments they cover. A segment is marked
    /// complete only when its last record is in this batch.
    fn consume(&mut self, count: usize) -> Vec<RecordSegment> {
        debug_assert!(
            self.segments
                .iter()
                .map(|(_, records)| *records)
                .sum::<usize>()
                >= count,
            "a batch cannot consume more records than the segments hold"
        );
        let mut out = Vec::new();
        let mut left = count;
        while left > 0 {
            let (commit_seq, remaining) = self
                .segments
                .front_mut()
                .expect("every buffered record has a ticket segment");
            let take = (*remaining).min(left);
            *remaining -= take;
            left -= take;
            let complete = *remaining == 0;
            out.push(RecordSegment {
                commit_seq: *commit_seq,
                records: take,
                complete,
            });
            if complete {
                self.segments.pop_front();
            }
        }
        out
    }
}

pub(crate) struct OriginBuffers<T> {
    config: InsertBatcherConfig,
    /// One buffer per active origin.
    buffers: Vec<(BatchOrigin, OriginBuffer<T>)>,
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
            .filter_map(|(_, buffer)| buffer.batcher.deadline())
            .min()
    }

    pub(crate) fn buffered(&self) -> usize {
        self.buffers
            .iter()
            .map(|(_, buffer)| buffer.batcher.buffered())
            .sum()
    }

    /// Pushes a mapped chunk, emitting any full batches that became ready.
    ///
    /// The chunk's ticket joins the buffer before any emission, so a full
    /// batch emitted by this push carries segments for the incoming chunk.
    /// If the chunk is split, only the batch containing its last record marks
    /// the ticket complete. Returns the ready submissions in emission order;
    /// an empty vector means everything stayed buffered.
    pub(crate) fn push(
        &mut self,
        origin: BatchOrigin,
        records: Vec<T>,
        commit_seq: u64,
    ) -> Vec<Submission<T>> {
        let index = if let Some(index) = self.buffers.iter().position(|(known, _)| *known == origin)
        {
            index
        } else {
            self.buffers
                .push((origin.clone(), OriginBuffer::new(self.config)));
            self.buffers.len() - 1
        };

        let buffer = &mut self.buffers[index].1;
        if !records.is_empty() {
            buffer.segments.push_back((commit_seq, records.len()));
        }
        let output = buffer.batcher.push(records);
        let mut emitted = Vec::with_capacity(output.len());
        for batch in output {
            let segments = buffer.consume(batch.len());
            emitted.push(Submission::new(origin.clone(), batch, segments));
        }
        if buffer.is_empty() {
            self.buffers.remove(index);
        }
        emitted
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
        for (origin, buffer) in &mut self.buffers {
            if let Some(records) = flush_one(&mut buffer.batcher) {
                let segments = buffer.consume(records.len());
                submissions.push(Submission::new(origin.clone(), records, segments));
            }
        }
        self.retain_active();
        submissions
    }

    fn retain_active(&mut self) {
        self.buffers.retain(|(_, buffer)| !buffer.is_empty());
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
        assert!(pushed.is_empty());

        let pushed = buffers.push(
            origin("a"),
            (0..2).map(|_| LogRecord::default()).collect(),
            3,
        );
        assert!(pushed.is_empty());
        assert_eq!(buffers.buffered(), 2);
        assert!(buffers.deadline().is_some());

        // Interleaved origins accumulate independently instead of forcing a
        // partial flush on every origin change.
        let pushed = buffers.push(
            origin("b"),
            (0..3).map(|_| LogRecord::default()).collect(),
            5,
        );
        assert!(pushed.is_empty());

        let submissions = buffers.push(
            origin("a"),
            (0..9).map(|_| LogRecord::default()).collect(),
            7,
        );
        assert_eq!(
            submissions
                .iter()
                .map(|s| s.records.len())
                .collect::<Vec<_>>(),
            vec![4, 4]
        );
        assert!(submissions.iter().all(|s| s.origin.schema_url == "a"));
        // Ticket 3 ends inside the first full batch; ticket 7 is split, so
        // only its final flush may complete it.
        assert_eq!(submissions[0].commit_seqs, vec![3, 7]);
        assert_eq!(
            submissions[0].segments,
            vec![
                RecordSegment {
                    commit_seq: 3,
                    records: 2,
                    complete: true
                },
                RecordSegment {
                    commit_seq: 7,
                    records: 2,
                    complete: false
                },
            ]
        );
        assert_eq!(submissions[1].commit_seqs, vec![7]);
        assert_eq!(
            submissions[1].segments,
            vec![RecordSegment {
                commit_seq: 7,
                records: 4,
                complete: false
            }]
        );
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
            vec![("a", 3, [7].as_slice()), ("b", 3, [5].as_slice())]
        );
        assert_eq!(
            submissions[0].segments,
            vec![RecordSegment {
                commit_seq: 7,
                records: 3,
                complete: true
            }]
        );
        assert_eq!(
            submissions[1].segments,
            vec![RecordSegment {
                commit_seq: 5,
                records: 3,
                complete: true
            }]
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
        assert!(pushed.is_empty());
        assert_eq!(submissions.len(), 1);
        assert_eq!(submissions[0].commit_seqs, vec![1, 3]);
        assert_eq!(
            submissions[0].segments,
            vec![
                RecordSegment {
                    commit_seq: 1,
                    records: 2,
                    complete: true
                },
                RecordSegment {
                    commit_seq: 3,
                    records: 2,
                    complete: true
                },
            ]
        );
    }
}
