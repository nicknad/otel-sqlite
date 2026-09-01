//! Hand-off of mapped ingest chunks to the storage pipeline.
//!
//! [`enqueue`] is the only way OTLP handlers touch the queue. Requests are
//! admitted **all-or-nothing**: [`IngestSender::reserve`] either reserves room
//! for every chunk of the request up front or rejects the whole request
//! without touching the queue or the commit ledger.
//!
//! All-or-nothing admission is what makes retries safe. A handler that answers
//! `UNAVAILABLE` has had *none* of the request's records accepted, so a
//! retrying exporter resends the full request with nothing to duplicate —
//! there is no idempotency key on event or data-point rows, so partial
//! acceptance would otherwise convert a retry into duplicate telemetry.
//!
//! Accepted chunks are stamped with a durability ticket from `ledger` before
//! the send, and the highest ticket is returned as
//! [`EnqueueOutcome::last_ticket`] so durable acks can wait for the commit.
//! Because acceptance is all-or-nothing, a rejected request issues no tickets
//! at all and never moves the watermark; a request disconnected mid-batch
//! voids every ticket it issued before giving up.

use std::time::Instant;

use otel_sqlite_core::storage::{CommitLedger, IngestMessage};

use crate::{IngestSender, SendFailure};

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct EnqueueOutcome {
    pub accepted: u64,
    pub rejected: u64,
    pub disconnected: bool,
    /// Highest durability ticket among *accepted* chunks; `None` when nothing
    /// was accepted. Waiting for this single ticket covers every record this
    /// request had accepted.
    pub last_ticket: Option<u64>,
}

/// Hands mapped ingest chunks to the storage insert batcher through the
/// bounded input channel.
///
/// All-or-nothing: if every chunk cannot be admitted, the whole request is
/// rejected (`accepted = 0`, `rejected = total`, `last_ticket = None`) without
/// issuing any durability tickets. On success the chunks are enqueued in order
/// under one admission reservation, so no other producer can interleave.
pub(crate) fn enqueue(
    signal: &'static str,
    queue: &IngestSender,
    ledger: &CommitLedger,
    work: Vec<(u64, IngestMessage)>,
) -> EnqueueOutcome {
    let mut outcome = EnqueueOutcome::default();
    let total_records: u64 = work.iter().map(|(records, _)| *records).sum();

    // Reserve room for the whole request up front. Failing here rejects the
    // request wholesale: no tickets were issued, nothing was enqueued, and the
    // watermark is untouched, so the handler can return UNAVAILABLE without
    // any durable wait.
    let Ok(guard) = queue.reserve(work.len()) else {
        outcome.rejected = total_records;
        ::metrics::counter!("ingress_queue_full_total", "signal" => signal).increment(1);
        return outcome;
    };

    for (records, mut message) in work {
        ::metrics::histogram!("ingress_batch_size", "signal" => signal).record(records as f64);

        let started = Instant::now();
        let ticket = ledger.issue();
        message.set_commit_seq(ticket);
        // The admission gate is held, so no other producer can consume a
        // reserved slot; the consumer can only drain. This send therefore
        // succeeds unless the storage side disconnected.
        match guard.try_send(message) {
            Ok(()) => {
                outcome.accepted += records;
                outcome.last_ticket = Some(ticket);
            }
            Err(SendFailure::Full) => {
                // Unreachable while holding the gate (reserve checked the whole
                // request's worth of slots); kept as a defensive void so a
                // stray failure can never stall the watermark.
                ledger.void(ticket);
                outcome.rejected += records;
            }
            Err(SendFailure::Disconnected) => {
                // The storage side is gone mid-request: this ticket was issued
                // but never sent, so void it, and count every record of this
                // and every later chunk as rejected so `accepted + rejected`
                // always equals the request total.
                ledger.void(ticket);
                outcome.rejected = total_records.saturating_sub(outcome.accepted);
                outcome.disconnected = true;
            }
        }

        ::metrics::gauge!("ingress_queue_depth", "signal" => signal).set(queue.len() as f64);
        ::metrics::histogram!("ingress_enqueue_duration", "signal" => signal)
            .record(started.elapsed().as_secs_f64());

        if outcome.disconnected {
            break;
        }
    }

    outcome
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::channel;
    use otel_sqlite_core::model::LogRecord;
    use otel_sqlite_core::storage::{BatchOrigin, IngestMessage, LogChunk};
    use std::sync::Arc;

    fn log_chunk(records: usize) -> (u64, IngestMessage) {
        (
            records as u64,
            IngestMessage::Logs(LogChunk {
                origin: BatchOrigin::default(),
                records: (0..records).map(|_| LogRecord::default()).collect(),
                commit_seq: 0,
            }),
        )
    }

    #[test]
    fn whole_request_is_rejected_when_it_cannot_all_fit() {
        let ledger = Arc::new(CommitLedger::new());
        let (queue, receiver) = channel(1);

        // The one-slot queue can hold a single chunk; two chunks can never
        // both fit, so the entire request is refused.
        let outcome = enqueue("logs", &queue, &ledger, vec![log_chunk(2), log_chunk(3)]);

        assert_eq!(outcome.accepted, 0);
        assert_eq!(outcome.rejected, 5);
        assert_eq!(outcome.last_ticket, None);
        assert!(!outcome.disconnected);
        assert!(
            receiver.is_empty(),
            "a rejected request must not leave chunks behind"
        );
        assert_eq!(
            ledger.watermark().committed_through,
            0,
            "no tickets were issued, so the watermark cannot move"
        );
    }

    #[test]
    fn whole_request_is_admitted_contiguously_when_it_fits() {
        let ledger = Arc::new(CommitLedger::new());
        let (queue, receiver) = channel(4);

        let outcome = enqueue("logs", &queue, &ledger, vec![log_chunk(2), log_chunk(3)]);

        assert_eq!(outcome.accepted, 5);
        assert_eq!(outcome.rejected, 0);
        assert_eq!(outcome.last_ticket, Some(2));

        let first = receiver.recv().expect("first chunk in flight");
        let second = receiver.recv().expect("second chunk in flight");
        assert_eq!(first.commit_seq(), Some(1));
        assert_eq!(second.commit_seq(), Some(2));
    }

    #[test]
    fn disconnected_queue_rejects_the_whole_request() {
        let ledger = Arc::new(CommitLedger::new());
        let (queue, receiver) = channel(4);

        // Storage gone: every send is rejected as disconnected, no ticket is
        // ever left outstanding, and the ledger is not closed by ingress.
        drop(receiver);
        let outcome = enqueue("logs", &queue, &ledger, vec![log_chunk(1), log_chunk(1)]);

        assert!(outcome.disconnected);
        assert_eq!(outcome.rejected, 2);
        assert_eq!(outcome.accepted, 0);
        assert_eq!(outcome.last_ticket, None);
        assert!(
            !ledger.watermark().closed,
            "a rejected enqueue must not look like pipeline shutdown"
        );
    }
}
