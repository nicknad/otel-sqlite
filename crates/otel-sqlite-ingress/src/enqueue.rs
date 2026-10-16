//! Hand-off of mapped ingest chunks to the storage pipeline.
//!
//! [`enqueue`] is the only way OTLP handlers touch the queue. Requests are
//! admitted **all-or-nothing**: [`IngestSender::reserve`] either reserves room
//! for every chunk of the request up front or rejects the whole request
//! without touching the queue or the commit ledger. A request that maps to
//! more chunks than the queue can ever hold (even when empty) is a permanent
//! mapping error instead: retrying can never succeed, so handlers answer
//! `INVALID_ARGUMENT` and tell the client to split the request.
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

use crate::error::IngressError;
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
///
/// Returns [`IngressError::Mapping`] when the request maps to more chunks than
/// the queue's total capacity: no retry could ever fit, so the caller must
/// answer with a permanent error instead of `UNAVAILABLE`.
pub(crate) fn enqueue(
    queue: &IngestSender,
    ledger: &CommitLedger,
    work: Vec<IngestMessage>,
) -> Result<EnqueueOutcome, IngressError> {
    let mut outcome = EnqueueOutcome::default();
    let total_records: u64 = work
        .iter()
        .map(|message| message.record_count() as u64)
        .sum();

    // A request with more chunks than the queue can hold is permanently
    // unadmittable (an empty queue still has `capacity` slots), so a retry
    // loop would spin forever. Surface it as a permanent mapping error and
    // tell the client to split the request.
    if work.len() > queue.capacity() {
        return Err(IngressError::Mapping(format!(
            "request maps to {} chunks, which exceeds the ingest queue capacity of {}; split the request",
            work.len(),
            queue.capacity()
        )));
    }

    // Reserve room for the whole request up front. Failing here rejects the
    // request wholesale: no tickets were issued, nothing was enqueued, and the
    // watermark is untouched, so the handler can return UNAVAILABLE without
    // any durable wait.
    let Ok(guard) = queue.reserve(work.len()) else {
        outcome.rejected = total_records;
        ::metrics::counter!("ingress_queue_full_total", "signal" => "logs").increment(1);
        return Ok(outcome);
    };

    // Record metrics only after the gate is dropped: the admission hold must
    // cover just the check-then-send sequence, never the metric recorder
    // calls, which would otherwise extend the hold for every concurrent
    // export. The queue-depth gauge keeps its per-chunk peak semantics by
    // sampling `queue.len()` in the loop and publishing once below.
    let mut samples: Vec<(u64, f64)> = Vec::with_capacity(work.len());
    let mut peak_depth = 0usize;
    for mut message in work {
        let records = message.record_count() as u64;
        let started = Instant::now();
        let ticket = ledger.issue();
        message.set_commit_seq(ticket);
        // The admission gate is held, so no other producer can consume a
        // reserved slot; the consumer can only drain. This send therefore
        // succeeds unless the storage side disconnected.
        let mut stop = false;
        match guard.try_send(message) {
            Ok(()) => {
                outcome.accepted += records;
                outcome.last_ticket = Some(ticket);
            }
            Err(SendFailure::Full) => {
                // Unreachable while holding the gate: `reserve` verified room
                // for every chunk and no producer can interleave. If it ever
                // fires the admission invariant is already broken and the
                // chunks already sent cannot be recalled, so fail loudly in
                // debug and stop accepting more; the request still leaves as
                // rejected (`rejected > 0`) rather than half-admitted.
                debug_assert!(
                    false,
                    "admission reserved a slot for every chunk, yet try_send reported Full"
                );
                ledger.void(ticket);
                outcome.rejected = total_records.saturating_sub(outcome.accepted);
                stop = true;
            }
            Err(SendFailure::Disconnected) => {
                // The storage side is gone mid-request: this ticket was issued
                // but never sent, so void it, and count every record of this
                // and every later chunk as rejected so `accepted + rejected`
                // always equals the request total.
                ledger.void(ticket);
                outcome.rejected = total_records.saturating_sub(outcome.accepted);
                outcome.disconnected = true;
                stop = true;
            }
        }
        samples.push((records, started.elapsed().as_secs_f64()));
        peak_depth = peak_depth.max(queue.len());
        if stop {
            break;
        }
    }

    drop(guard);

    for (records, elapsed) in samples {
        ::metrics::histogram!("ingress_batch_size", "signal" => "logs").record(records as f64);
        ::metrics::histogram!("ingress_enqueue_duration", "signal" => "logs").record(elapsed);
    }
    ::metrics::gauge!("ingress_queue_depth", "signal" => "logs").set(peak_depth as f64);

    Ok(outcome)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::channel;
    use otel_sqlite_core::model::LogRecord;
    use otel_sqlite_core::storage::{BatchOrigin, IngestMessage, LogChunk};
    use std::sync::Arc;

    fn log_chunk(records: usize) -> IngestMessage {
        IngestMessage::Logs(LogChunk {
            origin: BatchOrigin::default(),
            records: (0..records).map(|_| LogRecord::default()).collect(),
            commit_seq: 0,
        })
    }

    #[test]
    fn whole_request_is_rejected_when_it_cannot_all_fit() {
        let ledger = Arc::new(CommitLedger::new());
        let (queue, receiver) = channel(2);

        // A two-slot queue already holding a control message; the request's
        // two chunks can never both fit right now, so the entire request is
        // refused (transiently — this is the retryable case).
        queue.send(IngestMessage::Flush).expect("control send");
        let outcome = enqueue(&queue, &ledger, vec![log_chunk(2), log_chunk(3)])
            .expect("transient queue pressure is not a permanent error");

        assert_eq!(outcome.accepted, 0);
        assert_eq!(outcome.rejected, 5);
        assert_eq!(outcome.last_ticket, None);
        assert!(!outcome.disconnected);
        assert_eq!(
            receiver.len(),
            1,
            "a rejected request must not leave chunks behind"
        );
        assert_eq!(
            ledger.watermark().committed_through,
            0,
            "no tickets were issued, so the watermark cannot move"
        );
    }

    #[test]
    fn request_larger_than_the_queue_capacity_is_a_permanent_mapping_error() {
        let ledger = Arc::new(CommitLedger::new());
        let (queue, receiver) = channel(1);

        // Two chunks can never fit in a one-slot queue, no matter how long
        // the client retries: that must be a permanent, non-retryable error.
        let error = enqueue(&queue, &ledger, vec![log_chunk(2), log_chunk(3)])
            .expect_err("a request exceeding the total capacity can never fit");
        let message = error.to_string();
        assert!(message.contains("2 chunks"), "{message}");
        assert!(message.contains("capacity of 1"), "{message}");
        assert!(message.contains("split the request"), "{message}");

        assert_eq!(ledger.watermark().committed_through, 0);
        assert!(receiver.is_empty(), "nothing may be enqueued");
    }

    #[test]
    fn whole_request_is_admitted_contiguously_when_it_fits() {
        let ledger = Arc::new(CommitLedger::new());
        let (queue, receiver) = channel(4);

        let outcome =
            enqueue(&queue, &ledger, vec![log_chunk(2), log_chunk(3)]).expect("request fits");

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
        let outcome = enqueue(&queue, &ledger, vec![log_chunk(1), log_chunk(1)])
            .expect("disconnect is not a mapping error");

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
