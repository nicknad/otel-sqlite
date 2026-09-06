// Unit tests may assert invariants with unwrap() and compare floats against
// exact literals; production code must not.
#![cfg_attr(test, allow(clippy::unwrap_used, clippy::float_cmp))]

pub mod auth;
pub mod config;
pub mod enqueue;
pub mod error;
mod grpc;
mod health;
pub mod logs;
pub mod mapping;
pub mod metrics;
mod shutdown;

use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use crossbeam_channel::Sender;
use otel_sqlite_core::storage::IngestMessage;

pub use auth::{AuthError, BearerInterceptor, TokenFileVault, bearer_interceptor};
pub use config::{
    AuthConfig, DEFAULT_LISTEN_ADDRESS, DEFAULT_MAX_ATTRIBUTE_KEY_BYTES,
    DEFAULT_MAX_ATTRIBUTE_VALUE_BYTES, DEFAULT_MAX_ATTRIBUTES_PER_RECORD, DEFAULT_MAX_BODY_BYTES,
    DEFAULT_MAX_BUCKETS_PER_POINT, DEFAULT_MAX_CONCURRENT_STREAMS, DEFAULT_MAX_EXEMPLARS_PER_POINT,
    DEFAULT_MAX_RECORDS_PER_REQUEST, DEFAULT_MAX_RECV_MSG_SIZE, DEFAULT_SHUTDOWN_TIMEOUT,
    IngressConfig, MAX_ATTRIBUTE_KEY_BYTES, MAX_ATTRIBUTE_VALUE_BYTES, MAX_ATTRIBUTES_PER_RECORD,
    MAX_BODY_BYTES, MAX_BUCKETS_PER_POINT, MAX_EXEMPLARS_PER_POINT, TlsConfig,
};
pub use error::IngressError;
pub use grpc::{serve, serve_with_shutdown};
pub use logs::LogsIngress;
pub use mapping::pb;
pub use metrics::MetricsIngress;

/// Sender half of the bounded channel feeding the storage insert batcher.
///
/// Mapped record chunks cross this boundary by ownership transfer; a full
/// channel applies backpressure to OTLP clients instead of dropping records.
///
/// Unlike the raw crossbeam channel it wraps, this sender admits messages
/// **all-or-nothing**: [`IngestSender::reserve`] either reserves room for an
/// entire request's chunks up front or rejects the request wholesale, so a
/// request can never be partially accepted. That is what keeps retries
/// duplicate-free — a client told to retry (`UNAVAILABLE`) has had none of its
/// records accepted.
///
/// crossbeam's bounded channel has no multi-slot reservation primitive, so
/// atomicity is provided by an admission gate: an `Arc<Mutex<()>>` shared by
/// every clone of this sender. [`IngestSender::reserve`] holds the gate for
/// the whole check-then-send sequence, and every other way into the channel
/// ([`IngestSender::try_send`], [`IngestSender::send`],
/// [`IngestSender::send_timeout`]) briefly acquires the same gate, so no other
/// producer — including the control messages that share this queue (Flush
/// barriers and the like) — can consume a reserved slot mid-request. The
/// storage consumer drains concurrently, but draining only frees capacity, so
/// a reservation can never be broken by it.
#[derive(Debug, Clone)]
pub struct IngestSender {
    inner: Sender<IngestMessage>,
    /// Channel capacity in messages, captured at construction so [`reserve`]
    /// (which owns the gate) does not need a separate atomic read.
    ///
    /// [`reserve`]: Self::reserve
    capacity: usize,
    /// Serializes "check the queue fits, then send" across every producer so
    /// chunk batches and control messages cannot interleave. See the
    /// type-level docs for the full argument.
    admission: Arc<Mutex<()>>,
}

/// A successfully reserved slice of the ingest channel's capacity.
///
/// Returned by [`IngestSender::reserve`]. While the guard lives it holds the
/// admission gate, so no other producer can steal one of the reserved slots;
/// the storage consumer may still drain, which only makes more room.
/// [`AdmissionGuard::try_send`] is therefore guaranteed to succeed for every
/// reserved message unless the channel is disconnected.
#[derive(Debug)]
pub struct AdmissionGuard<'a> {
    sender: &'a IngestSender,
    _gate: MutexGuard<'a, ()>,
}

/// The request's chunks do not all fit in the bounded ingest queue.
///
/// Returned by [`IngestSender::reserve`]. It is the only queue-full outcome a
/// caller can observe: nothing has been issued or sent, so a rejected request
/// leaves the queue and the commit watermark untouched.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueueFull;

/// Why a single-message enqueue attempt did not deliver its message.
///
/// Returned by [`IngestSender::try_send`], [`IngestSender::send`] and
/// [`AdmissionGuard::try_send`]. Deliberately small: callers only need to
/// distinguish backpressure from a dead pipeline, never the message itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendFailure {
    /// The queue has no free slot right now.
    Full,
    /// The storage side is gone; the message was not delivered.
    Disconnected,
}

/// Why a deadline-bounded control send gave up.
///
/// Returned by [`IngestSender::send_timeout`]. Like [`SendFailure`], the
/// failure carries no payload: retry decisions only need to distinguish a
/// saturated-but-alive pipeline from a dead one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendTimeoutFailure {
    /// The deadline passed with the queue still full.
    Timeout,
    /// The storage side is gone; the message was not delivered.
    Disconnected,
}

/// Pause between retry attempts when a control message finds the queue full.
///
/// Control-path only (Flush barriers, benchmarks, shutdown): it bounds the
/// retry loop's CPU cost without adding meaningful latency, because the
/// batcher drains the queue continuously and the gate is released between
/// attempts.
const CONTROL_RETRY_PAUSE: Duration = Duration::from_micros(100);

impl IngestSender {
    /// Number of messages currently in the channel (including any still
    /// reserved by an outstanding [`AdmissionGuard`]).
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Whether the channel currently holds no messages.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Maximum number of messages the channel can hold.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Reserves room for `n` messages, or rejects the whole reservation.
    ///
    /// On success the returned guard holds the admission gate until dropped,
    /// so the caller can issue commit tickets and enqueue every chunk with no
    /// other producer able to interleave. On [`QueueFull`] nothing was
    /// reserved: no tickets should be issued, nothing should be sent, and the
    /// request is rejected wholesale.
    ///
    /// This is the only way OTLP handlers enqueue mapped chunks (see
    /// `crate::enqueue`).
    pub fn reserve(&self, n: usize) -> Result<AdmissionGuard<'_>, QueueFull> {
        let gate = self
            .admission
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if self.inner.len().saturating_add(n) <= self.capacity {
            Ok(AdmissionGuard {
                sender: self,
                _gate: gate,
            })
        } else {
            Err(QueueFull)
        }
    }

    /// Single-message send that briefly serializes on the admission gate.
    ///
    /// Non-blocking except for a momentary wait if another producer is inside
    /// a reservation critical section. Returns [`SendFailure::Full`] when the
    /// queue is full and [`SendFailure::Disconnected`] when the storage side
    /// is gone.
    pub fn try_send(&self, message: IngestMessage) -> Result<(), SendFailure> {
        let _gate = self
            .admission
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        match self.inner.try_send(message) {
            Ok(()) => Ok(()),
            Err(crossbeam_channel::TrySendError::Full(_)) => Err(SendFailure::Full),
            Err(crossbeam_channel::TrySendError::Disconnected(_)) => Err(SendFailure::Disconnected),
        }
    }

    /// Blocking control-message send, serialized with request reservations.
    ///
    /// Waits outside the admission gate when the queue is full (a consumer is
    /// always draining), so it never blocks a concurrent
    /// [`IngestSender::reserve`]. Only fails with
    /// [`SendFailure::Disconnected`] when the storage side is gone.
    pub fn send(&self, message: IngestMessage) -> Result<(), SendFailure> {
        let mut message = message;
        loop {
            let _gate = self
                .admission
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            match self.inner.try_send(message) {
                Ok(()) => return Ok(()),
                Err(crossbeam_channel::TrySendError::Full(back)) => message = back,
                Err(crossbeam_channel::TrySendError::Disconnected(_)) => {
                    return Err(SendFailure::Disconnected);
                }
            }
            std::thread::sleep(CONTROL_RETRY_PAUSE);
        }
    }

    /// Blocking control-message send with a deadline, serialized with request
    /// reservations.
    ///
    /// Waits outside the admission gate when the queue is full, so it never
    /// blocks a concurrent [`IngestSender::reserve`]. Returns
    /// [`SendTimeoutFailure::Timeout`] once the deadline passes with the queue
    /// still full, or [`SendTimeoutFailure::Disconnected`] when the storage
    /// side is gone.
    pub fn send_timeout(
        &self,
        message: IngestMessage,
        timeout: Duration,
    ) -> Result<(), SendTimeoutFailure> {
        let deadline = Instant::now() + timeout;
        let mut message = message;
        loop {
            let _gate = self
                .admission
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            match self.inner.try_send(message) {
                Ok(()) => return Ok(()),
                Err(crossbeam_channel::TrySendError::Full(back)) => message = back,
                Err(crossbeam_channel::TrySendError::Disconnected(_)) => {
                    return Err(SendTimeoutFailure::Disconnected);
                }
            }
            if Instant::now() >= deadline {
                return Err(SendTimeoutFailure::Timeout);
            }
            std::thread::sleep(CONTROL_RETRY_PAUSE);
        }
    }
}

impl AdmissionGuard<'_> {
    /// Sends one message into the reserved capacity.
    ///
    /// The gate is already held, so this is a direct `try_send` with no further
    /// locking. It can only fail with [`SendFailure::Disconnected`] — the
    /// storage side is gone — because the reservation checked the whole
    /// request's worth of slots and the consumer can only drain.
    pub fn try_send(&self, message: IngestMessage) -> Result<(), SendFailure> {
        match self.sender.inner.try_send(message) {
            Ok(()) => Ok(()),
            Err(crossbeam_channel::TrySendError::Full(_)) => Err(SendFailure::Full),
            Err(crossbeam_channel::TrySendError::Disconnected(_)) => Err(SendFailure::Disconnected),
        }
    }
}

/// Receiver half of the bounded ingest channel; owned by the storage layer.
pub type IngestReceiver = crossbeam_channel::Receiver<IngestMessage>;

/// Answers "should load balancers route traffic to me right now?".
///
/// Implemented by the binary over the storage health probe so the gRPC health
/// service reflects real pipeline state: `false` while the writer or batcher
/// is down, `true` otherwise. Kept as a trait because the ingress crate never
/// depends on the storage crate.
pub trait ServingCheck: Send + Sync + 'static {
    fn is_serving(&self) -> bool;
}

/// Creates the bounded channel between OTLP handlers and the storage insert
/// batcher. Capacity bounds how many mapped chunks may be in flight before
/// backpressure reaches clients.
///
/// The returned [`IngestSender`] enforces all-or-nothing admission for
/// requests (see the type-level docs); [`IngestReceiver`] is the plain
/// crossbeam receiver the storage batcher already consumes.
pub fn channel(capacity: usize) -> (IngestSender, IngestReceiver) {
    let (inner, receiver) = crossbeam_channel::bounded(capacity);
    (
        IngestSender {
            inner,
            capacity,
            admission: Arc::new(Mutex::new(())),
        },
        receiver,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use otel_sqlite_core::model::LogRecord;
    use otel_sqlite_core::storage::{BatchOrigin, LogChunk};

    fn chunk_message(records: usize) -> IngestMessage {
        IngestMessage::Logs(LogChunk {
            origin: BatchOrigin::default(),
            records: (0..records).map(|_| LogRecord::default()).collect(),
            commit_seq: 0,
        })
    }

    #[test]
    fn reserve_rejects_a_whole_request_that_cannot_fit() {
        let (sender, receiver) = channel(1);

        assert!(matches!(sender.reserve(2), Err(QueueFull)));
        assert!(sender.reserve(1).is_ok());

        // A rejected reservation issued nothing and sent nothing.
        assert_eq!(sender.len(), 0);
        assert!(receiver.is_empty());
    }

    #[test]
    fn reserve_holds_the_gate_across_all_reserved_sends() {
        let (sender, receiver) = channel(2);
        let guard = sender.reserve(2).expect("both chunks fit");

        // A control message cannot consume one of the reserved slots: it must
        // wait on the admission gate instead of sneaking into the queue.
        let flusher = std::thread::spawn({
            let sender = sender.clone();
            move || sender.send(IngestMessage::Flush)
        });
        std::thread::sleep(Duration::from_millis(20));
        assert!(
            !flusher.is_finished(),
            "control send must wait on the gate while slots are reserved"
        );

        guard.try_send(chunk_message(1)).unwrap();
        guard.try_send(chunk_message(1)).unwrap();
        drop(guard);

        // Both reserved chunks are in the queue; draining one lets the Flush
        // through, but it must never have displaced a chunk.
        let first = receiver.recv().unwrap();
        assert!(matches!(first, IngestMessage::Logs(_)));
        flusher.join().unwrap().unwrap();
        let second = receiver.recv().unwrap();
        let third = receiver.recv().unwrap();
        assert!(matches!(second, IngestMessage::Logs(_)));
        assert!(matches!(third, IngestMessage::Flush));
    }

    #[test]
    fn reserved_send_surfaces_disconnect() {
        let (sender, receiver) = channel(4);
        let guard = sender.reserve(2).expect("room");

        drop(receiver);
        assert!(matches!(
            guard.try_send(chunk_message(1)),
            Err(SendFailure::Disconnected)
        ));
        assert!(matches!(
            guard.try_send(chunk_message(1)),
            Err(SendFailure::Disconnected)
        ));
    }

    #[test]
    fn send_timeout_honors_the_deadline_when_the_queue_stays_full() {
        let (sender, _receiver) = channel(1);
        sender.send(chunk_message(1)).unwrap();

        let started = Instant::now();
        let result = sender.send_timeout(chunk_message(1), Duration::from_millis(50));
        assert!(matches!(result, Err(SendTimeoutFailure::Timeout)));
        assert!(
            started.elapsed() >= Duration::from_millis(45),
            "control send must actually wait out the deadline"
        );
    }
}
