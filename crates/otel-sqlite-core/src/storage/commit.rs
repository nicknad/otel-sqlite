//! Commit-ticket ledger backing the `durability = "commit"` ack mode.
//!
//! # How the durable-ack handshake works
//!
//! 1. **Issuance** — ingress stamps every accepted chunk with a ticket from
//!    [`CommitLedger::issue`] before handing it to the ingest queue.
//! 2. **Completion** — the SQLite writer calls [`CommitLedger::complete`]
//!    after the transaction containing that chunk's records has committed.
//! 3. **Waiting** — an OTLP handler that must acknowledge durably awaits
//!    [`CommitLedger::committed`], which resolves once the writer has
//!    published a commit covering the request's highest ticket.
//!
//! Tickets may complete out of order: per-origin batching emits batches in
//! arrival order per origin, but two origins can emit in either order, and a
//! rejected enqueue voids its ticket ([`CommitLedger::void`]). The published
//! [`Watermark::committed_through`] therefore advances *contiguously* — past
//! ticket `N` only once tickets `1..=N` are all completed or voided — so a
//! waiter can never be released while any of its own accepted records are
//! still uncommitted.
//!
//! The ledger doubles as a liveness signal: [`CommitLedger::close`] marks the
//! pipeline as gone (writer exited, or the batcher lost records), which fails
//! every pending and future waiter immediately instead of leaving requests
//! hanging until the server drain deadline.

use std::collections::BTreeSet;
use std::fmt;
use std::sync::LazyLock;
use std::sync::Mutex;
use std::sync::MutexGuard;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use tokio::sync::watch;

/// Process-wide epoch for monotonic stall-evidence timestamps.
static MONOTONIC_EPOCH: LazyLock<Instant> = LazyLock::new(Instant::now);

fn monotonic_millis() -> u64 {
    u64::try_from(LazyLock::force(&MONOTONIC_EPOCH).elapsed().as_millis()).unwrap_or(u64::MAX)
}

/// Published progress of the SQLite writer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Watermark {
    /// Every ticket `<= committed_through` is completed (its transaction
    /// committed) or voided (it never carried records).
    pub committed_through: u64,
    /// The owning pipeline is gone; no further completions will arrive.
    pub closed: bool,
}

/// Resolved by [`CommitLedger::committed`] when the pipeline shut down before
/// the requested ticket was committed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommitLedgerClosed;

impl fmt::Display for CommitLedgerClosed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("storage pipeline shut down before the records were committed")
    }
}

impl std::error::Error for CommitLedgerClosed {}

#[derive(Debug)]
struct Inner {
    /// Completed-or-voided tickets above `committed_through`, awaiting their
    /// predecessors so the contiguous watermark can advance. Small in
    /// practice: entries leave as soon as the missing predecessor arrives.
    settled_above: BTreeSet<u64>,
    committed_through: u64,
    /// Monotonic ms of the last time `committed_through` advanced. Evidence
    /// for stall detection: a frozen watermark while tickets are outstanding
    /// means some accepted chunk's records were lost without its ticket
    /// being voided.
    last_advance_ms: u64,
    closed: bool,
}

/// Shared registry of commit tickets; cheap to clone via [`Arc`].
///
/// One instance lives in [`crate::storage`]'s pipeline: ingress issues and
/// voids tickets, the single writer thread completes them, and waiting OTLP
/// handlers observe progress through the watch channel. All mutating calls
/// are non-blocking (short mutex sections plus a watch slot write), so the
/// writer's commit pace is unaffected by the number of waiters.
#[derive(Debug)]
pub struct CommitLedger {
    next_ticket: AtomicU64,
    inner: Mutex<Inner>,
    updates: watch::Sender<Watermark>,
}

impl Default for CommitLedger {
    fn default() -> Self {
        Self::new()
    }
}

impl CommitLedger {
    /// Creates a ledger whose watermark starts at zero with no tickets.
    pub fn new() -> Self {
        let (updates, _) = watch::channel(Watermark {
            committed_through: 0,
            closed: false,
        });
        Self {
            next_ticket: AtomicU64::new(0),
            inner: Mutex::new(Inner {
                settled_above: BTreeSet::new(),
                committed_through: 0,
                last_advance_ms: monotonic_millis(),
                closed: false,
            }),
            updates,
        }
    }

    /// Reserves the next ticket. Called by ingress after a chunk was mapped
    /// and before it is enqueued; a ticket whose enqueue fails must be
    /// reported via [`CommitLedger::void`].
    pub fn issue(&self) -> u64 {
        self.next_ticket.fetch_add(1, Ordering::Relaxed) + 1
    }

    /// Locks the ledger state, recovering from a poisoned mutex instead of
    /// panicking. Poison means some previous holder panicked mid-update; the
    /// guarded counters remain valid, and panicking here would convert that
    /// one fault into a permanent denial of service — every later OTLP
    /// export, writer commit and health poll funnels through this lock.
    /// (This crate has no logging dependency; the original panic is already
    /// visible in the panic log, so recovery itself stays silent.)
    fn lock_inner(&self) -> MutexGuard<'_, Inner> {
        match self.inner.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    /// Marks `ticket` as committed. Called by the writer after the
    /// transaction carrying the ticket's records has committed.
    pub fn complete(&self, ticket: u64) {
        self.settle(ticket);
    }

    /// Marks `ticket` as never going to carry records (enqueue rejected, or
    /// an empty chunk skipped downstream). Voided tickets release the
    /// watermark exactly like committed ones — there is nothing to lose.
    pub fn void(&self, ticket: u64) {
        self.settle(ticket);
    }

    fn settle(&self, ticket: u64) {
        if ticket == 0 {
            return;
        }
        let watermark = {
            let mut inner = self.lock_inner();
            if ticket == inner.committed_through + 1 {
                inner.committed_through = ticket;
                inner.last_advance_ms = monotonic_millis();
                let mut next = ticket + 1;
                while inner.settled_above.remove(&next) {
                    inner.committed_through = next;
                    next += 1;
                }
            } else if ticket > inner.committed_through {
                inner.settled_above.insert(ticket);
            }
            Watermark {
                committed_through: inner.committed_through,
                closed: inner.closed,
            }
        };
        self.updates.send_replace(watermark);
    }

    /// Marks the owning pipeline as gone. Every pending and future
    /// [`CommitLedger::committed`] call resolves with [`CommitLedgerClosed`].
    pub fn close(&self) {
        let watermark = {
            let mut inner = self.lock_inner();
            inner.closed = true;
            Watermark {
                committed_through: inner.committed_through,
                closed: true,
            }
        };
        self.updates.send_replace(watermark);
    }

    /// Current published progress; a cheap synchronous snapshot.
    pub fn watermark(&self) -> Watermark {
        *self.updates.borrow()
    }

    /// Number of issued tickets that have been neither completed nor voided.
    ///
    /// In steady operation this counts only records currently in flight
    /// (buffered in the insert batcher, queued for the writer, or mid-
    /// transaction). A ticket that stays outstanding while its records are
    /// visible nowhere in the pipeline is a *leak*: some drop path forgot to
    /// void it, and it will hold the contiguous watermark — and therefore
    /// every durable ack behind it — forever. Health sampling surfaces this
    /// via `outstanding_tickets` so a watchdog can raise the alarm.
    pub fn outstanding_tickets(&self) -> u64 {
        let issued = self.next_ticket.load(Ordering::Relaxed);
        let inner = self.lock_inner();
        issued
            .saturating_sub(inner.committed_through)
            .saturating_sub(u64::try_from(inner.settled_above.len()).unwrap_or(u64::MAX))
    }

    /// Monotonic milliseconds of the last time the contiguous watermark
    /// advanced. Out-of-order settlements that only park tickets above the
    /// watermark do not refresh this; only genuine progress does. Combined
    /// with [`CommitLedger::outstanding_tickets`] this distinguishes "no
    /// commits lately" from "the commit watermark is stuck".
    pub fn last_advance_ms(&self) -> u64 {
        self.lock_inner().last_advance_ms
    }

    /// Waits until every record behind `ticket` is durably committed, or the
    /// pipeline closes first. Resolves immediately when the watermark already
    /// covers the ticket.
    ///
    /// A dropped ledger also reports [`CommitLedgerClosed`] — without an
    /// owner nobody can complete anything anymore.
    pub async fn committed(&self, ticket: u64) -> Result<(), CommitLedgerClosed> {
        if ticket == 0 {
            return Ok(());
        }
        let mut updates = self.updates.subscribe();
        loop {
            let state = *updates.borrow_and_update();
            if state.closed {
                return Err(CommitLedgerClosed);
            }
            if state.committed_through >= ticket {
                return Ok(());
            }
            updates.changed().await.map_err(|_| CommitLedgerClosed)?;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::Duration;

    #[test]
    fn issued_tickets_are_monotonic() {
        let ledger = CommitLedger::new();
        assert_eq!(ledger.issue(), 1);
        assert_eq!(ledger.issue(), 2);
        assert_eq!(ledger.issue(), 3);
        assert_eq!(
            ledger.watermark(),
            Watermark {
                committed_through: 0,
                closed: false
            }
        );
    }

    #[test]
    fn contiguous_completions_advance_the_watermark() {
        let ledger = CommitLedger::new();
        let first = ledger.issue();
        let second = ledger.issue();

        ledger.complete(first);
        assert_eq!(ledger.watermark().committed_through, first);

        ledger.complete(second);
        assert_eq!(ledger.watermark().committed_through, second);
    }

    #[test]
    fn out_of_order_completion_holds_back_the_watermark() {
        let ledger = CommitLedger::new();
        let first = ledger.issue();
        let second = ledger.issue();
        let third = ledger.issue();

        // The batcher emitted origin-B traffic (tickets 2 and 3) before the
        // still-buffered origin-A batch (ticket 1).
        ledger.complete(third);
        ledger.complete(second);
        assert_eq!(
            ledger.watermark(),
            Watermark {
                committed_through: 0,
                closed: false
            },
            "ticket 1 is still outstanding; nobody may be told the data is committed"
        );

        ledger.complete(first);
        assert_eq!(ledger.watermark().committed_through, third);
        assert_eq!(ledger.inner.lock().unwrap().settled_above.len(), 0);
    }

    #[tokio::test]
    async fn voided_tickets_pass_the_watermark_like_completed_ones() {
        let ledger = CommitLedger::new();
        let rejected = ledger.issue();
        let accepted = ledger.issue();

        // Ticket `rejected` never reached the queue, so it carries no
        // durability promise: it must not stall later commits.
        ledger.void(rejected);
        assert_eq!(
            ledger.watermark().committed_through,
            rejected,
            "a leading void releases its own position"
        );

        // The accepted ticket stays outstanding, though: waiters for it must
        // not be released by the unrelated void.
        let pending =
            tokio::time::timeout(Duration::from_millis(50), ledger.committed(accepted)).await;
        assert!(
            pending.is_err(),
            "the outstanding accepted ticket must keep its waiter waiting"
        );

        ledger.complete(accepted);
        assert_eq!(ledger.watermark().committed_through, accepted);
        assert!(ledger.committed(accepted).await.is_ok());
    }

    #[test]
    fn duplicate_settlement_is_idempotent() {
        let ledger = CommitLedger::new();
        let first = ledger.issue();
        let second = ledger.issue();

        ledger.complete(first);
        ledger.complete(first);
        ledger.void(first);
        ledger.complete(second);
        assert_eq!(ledger.watermark().committed_through, second);
    }

    /// Every documented path by which an issued ticket's records can vanish
    /// must still let the watermark advance (or fail waiters via `close`).
    /// Missing any one of these is the head-of-line stall the leak detection
    /// exists to catch.
    #[tokio::test]
    async fn every_drop_path_settles_its_ticket() {
        // 1. Enqueue rejected in ingress: the ticket is voided at the send
        //    site and releases its position immediately.
        {
            let ledger = CommitLedger::new();
            let rejected = ledger.issue();
            ledger.void(rejected);
            assert_eq!(ledger.watermark().committed_through, rejected);
        }

        // 2. Empty chunk skipped downstream: the batcher/writer voids the
        //    tickets of a chunk that carries no records.
        {
            let ledger = CommitLedger::new();
            let first = ledger.issue();
            let empty = ledger.issue();
            let later = ledger.issue();
            ledger.complete(first);
            // While the empty chunk's ticket is unsettled it holds the
            // watermark back...
            assert_eq!(ledger.watermark().committed_through, first);
            // ...but once voided it releases its position like a commit.
            ledger.void(empty);
            ledger.complete(later);
            assert_eq!(ledger.watermark().committed_through, later);
        }

        // 3. Writer death mid-stream: close() fails pending and future
        //    waiters instead of leaving them waiting on commits that can no
        //    longer happen.
        {
            let ledger = Arc::new(CommitLedger::new());
            let committed = ledger.issue();
            let stranded = ledger.issue();
            ledger.complete(committed);

            let waiter = {
                let ledger = Arc::clone(&ledger);
                tokio::spawn(async move { ledger.committed(stranded).await })
            };
            tokio::time::sleep(Duration::from_millis(20)).await;

            ledger.close();
            let result = tokio::time::timeout(Duration::from_secs(2), waiter)
                .await
                .expect("waiter finishes after close")
                .expect("no panic");
            assert_eq!(result, Err(CommitLedgerClosed));
            assert!(ledger.watermark().closed);
        }
    }

    #[test]
    fn outstanding_tickets_tracks_in_flight_and_leaks() {
        let ledger = CommitLedger::new();
        assert_eq!(ledger.outstanding_tickets(), 0);

        let first = ledger.issue();
        let second = ledger.issue();
        let third = ledger.issue();
        assert_eq!(
            ledger.outstanding_tickets(),
            3,
            "every issued ticket counts until settled"
        );

        // Out-of-order settlement parks ticket 3 but keeps all three
        // "unsettled from the watermark's point of view".
        ledger.complete(third);
        assert_eq!(ledger.outstanding_tickets(), 2);

        ledger.complete(first);
        assert_eq!(ledger.outstanding_tickets(), 1);

        // The remaining leaked ticket keeps the count pinned at 1 forever:
        // this is the quantity watchdog stall detection watches.
        std::thread::sleep(Duration::from_millis(5));
        assert_eq!(ledger.outstanding_tickets(), 1);
        assert_eq!(ledger.watermark().committed_through, second - 1);

        ledger.complete(second);
        assert_eq!(ledger.outstanding_tickets(), 0);
    }

    #[test]
    fn last_advance_ms_reflects_only_genuine_progress() {
        let ledger = CommitLedger::new();
        let issued_at = ledger.last_advance_ms();

        // Parking a ticket above the watermark is bookkeeping, not progress.
        let first = ledger.issue();
        let second = ledger.issue();
        ledger.complete(second);
        assert_eq!(
            ledger.last_advance_ms(),
            issued_at,
            "out-of-order settlement must not refresh the progress timestamp"
        );

        std::thread::sleep(Duration::from_millis(5));
        ledger.complete(first);
        assert!(
            ledger.last_advance_ms() >= issued_at + 5,
            "advancing the watermark refreshes the progress timestamp"
        );
    }

    /// A poisoned mutex must degrade, not deadlock the pipeline: after one
    /// holder panics mid-update, every later call recovers the guarded
    /// counters instead of panicking on the OTLP hot path.
    #[test]
    fn ledger_recovers_from_a_poisoned_lock() {
        let ledger = Arc::new(CommitLedger::new());
        let ticket = ledger.issue();

        // Poison the mutex by panicking while holding it on another thread.
        let holder = Arc::clone(&ledger);
        let outcome = std::thread::spawn(move || {
            let _guard = holder.inner.lock().unwrap();
            panic!("intentional poison for recovery test");
        })
        .join();
        assert!(outcome.is_err(), "the holder thread must have panicked");

        // All lock users must work afterwards instead of panicking.
        ledger.complete(ticket);
        assert_eq!(ledger.watermark().committed_through, ticket);
        assert_eq!(ledger.outstanding_tickets(), 0);
        let _ = ledger.last_advance_ms();
        ledger.close();
        assert!(ledger.watermark().closed);
    }

    #[tokio::test]
    async fn committed_resolves_for_an_already_satisfied_ticket() {
        let ledger = Arc::new(CommitLedger::new());
        let ticket = ledger.issue();
        ledger.complete(ticket);

        assert!(ledger.committed(ticket).await.is_ok());
        // Ticket zero is the "nothing accepted" sentinel and never waits.
        assert!(ledger.committed(0).await.is_ok());
    }

    #[tokio::test]
    async fn committed_wakes_up_when_the_writer_publishes() {
        let ledger = Arc::new(CommitLedger::new());
        let ticket = ledger.issue();
        let waiter = {
            let ledger = Arc::clone(&ledger);
            tokio::spawn(async move { ledger.committed(ticket).await })
        };

        // Give the waiter time to register, then commit on another task.
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!waiter.is_finished(), "waiter must still be pending");
        ledger.complete(ticket);

        let result = tokio::time::timeout(Duration::from_secs(2), waiter)
            .await
            .expect("waiter finishes after completion")
            .expect("waiter task did not panic");
        assert_eq!(result, Ok(()));
    }

    #[tokio::test]
    async fn committed_fails_once_the_pipeline_closes() {
        let ledger = Arc::new(CommitLedger::new());
        let ticket = ledger.issue();
        let waiter = {
            let ledger = Arc::clone(&ledger);
            tokio::spawn(async move { ledger.committed(ticket).await })
        };

        tokio::time::sleep(Duration::from_millis(20)).await;
        ledger.close();

        let result = tokio::time::timeout(Duration::from_secs(2), waiter)
            .await
            .expect("waiter finishes after close")
            .expect("waiter task did not panic");
        assert_eq!(result, Err(CommitLedgerClosed));
        // Late waiters must fail too instead of hanging forever.
        assert_eq!(ledger.committed(ticket).await, Err(CommitLedgerClosed));
    }

    #[tokio::test]
    async fn committed_survives_out_of_order_settlements_without_wakeup_loss() {
        let ledger = Arc::new(CommitLedger::new());
        let first = ledger.issue();
        let second = ledger.issue(); // stays outstanding the whole time
        let third = ledger.issue();

        // Settle ticket 3 first: it parks in the hole set and publishes an
        // update that does not help the waiter for ticket 1.
        ledger.complete(third);
        let waiter = {
            let ledger = Arc::clone(&ledger);
            tokio::spawn(async move { ledger.committed(first).await })
        };
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!waiter.is_finished(), "ticket 2 still blocks the watermark");

        ledger.complete(first);

        let result = tokio::time::timeout(Duration::from_secs(2), waiter)
            .await
            .expect("waiter finishes")
            .expect("no panic");
        assert_eq!(result, Ok(()));
        assert_eq!(
            ledger.watermark().committed_through,
            second - 1,
            "the outstanding ticket keeps holding the watermark back"
        );
    }
}
