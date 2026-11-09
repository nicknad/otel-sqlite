//! Per-run sequence accounting feeding post-run correctness validation.
//!
//! The ledger records the outcome ranges reported by workers; `freeze`
//! converts them into sorted interval sets plus a flag telling validation
//! whether rejected sequence positions are still exact (mixed partial
//! rejections inside multi-batch requests degrade them to ambiguous).

use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::client::{OutcomeKind, RequestResult};
use crate::interval_set::IntervalSet;

/// Collects per-run sequence accounting for post-run validation.
pub(crate) struct Ledger {
    rejected_ranges: Mutex<Vec<(u64, u64)>>,
    ambiguous_ranges: Mutex<Vec<(u64, u64)>>,
    positions_exact: AtomicBool,
}

impl Ledger {
    pub(crate) fn new() -> Self {
        Self {
            rejected_ranges: Mutex::new(Vec::new()),
            ambiguous_ranges: Mutex::new(Vec::new()),
            positions_exact: AtomicBool::new(true),
        }
    }

    pub(crate) fn record(&self, result: &RequestResult) {
        let range = (
            result.start_seq,
            result.start_seq + result.records as u64 - 1,
        );
        match result.kind {
            OutcomeKind::AllAccepted => {}
            OutcomeKind::PartiallyRejected { accepted, rejected } => {
                if rejected >= result.records as u64 {
                    self.rejected_ranges
                        .lock()
                        .expect("ledger lock")
                        .push(range);
                } else if accepted < result.records as u64 {
                    // Mixed partial rejection in a multi-batch request: the
                    // response reports aggregate counts only, so positions
                    // are unknown and the range degrades to ambiguous.
                    self.positions_exact.store(false, Ordering::Relaxed);
                    self.ambiguous_ranges
                        .lock()
                        .expect("ledger lock")
                        .push(range);
                }
            }
            OutcomeKind::Rejected => {
                self.rejected_ranges
                    .lock()
                    .expect("ledger lock")
                    .push(range);
            }
            OutcomeKind::Ambiguous => {
                self.ambiguous_ranges
                    .lock()
                    .expect("ledger lock")
                    .push(range);
            }
        }
    }

    pub(crate) fn freeze(&self) -> (IntervalSet, IntervalSet, bool) {
        (
            IntervalSet::from_ranges(self.rejected_ranges.lock().expect("ledger lock").clone()),
            IntervalSet::from_ranges(self.ambiguous_ranges.lock().expect("ledger lock").clone()),
            self.positions_exact.load(Ordering::Relaxed),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn result(start_seq: u64, records: usize, kind: OutcomeKind) -> RequestResult {
        RequestResult {
            start_seq,
            records,
            kind,
        }
    }

    #[test]
    fn accepted_requests_leave_the_interval_sets_empty() {
        let ledger = Ledger::new();
        ledger.record(&result(1, 10, OutcomeKind::AllAccepted));
        let (rejected, ambiguous, positions_exact) = ledger.freeze();
        assert_eq!(rejected.total(), 0);
        assert_eq!(ambiguous.total(), 0);
        assert!(positions_exact);
    }

    #[test]
    fn rejected_and_ambiguous_ranges_are_tracked_separately() {
        let ledger = Ledger::new();
        ledger.record(&result(1, 10, OutcomeKind::Rejected));
        ledger.record(&result(11, 5, OutcomeKind::Ambiguous));
        let (rejected, ambiguous, positions_exact) = ledger.freeze();
        assert_eq!(rejected.ranges(), &[(1, 10)]);
        assert_eq!(ambiguous.ranges(), &[(11, 15)]);
        assert!(positions_exact);
    }

    #[test]
    fn a_fully_rejected_partial_success_is_an_exact_rejection() {
        let ledger = Ledger::new();
        ledger.record(&result(
            1,
            4,
            OutcomeKind::PartiallyRejected {
                accepted: 0,
                rejected: 4,
            },
        ));
        let (rejected, ambiguous, positions_exact) = ledger.freeze();
        assert_eq!(rejected.ranges(), &[(1, 4)]);
        assert_eq!(ambiguous.total(), 0);
        assert!(positions_exact);
    }

    #[test]
    fn a_mixed_partial_success_degrades_positions_to_ambiguous() {
        let ledger = Ledger::new();
        ledger.record(&result(
            1,
            10,
            OutcomeKind::PartiallyRejected {
                accepted: 6,
                rejected: 4,
            },
        ));
        let (rejected, ambiguous, positions_exact) = ledger.freeze();
        assert_eq!(rejected.total(), 0);
        assert_eq!(ambiguous.ranges(), &[(1, 10)]);
        assert!(!positions_exact);
    }
}
