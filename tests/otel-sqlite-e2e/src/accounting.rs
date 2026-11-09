//! Per-run accounting shared by normal benchmark steps and crash recovery.
//!
//! The client counters and the ledger interval sets are two views of the same
//! requests. Keeping them in one value makes their agreement explicit: the
//! ledger is only trusted after its ranges reproduce the atomic counters and
//! the derived expected-row set.

use anyhow::bail;

use crate::client::ClientCountersSnapshot;
use crate::interval_set::IntervalSet;

/// Client counters plus the ledger-derived rejected/ambiguous sequence sets
/// for one run.
#[derive(Debug, Clone)]
pub struct RunAccounting {
    pub counters: ClientCountersSnapshot,
    /// Sequences definitively rejected by the server.
    pub rejected: IntervalSet,
    /// Sequences whose fate is unknown client-side.
    pub ambiguous: IntervalSet,
    /// False when a mixed partial rejection made rejected positions
    /// unknowable. Crash recovery refuses to bless such a run.
    pub positions_exact: bool,
}

impl RunAccounting {
    /// Builds a value and verifies the two views agree.
    pub fn new(
        counters: ClientCountersSnapshot,
        rejected: IntervalSet,
        ambiguous: IntervalSet,
        positions_exact: bool,
    ) -> anyhow::Result<Self> {
        let accounting = Self {
            counters,
            rejected,
            ambiguous,
            positions_exact,
        };
        accounting.check()?;
        Ok(accounting)
    }

    /// Asserts the counter identity and that the ledger ranges partition the
    /// generated sequences. When rejected positions are exact, the ledger
    /// totals must also reproduce the client counters exactly. When a mixed
    /// partial rejection made positions unknowable, the whole request is
    /// conservatively in the ambiguous set, so only the directional bounds
    /// hold: the expected set can only shrink by accepted records hidden in an
    /// ambiguous range.
    pub fn check(&self) -> anyhow::Result<()> {
        let counters = &self.counters;
        let classified = counters
            .records_accepted
            .checked_add(counters.records_rejected)
            .and_then(|total| total.checked_add(counters.records_ambiguous))
            .unwrap_or(u64::MAX);
        if classified != counters.records_generated {
            bail!(
                "accounting mismatch: generated {} != accepted {} + rejected {} + ambiguous {}",
                counters.records_generated,
                counters.records_accepted,
                counters.records_rejected,
                counters.records_ambiguous,
            );
        }

        let expected = self.expected_persisted();
        let rejected_total = self.rejected.total();
        let ambiguous_total = self.ambiguous.total();
        let partition = expected
            .total()
            .saturating_add(rejected_total)
            .saturating_add(ambiguous_total);
        if partition != counters.records_generated {
            bail!(
                "ledger ranges partition {partition} of {} generated sequences \
                 (expected {}, rejected {rejected_total}, ambiguous {ambiguous_total}); \
                 a range is overlapping or outside the run window",
                counters.records_generated,
                expected.total(),
            );
        }

        if self.positions_exact {
            if rejected_total != counters.records_rejected {
                bail!(
                    "ledger rejected ranges total {rejected_total}, but the client rejected counter is {}",
                    counters.records_rejected,
                );
            }
            if ambiguous_total != counters.records_ambiguous {
                bail!(
                    "ledger ambiguous ranges total {ambiguous_total}, but the client ambiguous counter is {}",
                    counters.records_ambiguous,
                );
            }
            if expected.total() != counters.records_accepted {
                bail!(
                    "ledger-derived expected rows {} disagree with accepted counter {} \
                     (generated {}, rejected {rejected_total}, ambiguous {ambiguous_total})",
                    expected.total(),
                    counters.records_accepted,
                    counters.records_generated,
                );
            }
        } else {
            if rejected_total > counters.records_rejected {
                bail!(
                    "ledger rejected ranges total {rejected_total} exceeds the client \
                     rejected counter {}",
                    counters.records_rejected,
                );
            }
            if ambiguous_total < counters.records_ambiguous {
                bail!(
                    "ledger ambiguous ranges total {ambiguous_total} is smaller than the \
                     client ambiguous counter {}",
                    counters.records_ambiguous,
                );
            }
            if expected.total() > counters.records_accepted {
                bail!(
                    "ledger-derived expected rows {} exceed the accepted counter {}",
                    expected.total(),
                    counters.records_accepted,
                );
            }
        }
        Ok(())
    }

    /// Sequences that must be present in SQLite: `[1, generated]` minus the
    /// rejected and ambiguous ranges.
    pub fn expected_persisted(&self) -> IntervalSet {
        IntervalSet::from_ranges(vec![(1, self.counters.records_generated)])
            .subtract(&self.rejected)
            .subtract(&self.ambiguous)
    }

    /// Number of rows validation must find for this run.
    pub fn expected_rows(&self) -> u64 {
        self.expected_persisted().total()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn counters(
        generated: u64,
        accepted: u64,
        rejected: u64,
        ambiguous: u64,
    ) -> ClientCountersSnapshot {
        ClientCountersSnapshot {
            records_generated: generated,
            records_accepted: accepted,
            records_rejected: rejected,
            records_ambiguous: ambiguous,
            ..ClientCountersSnapshot::default()
        }
    }

    fn ranges(ranges: &[(u64, u64)]) -> IntervalSet {
        IntervalSet::from_ranges(ranges.to_vec())
    }

    #[test]
    fn accepted_only_run_expects_the_full_range() {
        let accounting =
            RunAccounting::new(counters(100, 100, 0, 0), ranges(&[]), ranges(&[]), true)
                .expect("consistent accounting");
        assert_eq!(accounting.expected_rows(), 100);
        assert_eq!(accounting.expected_persisted().ranges(), &[(1, 100)]);
    }

    #[test]
    fn rejected_and_ambiguous_ranges_are_subtracted() {
        let accounting = RunAccounting::new(
            counters(100, 78, 11, 11),
            ranges(&[(10, 20)]),
            ranges(&[(30, 40)]),
            true,
        )
        .expect("consistent accounting");
        assert_eq!(accounting.expected_rows(), 78);
        assert_eq!(
            accounting.expected_persisted().ranges(),
            &[(1, 9), (21, 29), (41, 100)]
        );
    }

    #[test]
    fn counter_identity_is_required() {
        let error = RunAccounting::new(
            counters(100, 70, 20, 0),
            ranges(&[(81, 100)]),
            ranges(&[]),
            true,
        )
        .expect_err("missing classification must fail");
        assert!(error.to_string().contains("accounting mismatch"), "{error}");
    }

    #[test]
    fn ledger_totals_must_match_counters() {
        let error = RunAccounting::new(
            counters(100, 80, 10, 10),
            ranges(&[(81, 90)]),
            ranges(&[(91, 99)]),
            true,
        )
        .expect_err("ledger totals must match counters");
        assert!(
            error.to_string().contains("ambiguous ranges total"),
            "{error}"
        );
    }

    #[test]
    fn overlapping_ranges_cannot_shrink_the_expected_set() {
        let error = RunAccounting::new(
            counters(100, 78, 11, 11),
            ranges(&[(10, 20)]),
            ranges(&[(15, 25)]),
            true,
        )
        .expect_err("overlap makes ledger-derived rows disagree with accepted");
        assert!(error.to_string().contains("partition"), "{error}");
    }

    #[test]
    fn ranges_outside_the_generated_window_fail_the_derivation() {
        let error = RunAccounting::new(
            counters(100, 89, 11, 0),
            ranges(&[(95, 105)]),
            ranges(&[]),
            true,
        )
        .expect_err("out-of-window rejected positions are a ledger bug");
        assert!(error.to_string().contains("partition"), "{error}");
    }

    #[test]
    fn inexact_positions_allow_accepted_records_inside_an_ambiguous_range() {
        // A mixed partial rejection of 20 records (10 accepted, 10 rejected)
        // is recorded as one ambiguous range because positions are unknown.
        let accounting = RunAccounting::new(
            counters(100, 70, 10, 20),
            ranges(&[]),
            ranges(&[(61, 80), (81, 100)]),
            false,
        )
        .expect("conservative accounting is consistent");
        assert_eq!(accounting.expected_rows(), 60);
    }

    #[test]
    fn inexact_positions_still_reject_overlapping_ranges() {
        let error = RunAccounting::new(
            counters(100, 70, 10, 20),
            ranges(&[(61, 70)]),
            ranges(&[(61, 80), (81, 100)]),
            false,
        )
        .expect_err("overlap must shrink the partition");
        assert!(error.to_string().contains("partition"), "{error}");
    }
}
