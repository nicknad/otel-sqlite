//! Sorted, non-overlapping inclusive ranges of sequence numbers.
//!
//! Generic utility with no dependency on SQLite or the harness; used by the
//! ledger to accumulate rejected/ambiguous sequence ranges and by validation
//! to compute expected-persisted sets.

#[derive(Debug, Clone, Default)]
pub struct IntervalSet {
    pub(crate) ranges: Vec<(u64, u64)>,
}

impl IntervalSet {
    pub fn from_ranges(mut ranges: Vec<(u64, u64)>) -> Self {
        ranges.retain(|(start, end)| start <= end);
        ranges.sort_unstable();
        let mut merged: Vec<(u64, u64)> = Vec::with_capacity(ranges.len());
        for (start, end) in ranges {
            match merged.last_mut() {
                Some((_, last_end)) if start <= last_end.saturating_add(1) => {
                    *last_end = (*last_end).max(end);
                }
                _ => merged.push((start, end)),
            }
        }
        Self { ranges: merged }
    }

    pub fn total(&self) -> u64 {
        self.ranges.iter().map(|(start, end)| end - start + 1).sum()
    }

    /// The sorted, merged inclusive ranges backing this set.
    pub fn ranges(&self) -> &[(u64, u64)] {
        &self.ranges
    }

    pub fn contains(&self, value: u64) -> bool {
        self.ranges
            .binary_search_by(|(start, end)| {
                if value < *start {
                    std::cmp::Ordering::Greater
                } else if value > *end {
                    std::cmp::Ordering::Less
                } else {
                    std::cmp::Ordering::Equal
                }
            })
            .is_ok()
    }

    /// `self` minus `other`, as a new set.
    #[must_use]
    pub fn subtract(&self, other: &IntervalSet) -> IntervalSet {
        let mut result: Vec<(u64, u64)> = Vec::new();
        for &(start, end) in &self.ranges {
            let mut cursor = start;
            for &(o_start, o_end) in &other.ranges {
                if o_end < cursor || o_start > end {
                    continue;
                }
                if o_start > cursor {
                    result.push((cursor, o_start - 1));
                }
                cursor = cursor.max(o_end.saturating_add(1));
                if cursor > end {
                    break;
                }
            }
            if cursor <= end {
                result.push((cursor, end));
            }
        }
        Self::from_ranges(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn interval_set_basics() {
        let set = IntervalSet::from_ranges(vec![(5, 10), (1, 3), (11, 20), (2, 4)]);
        // (1,3)+(2,4) -> (1,4); adjacent to (5,10) -> (1,10); adjacent to
        // (11,20) -> (1,20).
        assert_eq!(set.total(), 20);
        assert!(set.contains(1));
        assert!(set.contains(4));
        assert!(set.contains(15));
        assert!(!set.contains(21));

        let minus = set.subtract(&IntervalSet::from_ranges(vec![(7, 9)]));
        assert_eq!(minus.total(), 17);
        assert!(!minus.contains(7));
        assert!(!minus.contains(9));
        assert!(minus.contains(6));
        assert!(minus.contains(10));
    }
}
