//! Wall-clock helpers shared by the writer thread and tests.

use std::time::{SystemTime, UNIX_EPOCH};

/// Current UNIX timestamp in nanoseconds, saturating at `i64` bounds on
/// platforms with a clock set before the epoch or beyond `i64` nanos.
pub fn unix_nano_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            i64::try_from(duration.as_nanos()).unwrap_or(i64::MAX)
        })
}
