//! Wall-clock helpers shared by the writer thread and tests.

use std::time::{SystemTime, UNIX_EPOCH};

/// Current UNIX timestamp in nanoseconds, saturating at `i64` bounds on
/// platforms with a clock set before the epoch or beyond `i64` nanos.
pub fn unix_nano_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .map_or(0, |nanos| i64::try_from(nanos).unwrap_or(i64::MAX))
}
