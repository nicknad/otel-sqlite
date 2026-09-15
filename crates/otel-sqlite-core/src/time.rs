//! Clock helpers shared by the writer thread, health sampling and tests.

use std::sync::LazyLock;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Process-wide epoch for monotonic health timestamps. Values are comparable
/// across threads and components because everyone converts through this same
/// instant; a second epoch elsewhere would silently mix clock bases and make
/// idle-time arithmetic meaningless.
static MONOTONIC_EPOCH: LazyLock<Instant> = LazyLock::new(Instant::now);

/// Milliseconds elapsed since the process-wide monotonic epoch, saturating at
/// `u64::MAX` on clocks that run longer than ~584 million years.
pub fn monotonic_millis() -> u64 {
    u64::try_from(LazyLock::force(&MONOTONIC_EPOCH).elapsed().as_millis()).unwrap_or(u64::MAX)
}

/// `now + after`, saturating instead of panicking when `Instant` cannot
/// represent the result. Callers may pass absurd durations (`Duration::MAX`
/// from configuration or tests); the deadline then clamps to the farthest
/// representable instant rather than aborting the thread.
pub fn saturating_deadline(now: Instant, after: Duration) -> Instant {
    let mut after = after;
    loop {
        if let Some(deadline) = now.checked_add(after) {
            return deadline;
        }
        // Halving terminates: once `after` reaches zero the add always fits.
        after /= 2;
    }
}

/// Current UNIX timestamp in nanoseconds, saturating at `i64` bounds on
/// platforms with a clock set before the epoch or beyond `i64` nanos.
pub fn unix_nano_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            i64::try_from(duration.as_nanos()).unwrap_or(i64::MAX)
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn monotonic_millis_never_goes_backwards() {
        let first = monotonic_millis();
        std::thread::sleep(Duration::from_millis(2));
        let second = monotonic_millis();
        assert!(second >= first);
    }

    #[test]
    fn saturating_deadline_survives_absurd_durations() {
        let now = Instant::now();
        assert_eq!(saturating_deadline(now, Duration::ZERO), now);
        assert!(saturating_deadline(now, Duration::MAX) >= now);
        assert!(saturating_deadline(now, Duration::from_secs(u64::MAX)) >= now);
    }
}
