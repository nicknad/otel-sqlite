//! Open-loop request pacing shared by all workers of a scenario step.
//!
//! Slots are handed out at a fixed period; a worker behind schedule fires
//! immediately so the offered load does not collapse under transient stalls
//! (realistic overload behaviour rather than self-throttling).

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::time::Instant as TokioInstant;

pub(crate) struct Pacer {
    start: TokioInstant,
    period: Option<Duration>,
    next_slot: AtomicU64,
}

impl Pacer {
    pub(crate) fn new(offered_requests_per_second: Option<f64>) -> Self {
        Self {
            start: TokioInstant::now(),
            period: offered_requests_per_second
                .filter(|rate| *rate > 0.0)
                .map(|rate| Duration::from_secs_f64(1.0 / rate)),
            next_slot: AtomicU64::new(0),
        }
    }

    pub(crate) async fn acquire(&self) {
        let Some(period) = self.period else {
            return;
        };
        let slot = self.next_slot.fetch_add(1, Ordering::Relaxed);
        let target = self.start + period * slot.min(u64::from(u32::MAX)) as u32;
        if target > TokioInstant::now() {
            tokio::time::sleep_until(target).await;
        }
    }
}
