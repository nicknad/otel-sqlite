//! Runtime worker configuration.
//!
//! Two workers, two configs:
//!
//! * [`MaintenanceConfig`] — scheduling of prune/checkpoint/optimize/vacuum/
//!   FTS-rebuild commands enqueued onto the shared command queue.
//! * [`WatchdogConfig`] — observation intervals and thresholds for the
//!   health watchdog.

use std::time::Duration;

use otel_sqlite_core::storage::CheckpointMode;
use thiserror::Error;

#[derive(Debug, Clone)]
pub struct MaintenanceConfig {
    /// Retention window for log events. Older rows are deleted by the writer
    /// when it executes a prune command; the worker never touches data.
    ///
    /// `None` disables purging entirely.
    pub retention: Option<Duration>,
    /// How often a retention purge command is enqueued. Ignored while
    /// `retention` is `None`.
    pub purge_interval: Option<Duration>,
    /// How often a WAL checkpoint command is enqueued.
    ///
    /// Periodic [`CheckpointMode::Passive`] keeps the WAL from growing
    /// without blocking readers or writers.
    pub checkpoint_interval: Option<Duration>,
    /// Checkpoint strategy for scheduled checkpoints.
    pub checkpoint_mode: CheckpointMode,
    /// How often an `ANALYZE` ("optimize") command is enqueued so SQLite's
    /// query planner statistics stay current.
    pub optimize_interval: Option<Duration>,
    /// How often a `VACUUM` command is enqueued.
    ///
    /// VACUUM can be expensive; keep this infrequent.
    pub vacuum_interval: Option<Duration>,
    /// How often a full search-index rebuild is enqueued.
    ///
    /// The index is maintained incrementally, so a rebuild is only recovery
    /// for corruption or schema changes; it re-reads every log row and is
    /// therefore disabled by default.
    pub rebuild_fts_interval: Option<Duration>,
    /// Delay before retrying an enqueue that failed because the command queue
    /// was full. Keeps maintenance best-effort: the operation stays due and is
    /// retried at a controlled pace instead of spinning against the queue.
    pub retry_delay: Duration,
}

impl Default for MaintenanceConfig {
    fn default() -> Self {
        Self {
            retention: None,
            purge_interval: Some(Duration::from_secs(15 * 60)),
            checkpoint_interval: Some(Duration::from_secs(5 * 60)),
            checkpoint_mode: CheckpointMode::Passive,
            optimize_interval: Some(Duration::from_secs(12 * 60 * 60)),
            vacuum_interval: Some(Duration::from_secs(24 * 60 * 60)),
            rebuild_fts_interval: None,
            retry_delay: Duration::from_secs(2),
        }
    }
}

/// Watchdog observation policy.
///
/// The watchdog derives health from evidence (progress ages, pending work,
/// thread liveness) instead of asking components questions. Thresholds are
/// deliberately conservative by default: a long-running `VACUUM` or a large
/// retention prune can legitimately pause insert commits for a while, and a
/// false "unhealthy" verdict triggers a process restart.
#[derive(Debug, Clone)]
pub struct WatchdogConfig {
    /// How often the watchdog samples pipeline evidence.
    ///
    /// Values below 10 ms are raised to that floor by [`crate::Watchdog::new`]
    /// so a misconfiguration cannot turn observation into a busy loop.
    pub tick_interval: Duration,
    /// Progress age (while pending work exists) at which the pipeline is
    /// reported degraded.
    pub degraded_after: Duration,
    /// Progress age (while pending work exists) beyond which the pipeline is
    /// declared unhealthy.
    pub unhealthy_after: Duration,
    /// How long the commit watermark may stay frozen while durability
    /// tickets are outstanding but nothing is queued or buffered. Beyond
    /// this, accepted records can never be acknowledged — the pipeline has
    /// leaked a ticket and must be restarted. Only meaningful in
    /// `durability = "commit"` deployments; keep it well above the largest
    /// expected commit gap.
    pub ack_stall_after: Duration,
    /// How long a single maintenance operation (checkpoint, prune, vacuum,
    /// FTS rebuild, quota eviction) may run before the watchdog treats it as
    /// a genuine hang. While maintenance is in progress the ordinary stall
    /// rules are suspended — the writer legitimately stops committing — so
    /// this is the only bound on it. Must exceed the longest expected
    /// `VACUUM`/rebuild.
    pub maintenance_stall_after: Duration,
    /// Fraction of the command queue depth treated as saturation pressure
    /// (`0.0 < r <= 1.0`). Reaching it reports degraded even while progress
    /// continues — the "SQLite is slow" signal. Clamped into range.
    pub queue_pressure_ratio: f32,
    /// When unhealthy, send the halt signal that stops OTLP ingestion and
    /// initiates graceful process shutdown so an external supervisor can
    /// restart the sidecar. If `false`, the watchdog only logs and emits
    /// metrics.
    pub halt_on_unhealthy: bool,
}

impl Default for WatchdogConfig {
    fn default() -> Self {
        Self {
            tick_interval: Duration::from_secs(5),
            degraded_after: Duration::from_secs(30),
            unhealthy_after: Duration::from_secs(120),
            ack_stall_after: Duration::from_secs(60),
            maintenance_stall_after: Duration::from_secs(30 * 60),
            queue_pressure_ratio: 0.75,
            halt_on_unhealthy: true,
        }
    }
}

/// A [`WatchdogConfig`] that cannot produce meaningful verdicts.
#[derive(Debug, Clone, PartialEq, Error)]
pub enum WatchdogConfigError {
    #[error("watchdog.{0} must be greater than zero")]
    ZeroDuration(&'static str),
    #[error(
        "watchdog.degraded_after ({degraded:?}) must be less than \
         watchdog.unhealthy_after ({unhealthy:?}); otherwise the degraded state is unreachable"
    )]
    DegradedNotBeforeUnhealthy {
        degraded: Duration,
        unhealthy: Duration,
    },
    #[error(
        "watchdog.maintenance_stall_after ({maintenance:?}) must be at least \
         watchdog.unhealthy_after ({unhealthy:?}); maintenance suspends the ordinary stall rules"
    )]
    MaintenanceStallBeforeUnhealthy {
        maintenance: Duration,
        unhealthy: Duration,
    },
    #[error("watchdog.queue_pressure_ratio must be finite and in (0.0, 1.0]; got {0}")]
    InvalidQueuePressureRatio(f32),
}

impl WatchdogConfig {
    /// Rejects orderings and ranges that would make verdicts unreachable or
    /// nonsensical.
    ///
    /// The binary applies the same rules while loading its config file; this
    /// method protects library embedders that build the struct directly.
    /// [`crate::Watchdog::new`] calls it automatically.
    pub fn validate(&self) -> Result<(), WatchdogConfigError> {
        for (name, value) in [
            ("degraded_after", self.degraded_after),
            ("unhealthy_after", self.unhealthy_after),
            ("ack_stall_after", self.ack_stall_after),
            ("maintenance_stall_after", self.maintenance_stall_after),
        ] {
            if value.is_zero() {
                return Err(WatchdogConfigError::ZeroDuration(name));
            }
        }
        if self.degraded_after >= self.unhealthy_after {
            return Err(WatchdogConfigError::DegradedNotBeforeUnhealthy {
                degraded: self.degraded_after,
                unhealthy: self.unhealthy_after,
            });
        }
        if self.maintenance_stall_after < self.unhealthy_after {
            return Err(WatchdogConfigError::MaintenanceStallBeforeUnhealthy {
                maintenance: self.maintenance_stall_after,
                unhealthy: self.unhealthy_after,
            });
        }
        let ratio = self.queue_pressure_ratio;
        if !ratio.is_finite() || ratio <= 0.0 || ratio > 1.0 {
            return Err(WatchdogConfigError::InvalidQueuePressureRatio(ratio));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_is_valid() {
        WatchdogConfig::default()
            .validate()
            .expect("defaults must be usable");
    }

    #[test]
    fn degraded_must_precede_unhealthy() {
        let config = WatchdogConfig {
            degraded_after: Duration::from_secs(120),
            unhealthy_after: Duration::from_secs(120),
            ..WatchdogConfig::default()
        };
        assert_eq!(
            config.validate(),
            Err(WatchdogConfigError::DegradedNotBeforeUnhealthy {
                degraded: Duration::from_secs(120),
                unhealthy: Duration::from_secs(120),
            })
        );
    }

    #[test]
    fn maintenance_stall_must_not_be_shorter_than_unhealthy() {
        let config = WatchdogConfig {
            maintenance_stall_after: Duration::from_secs(10),
            unhealthy_after: Duration::from_secs(120),
            ..WatchdogConfig::default()
        };
        assert!(matches!(
            config.validate(),
            Err(WatchdogConfigError::MaintenanceStallBeforeUnhealthy { .. })
        ));
    }

    #[test]
    fn queue_pressure_ratio_must_be_a_finite_fraction() {
        for ratio in [f32::NAN, f32::INFINITY, 0.0, -0.1, 1.1] {
            let config = WatchdogConfig {
                queue_pressure_ratio: ratio,
                ..WatchdogConfig::default()
            };
            assert!(
                matches!(
                    config.validate(),
                    Err(WatchdogConfigError::InvalidQueuePressureRatio(_))
                ),
                "ratio {ratio} must be rejected"
            );
        }
        for ratio in [f32::MIN_POSITIVE, 0.5, 1.0] {
            let config = WatchdogConfig {
                queue_pressure_ratio: ratio,
                ..WatchdogConfig::default()
            };
            config.validate().expect("valid ratio");
        }
    }

    #[test]
    fn zero_thresholds_are_rejected() {
        for (name, config) in [
            (
                "degraded_after",
                WatchdogConfig {
                    degraded_after: Duration::ZERO,
                    ..WatchdogConfig::default()
                },
            ),
            (
                "unhealthy_after",
                WatchdogConfig {
                    unhealthy_after: Duration::ZERO,
                    ..WatchdogConfig::default()
                },
            ),
            (
                "ack_stall_after",
                WatchdogConfig {
                    ack_stall_after: Duration::ZERO,
                    ..WatchdogConfig::default()
                },
            ),
            (
                "maintenance_stall_after",
                WatchdogConfig {
                    maintenance_stall_after: Duration::ZERO,
                    ..WatchdogConfig::default()
                },
            ),
        ] {
            assert_eq!(
                config.validate(),
                Err(WatchdogConfigError::ZeroDuration(name)),
                "{name}"
            );
        }
    }
}
