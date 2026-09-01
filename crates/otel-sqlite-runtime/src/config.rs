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

#[derive(Debug, Clone)]
pub struct MaintenanceConfig {
    /// Retention window for log events. Older rows are deleted by the writer
    /// when it executes a prune command; the worker never touches data.
    ///
    /// `None` disables log purging entirely.
    pub retention: Option<Duration>,
    /// Retention window for metric data points; pruned on the same schedule
    /// as logs, with orphaned series/metrics/scopes/resources collected in
    /// the same transaction.
    ///
    /// `None` disables metric purging entirely (metrics then grow unbounded).
    pub metric_retention: Option<Duration>,
    /// How often a retention purge command is enqueued (for both signals).
    /// Ignored while both retention windows are `None`.
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
            metric_retention: None,
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
            queue_pressure_ratio: 0.75,
            halt_on_unhealthy: true,
        }
    }
}
