use std::time::Duration;

use otel_sqlite_runtime::{MaintenanceConfig, WatchdogConfig};

/// Effective maintenance schedule as a JSON object (durations in
/// milliseconds), used by the JSON startup dump.
pub(crate) fn maintenance_dump(config: &MaintenanceConfig) -> serde_json::Value {
    let ms = |duration: &Option<Duration>| duration.map(|value| value.as_millis());
    serde_json::json!({
        "retention_ms": ms(&config.retention),
        "purge_interval_ms": ms(&config.purge_interval),
        "checkpoint_interval_ms": ms(&config.checkpoint_interval),
        "checkpoint_mode": format!("{:?}", config.checkpoint_mode),
        "optimize_interval_ms": ms(&config.optimize_interval),
        "vacuum_interval_ms": ms(&config.vacuum_interval),
        "rebuild_fts_interval_ms": ms(&config.rebuild_fts_interval),
        "retry_delay_ms": config.retry_delay.as_millis(),
    })
}

/// Effective watchdog policy as a JSON object (durations in milliseconds).
pub(crate) fn watchdog_dump(config: &WatchdogConfig) -> serde_json::Value {
    serde_json::json!({
        "tick_interval_ms": config.tick_interval.as_millis(),
        "degraded_after_ms": config.degraded_after.as_millis(),
        "unhealthy_after_ms": config.unhealthy_after.as_millis(),
        "ack_stall_after_ms": config.ack_stall_after.as_millis(),
        "maintenance_stall_after_ms": config.maintenance_stall_after.as_millis(),
        "queue_pressure_ratio": config.queue_pressure_ratio,
        "halt_on_unhealthy": config.halt_on_unhealthy,
    })
}
