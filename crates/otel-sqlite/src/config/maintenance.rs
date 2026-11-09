use std::time::Duration;

use anyhow::{Result, bail};
use otel_sqlite_core::storage::CheckpointMode;
use otel_sqlite_runtime::{MaintenanceConfig, PurgeSchedule};
use serde::Deserialize;

use super::duration::{RawDuration, apply_optional_duration, apply_required_duration};

/// Cadence used when `retention` enables purging but `purge_interval` is not
/// set explicitly. Documented as the default purge cadence.
const DEFAULT_PURGE_INTERVAL: Duration = Duration::from_secs(15 * 60);

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct MaintenanceSection {
    retention: Option<toml::Value>,
    purge_interval: Option<RawDuration>,
    checkpoint_interval: Option<RawDuration>,
    checkpoint_mode: Option<String>,
    optimize_interval: Option<RawDuration>,
    vacuum_interval: Option<RawDuration>,
    rebuild_fts_interval: Option<RawDuration>,
    retry_delay: Option<RawDuration>,
}

impl MaintenanceSection {
    pub(crate) fn apply_to(self, config: &mut MaintenanceConfig) -> Result<()> {
        // A purge schedule exists only when retention is enabled: the
        // `purge_interval` key is meaningless without a retention window.
        let retention = resolve_retention(self.retention)?;
        let interval = match self.purge_interval {
            Some(RawDuration::Enabled(interval)) => Some(interval),
            Some(RawDuration::Disabled) => None,
            // Retention enabled without an explicit cadence uses the
            // documented default.
            None => Some(DEFAULT_PURGE_INTERVAL),
        };
        config.purge = match (retention, interval) {
            (Some(retention), Some(interval)) => Some(PurgeSchedule {
                retention,
                interval,
            }),
            _ => None,
        };
        apply_optional_duration(&mut config.checkpoint_interval, self.checkpoint_interval);
        apply_optional_duration(&mut config.optimize_interval, self.optimize_interval);
        apply_optional_duration(&mut config.vacuum_interval, self.vacuum_interval);
        apply_optional_duration(&mut config.rebuild_fts_interval, self.rebuild_fts_interval);
        apply_required_duration(
            &mut config.retry_delay,
            self.retry_delay,
            "maintenance.retry_delay",
        )?;
        if let Some(mode) = self.checkpoint_mode {
            config.checkpoint_mode = CheckpointMode::parse(&mode).ok_or_else(|| {
                anyhow::anyhow!(
                    "unknown maintenance.checkpoint_mode `{mode}` \
                     (expected passive|full|restart|truncate)"
                )
            })?;
        }
        Ok(())
    }
}

/// Retention window as written in the config file.
///
/// One uniform window covers log events:
///
/// ```toml
/// [maintenance]
/// retention = "7d"
/// ```
///
/// Alternatively, the table form names the signal explicitly:
///
/// ```toml
/// [maintenance.retention]
/// logs = "7d"
/// ```
///
/// The raw TOML value is resolved in [`resolve_retention`] so an unknown key
/// in the table form (`metris = "24h"`) produces a named error instead of an
/// untagged-enum type mismatch. The retired `metrics` key is rejected with an
/// explicit explanation.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct RetentionSection {
    logs: Option<RawDuration>,
    /// Removed together with the OTLP metrics signal; only accepted here to
    /// emit a targeted error instead of a generic unknown-field message.
    metrics: Option<toml::Value>,
}

pub(crate) fn resolve_retention(raw: Option<toml::Value>) -> Result<Option<Duration>> {
    let Some(raw) = raw else { return Ok(None) };
    if matches!(raw, toml::Value::Table(_)) {
        let retention: RetentionSection = raw.try_into().map_err(|error| {
            anyhow::anyhow!("invalid [maintenance.retention] settings: {error}")
        })?;
        if retention.metrics.is_some() {
            bail!(
                "maintenance.retention.metrics is no longer supported: the OTLP metrics \
                 signal was removed; use `logs` (or a single uniform window) instead"
            );
        }
        return Ok(match retention.logs {
            Some(RawDuration::Enabled(window)) => Some(window),
            Some(RawDuration::Disabled) | None => None,
        });
    }
    let window = RawDuration::deserialize(raw)
        .map_err(|error| anyhow::anyhow!("invalid retention window: {error}"))?;
    Ok(match window {
        RawDuration::Enabled(window) => Some(window),
        // Explicit opt-out ("off") disables purging.
        RawDuration::Disabled => None,
    })
}
