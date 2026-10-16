use anyhow::{Result, bail};
use otel_sqlite_core::storage::CheckpointMode;
use otel_sqlite_runtime::MaintenanceConfig;
use serde::Deserialize;

use super::duration::{RawDuration, apply_optional_duration, apply_required_duration};

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
        apply_retention(config, self.retention)?;
        apply_optional_duration(&mut config.purge_interval, self.purge_interval);
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
            config.checkpoint_mode = parse_checkpoint_mode(&mode)?;
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
/// The raw TOML value is resolved in [`apply_retention`] so an unknown key in
/// the table form (`metris = "24h"`) produces a named error instead of an
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

pub(crate) fn apply_retention(
    config: &mut MaintenanceConfig,
    raw: Option<toml::Value>,
) -> Result<()> {
    let Some(raw) = raw else { return Ok(()) };
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
        apply_optional_duration(&mut config.retention, retention.logs);
        return Ok(());
    }
    let window = RawDuration::deserialize(raw)
        .map_err(|error| anyhow::anyhow!("invalid retention window: {error}"))?;
    config.retention = match window {
        RawDuration::Enabled(window) => Some(window),
        // Explicit opt-out ("off") disables purging.
        RawDuration::Disabled => None,
    };
    Ok(())
}

pub(crate) fn parse_checkpoint_mode(text: &str) -> Result<CheckpointMode> {
    let trimmed = text.trim();
    if trimmed.eq_ignore_ascii_case("passive") {
        Ok(CheckpointMode::Passive)
    } else if trimmed.eq_ignore_ascii_case("full") {
        Ok(CheckpointMode::Full)
    } else if trimmed.eq_ignore_ascii_case("restart") {
        Ok(CheckpointMode::Restart)
    } else if trimmed.eq_ignore_ascii_case("truncate") {
        Ok(CheckpointMode::Truncate)
    } else {
        bail!("unknown checkpoint_mode `{trimmed}` (expected passive|full|restart|truncate)")
    }
}
