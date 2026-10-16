use anyhow::Result;
use otel_sqlite_runtime::WatchdogConfig;
use serde::Deserialize;

use super::duration::{RawDuration, apply_required_duration};

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct WatchdogSection {
    tick_interval: Option<RawDuration>,
    degraded_after: Option<RawDuration>,
    unhealthy_after: Option<RawDuration>,
    ack_stall_after: Option<RawDuration>,
    maintenance_stall_after: Option<RawDuration>,
    queue_pressure_ratio: Option<f32>,
    halt_on_unhealthy: Option<bool>,
}

impl WatchdogSection {
    pub(crate) fn apply_to(self, config: &mut WatchdogConfig) -> Result<()> {
        apply_required_duration(
            &mut config.tick_interval,
            self.tick_interval,
            "watchdog.tick_interval",
        )?;
        apply_required_duration(
            &mut config.degraded_after,
            self.degraded_after,
            "watchdog.degraded_after",
        )?;
        apply_required_duration(
            &mut config.unhealthy_after,
            self.unhealthy_after,
            "watchdog.unhealthy_after",
        )?;
        apply_required_duration(
            &mut config.ack_stall_after,
            self.ack_stall_after,
            "watchdog.ack_stall_after",
        )?;
        apply_required_duration(
            &mut config.maintenance_stall_after,
            self.maintenance_stall_after,
            "watchdog.maintenance_stall_after",
        )?;
        if let Some(ratio) = self.queue_pressure_ratio {
            config.queue_pressure_ratio = ratio;
        }
        if let Some(halt) = self.halt_on_unhealthy {
            config.halt_on_unhealthy = halt;
        }
        Ok(())
    }
}
