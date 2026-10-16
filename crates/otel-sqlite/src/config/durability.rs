use anyhow::Result;
use otel_sqlite_core::storage::{DurabilityMode, SyncMode};
use serde::Deserialize;

use crate::config::Config;

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct DurabilitySection {
    mode: Option<String>,
    synchronous: Option<String>,
}

impl DurabilitySection {
    pub(crate) fn apply_to(self, config: &mut Config) -> Result<()> {
        if let Some(mode) = self.mode {
            config.durability_mode = DurabilityMode::parse(&mode).ok_or_else(|| {
                anyhow::anyhow!("unknown durability.mode `{mode}` (expected commit|enqueue)")
            })?;
        }
        if let Some(sync) = self.synchronous {
            config.sqlite_synchronous = SyncMode::parse(&sync).ok_or_else(|| {
                anyhow::anyhow!("unknown durability.synchronous `{sync}` (expected normal|full)")
            })?;
        }
        Ok(())
    }
}
