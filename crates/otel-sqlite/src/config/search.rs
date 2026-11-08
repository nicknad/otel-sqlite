use anyhow::{Context, Result};
use otel_sqlite_storage::FtsMode;
use serde::Deserialize;

use super::Config;

/// `[search]` section: the installed log search-index mode.
#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct SearchSection {
    mode: Option<String>,
}

impl SearchSection {
    pub(crate) fn apply_to(self, config: &mut Config) -> Result<()> {
        if let Some(raw) = self.mode {
            config.search_mode = FtsMode::parse(&raw)
                .with_context(|| format!("unknown search.mode `{raw}` (expected off|body|full)"))?;
        }
        Ok(())
    }
}
