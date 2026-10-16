use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use otel_sqlite_ingress::AuthConfig;
use serde::Deserialize;

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct AuthSection {
    mode: Option<String>,
    token_file: Option<String>,
}

impl AuthSection {
    pub(crate) fn apply_to(self) -> Result<Option<AuthConfig>> {
        let mode = self.mode.as_deref().map_or("none", str::trim);
        if mode.eq_ignore_ascii_case("none") {
            if self.token_file.is_some() {
                bail!("[auth] token_file is set but mode is \"none\"; remove one of them")
            }
            Ok(None)
        } else if mode.eq_ignore_ascii_case("token") {
            let file = self.token_file.as_deref().with_context(
                || "[auth] mode = \"token\" requires `token_file` (one bearer token per line)",
            )?;
            Ok(Some(AuthConfig {
                token_file: PathBuf::from(file),
            }))
        } else {
            bail!("unknown [auth] mode `{mode}` (expected none|token)")
        }
    }
}
