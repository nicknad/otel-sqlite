use std::path::PathBuf;

use anyhow::{Context, Result};
use otel_sqlite_ingress::TlsConfig;
use serde::Deserialize;

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct TlsSection {
    cert: Option<String>,
    key: Option<String>,
    client_ca: Option<String>,
}

impl TlsSection {
    pub(crate) fn apply_to(self) -> Result<TlsConfig> {
        let cert = self.cert.as_deref().with_context(
            || "[tls] requires `cert` (path to the PEM-encoded server certificate)",
        )?;
        let key = self
            .key
            .as_deref()
            .with_context(|| "[tls] requires `key` (path to the PEM-encoded private key)")?;
        Ok(TlsConfig {
            cert: PathBuf::from(cert),
            key: PathBuf::from(key),
            client_ca: self.client_ca.map(PathBuf::from),
        })
    }
}
