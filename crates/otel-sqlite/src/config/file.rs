use std::path::PathBuf;

use anyhow::{Context, Result};
use otel_sqlite_ingress::expand_bind_address;
use serde::Deserialize;

use super::Config;
use super::auth::AuthSection;
use super::durability::DurabilitySection;
use super::duration::{RawDuration, apply_required_duration};
use super::maintenance::MaintenanceSection;
use super::tls::TlsSection;
use super::watchdog::WatchdogSection;

/// Mirror of [`Config`] where every field is optional; `None` means "keep
/// the built-in default".
#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct FileConfig {
    listen_address: Option<String>,
    sqlite_path: Option<PathBuf>,
    ingest_queue_capacity: Option<usize>,
    command_queue_capacity: Option<usize>,
    batcher_max_batch_records: Option<usize>,
    batcher_max_batch_age: Option<RawDuration>,
    grpc_max_recv_msg_size: Option<usize>,
    grpc_max_concurrent_streams: Option<u32>,
    max_records_per_request: Option<usize>,
    max_attributes_per_record: Option<usize>,
    max_attribute_key_bytes: Option<usize>,
    max_attribute_value_bytes: Option<usize>,
    max_body_bytes: Option<usize>,
    max_scope_metadata_expansion_bytes: Option<usize>,
    max_db_bytes: Option<u64>,
    shutdown_timeout: Option<RawDuration>,
    maintenance: Option<MaintenanceSection>,
    watchdog: Option<WatchdogSection>,
    durability: Option<DurabilitySection>,
    metrics_address: Option<String>,
    allow_remote_metrics: Option<bool>,
    allow_insecure_remote: Option<bool>,
    tls: Option<TlsSection>,
    auth: Option<AuthSection>,
}

impl FileConfig {
    pub(crate) fn apply_to(self, config: &mut Config) -> Result<()> {
        if let Some(value) = self.listen_address {
            config.listen_address = value;
        }
        if let Some(value) = self.sqlite_path {
            config.sqlite_path = value;
        }
        if let Some(value) = self.ingest_queue_capacity {
            config.ingest_queue_capacity = value;
        }
        if let Some(value) = self.command_queue_capacity {
            config.command_queue_capacity = value;
        }
        if let Some(value) = self.batcher_max_batch_records {
            config.batcher_max_batch_records = value;
        }
        apply_required_duration(
            &mut config.batcher_max_batch_age,
            self.batcher_max_batch_age,
            "batcher_max_batch_age",
        )?;
        if let Some(value) = self.grpc_max_recv_msg_size {
            config.grpc_max_recv_msg_size = value;
        }
        if let Some(value) = self.grpc_max_concurrent_streams {
            config.grpc_max_concurrent_streams = value;
        }
        if let Some(value) = self.max_records_per_request {
            config.max_records_per_request = value;
        }
        if let Some(value) = self.max_attributes_per_record {
            config.max_attributes_per_record = value;
        }
        if let Some(value) = self.max_attribute_key_bytes {
            config.max_attribute_key_bytes = value;
        }
        if let Some(value) = self.max_attribute_value_bytes {
            config.max_attribute_value_bytes = value;
        }
        if let Some(value) = self.max_body_bytes {
            config.max_body_bytes = value;
        }
        if let Some(value) = self.max_scope_metadata_expansion_bytes {
            config.max_scope_metadata_expansion_bytes = value;
        }
        if let Some(value) = self.max_db_bytes {
            config.max_db_bytes = Some(value);
        }
        apply_required_duration(
            &mut config.shutdown_timeout,
            self.shutdown_timeout,
            "shutdown_timeout",
        )?;

        if let Some(section) = self.maintenance {
            section.apply_to(&mut config.maintenance)?;
        }
        if let Some(section) = self.watchdog {
            section.apply_to(&mut config.watchdog)?;
        }
        if let Some(section) = self.durability {
            section.apply_to(config)?;
        }
        if let Some(value) = self.metrics_address {
            config.metrics_address = parse_optional_address(&value, "metrics_address")?;
        }
        if let Some(value) = self.allow_remote_metrics {
            config.allow_remote_metrics = value;
        }
        if let Some(value) = self.allow_insecure_remote {
            config.allow_insecure_remote = value;
        }
        if let Some(section) = self.tls {
            config.tls = Some(section.apply_to()?);
        }
        if let Some(section) = self.auth {
            config.auth = section.apply_to()?;
        }
        Ok(())
    }
}

/// Parses an optional listen address; `"off"`/empty disable the endpoint.
/// Bare `":port"` binds all interfaces, mirroring `listen_address` handling.
pub(crate) fn parse_optional_address(raw: &str, key: &str) -> Result<Option<String>> {
    let trimmed = raw.trim();
    if trimmed.is_empty()
        || trimmed.eq_ignore_ascii_case("off")
        || trimmed.eq_ignore_ascii_case("none")
        || trimmed.eq_ignore_ascii_case("disabled")
    {
        return Ok(None);
    }
    expand_bind_address(trimmed)
        .parse::<std::net::SocketAddr>()
        .map(|_| Some(trimmed.to_owned()))
        .with_context(|| format!("invalid `{key}` `{raw}` (expected host:port or \"off\")"))
}
