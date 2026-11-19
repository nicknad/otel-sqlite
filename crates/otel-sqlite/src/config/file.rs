use std::path::PathBuf;

use anyhow::{Context, Result};
use otel_sqlite_ingress::expand_bind_address;
use serde::Deserialize;

use super::Config;
use super::auth::AuthSection;
use super::durability::DurabilitySection;
use super::duration::{RawDuration, apply_required_duration};
use super::maintenance::MaintenanceSection;
use super::search::SearchSection;
use super::tls::TlsSection;
use super::watchdog::WatchdogSection;

/// The operator's TOML contract: every key the file accepts, with its exact
/// spelling, in one table (`deny_unknown_fields` keeps typos fatal). Keys are
/// grouped by the crate that owns the effective value; `apply_to` hands each
/// group to that crate's config in one contiguous block, so no binary type
/// ever restates a section's fields.
#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct FileConfig {
    // Keys owned by the ingress crate's config.
    listen_address: Option<String>,
    grpc_max_recv_msg_size: Option<usize>,
    grpc_max_concurrent_streams: Option<u32>,
    mapping_concurrency: Option<usize>,
    max_records_per_request: Option<usize>,
    max_attributes_per_record: Option<usize>,
    max_attribute_key_bytes: Option<usize>,
    max_attribute_value_bytes: Option<usize>,
    max_body_bytes: Option<usize>,
    max_scope_metadata_expansion_bytes: Option<usize>,
    shutdown_timeout: Option<RawDuration>,
    tls: Option<TlsSection>,
    auth: Option<AuthSection>,
    // Keys owned by the storage crate's config.
    sqlite_path: Option<PathBuf>,
    command_queue_capacity: Option<usize>,
    batcher_max_batch_records: Option<usize>,
    batcher_max_batch_age: Option<RawDuration>,
    group_commit_max_records: Option<usize>,
    max_db_bytes: Option<u64>,
    search: Option<SearchSection>,
    // Keys the binary itself owns.
    ingest_queue_capacity: Option<usize>,
    metrics_address: Option<String>,
    allow_remote_metrics: Option<bool>,
    allow_insecure_remote: Option<bool>,
    // Runtime-owned sections.
    maintenance: Option<MaintenanceSection>,
    watchdog: Option<WatchdogSection>,
    // Cross-crate section: ack policy (ingress) and SQLite sync (storage).
    durability: Option<DurabilitySection>,
}

impl FileConfig {
    pub(crate) fn apply_to(self, config: &mut Config) -> Result<()> {
        // Ingress-owned keys write straight into the ingress crate's config.
        if let Some(value) = self.listen_address {
            config.ingress.listen_address = value;
        }
        if let Some(value) = self.grpc_max_recv_msg_size {
            config.ingress.max_recv_msg_size = value;
        }
        if let Some(value) = self.grpc_max_concurrent_streams {
            config.ingress.max_concurrent_streams = value;
        }
        if let Some(value) = self.mapping_concurrency {
            config.ingress.mapping_concurrency = value;
        }
        if let Some(value) = self.max_records_per_request {
            config.ingress.max_records_per_request = value;
        }
        if let Some(value) = self.max_attributes_per_record {
            config.ingress.max_attributes_per_record = value;
        }
        if let Some(value) = self.max_attribute_key_bytes {
            config.ingress.max_attribute_key_bytes = value;
        }
        if let Some(value) = self.max_attribute_value_bytes {
            config.ingress.max_attribute_value_bytes = value;
        }
        if let Some(value) = self.max_body_bytes {
            config.ingress.max_body_bytes = value;
        }
        if let Some(value) = self.max_scope_metadata_expansion_bytes {
            config.ingress.max_scope_metadata_expansion_bytes = value;
        }
        apply_required_duration(
            &mut config.ingress.shutdown_timeout,
            self.shutdown_timeout,
            "shutdown_timeout",
        )?;
        if let Some(section) = self.tls {
            config.ingress.tls = Some(section.apply_to()?);
        }
        if let Some(section) = self.auth {
            config.ingress.auth = section.apply_to()?;
        }

        // Storage-owned keys write straight into the storage crate's config.
        if let Some(value) = self.sqlite_path {
            config.storage.sqlite_path = value;
        }
        if let Some(value) = self.command_queue_capacity {
            config.storage.command_queue_capacity = value;
        }
        if let Some(value) = self.batcher_max_batch_records {
            config.storage.insert_batcher.max_batch_records = value;
        }
        apply_required_duration(
            &mut config.storage.insert_batcher.max_batch_age,
            self.batcher_max_batch_age,
            "batcher_max_batch_age",
        )?;
        if let Some(value) = self.group_commit_max_records {
            config.storage.group_commit_max_records = value;
        }
        if let Some(value) = self.max_db_bytes {
            config.storage.max_db_bytes = Some(value);
        }
        if let Some(section) = self.search {
            section.apply_to(config)?;
        }

        // Binary-owned assembly and admin knobs.
        if let Some(value) = self.ingest_queue_capacity {
            config.ingest_queue_capacity = value;
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

        // Runtime-owned sections and the cross-crate [durability] overlay.
        if let Some(section) = self.maintenance {
            section.apply_to(&mut config.maintenance)?;
        }
        if let Some(section) = self.watchdog {
            section.apply_to(&mut config.watchdog)?;
        }
        if let Some(section) = self.durability {
            section.apply_to(config)?;
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
