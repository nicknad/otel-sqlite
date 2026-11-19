mod auth;
mod dump;
mod durability;
mod duration;
mod file;
mod maintenance;
mod search;
mod tls;
mod validate;
mod watchdog;

#[cfg(test)]
mod tests;

pub(crate) use file::FileConfig;

use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result};
use otel_sqlite_ingress::{IngressConfig, expand_bind_address};
use otel_sqlite_runtime::{MaintenanceConfig, WatchdogConfig};
use otel_sqlite_storage::StorageConfig;

/// Environment variable holding the path of an optional TOML config file.
///
/// The documented lowercase spelling (`otel-sqlite-cfg-path`) is honoured as
/// a fallback so either casing works on case-sensitive platforms.
pub const CONFIG_PATH_ENV_VAR: &str = "OTEL_SQLITE_CFG_PATH";
const CONFIG_PATH_ENV_VAR_LOWER: &str = "otel-sqlite-cfg-path";

/// Depth of the bounded ingest queue between ingress handlers and the
/// storage insert batcher.
pub const DEFAULT_INGEST_QUEUE_CAPACITY: usize = 50_000;
/// Largest accepted `ingest_queue_capacity`. The queue is the admission
/// buffer for the whole pipeline; deeper queues hide backpressure longer and
/// pay memory per slot.
pub const MAX_INGEST_QUEUE_CAPACITY: usize = 1_000_000;
/// Ceiling the CLI applies uniformly to every mandatory duration knob, so an
/// operator cannot express an unusable schedule.
pub const MAX_TIMEOUT: Duration = Duration::from_secs(60 * 60);
/// Ceiling for retention-style knobs: ten years is longer than any
/// log-record retention window should claim.
pub const MAX_RETENTION: Duration = Duration::from_secs(10 * 365 * 24 * 60 * 60);

/// Effective, validated configuration for the whole binary.
///
/// Each section is stored as the owning crate's own config type —
/// [`IngressConfig`], [`StorageConfig`], [`MaintenanceConfig`] and
/// [`WatchdogConfig`] — so every default and per-section invariant lives
/// with the crate that consumes the values. The binary adds only what no
/// single crate can own: the pipeline-assembly knobs (`ingest_queue_capacity`
/// names a channel `main` creates between ingress and storage), the admin
/// metrics endpoint, and the cross-section gates in [`validate`].
#[derive(Debug)]
pub struct Config {
    /// Depth of the bounded channel between ingress handlers and the storage
    /// insert batcher. Owned here because `main` creates the channel and
    /// hands one end to each side.
    pub ingest_queue_capacity: usize,
    /// Admin address serving Prometheus `/metrics`. `None` disables the
    /// endpoint (TOML value `"off"`), and that is the default: the endpoint
    /// is opt-in and, when configured, should stay loopback-only.
    pub metrics_address: Option<String>,
    /// Explicit opt-in for exposing the unauthenticated plain HTTP metrics
    /// endpoint beyond loopback. A reverse proxy is preferred in production.
    pub allow_remote_metrics: bool,
    /// Explicit opt-in for binding a non-loopback `listen_address` without
    /// `[tls]`. Telemetry and bearer tokens then travel in cleartext to
    /// anyone on the path — prefer TLS (`otel-sqlite gen-certs`) and only
    /// set this on an isolated network you trust.
    pub allow_insecure_remote: bool,
    /// Ingress-owned section: bind address, wire and mapping limits,
    /// TLS/auth material, ack policy and the ingress drain budget. Defaults
    /// and invariants live on [`IngressConfig`].
    pub ingress: IngressConfig,
    /// Storage-owned section: database path, insert batching, command
    /// queue, group commit, search-index mode, size quota and the SQLite
    /// `synchronous` level. Defaults and invariants live on
    /// [`StorageConfig`].
    pub storage: StorageConfig,
    /// Periodic maintenance schedule (purge/checkpoint/optimize/vacuum),
    /// executed by the maintenance worker through the shared command queue.
    pub maintenance: MaintenanceConfig,
    /// Watchdog observation policy: evidence sampling, health thresholds and
    /// whether an unhealthy verdict halts ingestion for a clean restart.
    pub watchdog: WatchdogConfig,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            ingest_queue_capacity: DEFAULT_INGEST_QUEUE_CAPACITY,
            metrics_address: None,
            allow_remote_metrics: false,
            allow_insecure_remote: false,
            ingress: IngressConfig::default(),
            storage: StorageConfig::default(),
            maintenance: MaintenanceConfig::default(),
            watchdog: WatchdogConfig::default(),
        }
    }
}

impl Config {
    /// Builds the effective configuration from built-in defaults overlaid
    /// with the TOML file named by [`CONFIG_PATH_ENV_VAR`], when that
    /// variable is set to a non-empty path.
    ///
    /// Every key present in the file replaces the corresponding default;
    /// omitted keys keep their defaults, recursively through the
    /// `[maintenance]` and `[watchdog]` sections.
    pub fn from_env() -> Result<Self> {
        // `from_file` validates before returning; the defaults path must do
        // the same, so validation runs exactly once per path.
        if let Some(path) = Self::env_config_path() {
            Self::from_file(&path)
        } else {
            let config = Self::default();
            config.validate()?;
            Ok(config)
        }
    }

    /// Config file path from the environment, `None` when unset or empty.
    pub fn env_config_path() -> Option<PathBuf> {
        [CONFIG_PATH_ENV_VAR, CONFIG_PATH_ENV_VAR_LOWER]
            .into_iter()
            .find_map(|name| {
                std::env::var_os(name)
                    .filter(|value| !value.is_empty())
                    .map(PathBuf::from)
            })
    }

    /// Defaults overlaid with the settings in `path`.
    pub fn from_file(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read config file {}", path.display()))?;
        let mut config = Self::default();
        let overlay: FileConfig = toml::from_str(&text)
            .with_context(|| format!("failed to parse config file {}", path.display()))?;
        overlay
            .apply_to(&mut config)
            .with_context(|| format!("invalid settings in config file {}", path.display()))?;
        config.validate()?;
        Ok(config)
    }

    /// Validates the complete effective configuration before any listener or
    /// worker is started. Per-section invariants are delegated to the owning
    /// crates; the binary keeps only the cross-section gates. Delegates to
    /// [`validate`].
    pub fn validate(&self) -> Result<()> {
        validate::validate_config(self)
    }

    /// Machine-readable effective configuration for startup diagnostics.
    pub fn dump_json(&self) -> Result<String> {
        serde_json::to_string_pretty(&self.dump_value())
            .context("failed to serialize effective configuration")
    }

    /// Effective configuration as a JSON value. The keys and values are the
    /// operator-facing contract: each key keeps its historical name and
    /// reads from the owning crate's config.
    pub fn dump_value(&self) -> serde_json::Value {
        serde_json::json!({
            "listen_address": self.ingress.listen_address,
            "sqlite_path": self.storage.sqlite_path,
            "ingest_queue_capacity": self.ingest_queue_capacity,
            "command_queue_capacity": self.storage.command_queue_capacity,
            "batcher_max_batch_records": self.storage.insert_batcher.max_batch_records,
            "batcher_max_batch_age_ms": self.storage.insert_batcher.max_batch_age.as_millis(),
            "group_commit_max_records": self.storage.group_commit_max_records,
            "search_mode": self.storage.fts_mode.as_str(),
            "grpc_max_recv_msg_size": self.ingress.max_recv_msg_size,
            "grpc_max_concurrent_streams": self.ingress.max_concurrent_streams,
            "mapping_concurrency": self.ingress.mapping_concurrency,
            "max_records_per_request": self.ingress.max_records_per_request,
            "max_attributes_per_record": self.ingress.max_attributes_per_record,
            "max_attribute_key_bytes": self.ingress.max_attribute_key_bytes,
            "max_attribute_value_bytes": self.ingress.max_attribute_value_bytes,
            "max_body_bytes": self.ingress.max_body_bytes,
            "max_scope_metadata_expansion_bytes": self.ingress.max_scope_metadata_expansion_bytes,
            "max_db_bytes": self.storage.max_db_bytes,
            "shutdown_timeout_ms": self.ingress.shutdown_timeout.as_millis(),
            "durability_mode": self.ingress.durability_mode.as_str(),
            "sqlite_synchronous": self.storage.synchronous.as_str(),
            "metrics_address": self.metrics_address,
            "allow_remote_metrics": self.allow_remote_metrics,
            "allow_insecure_remote": self.allow_insecure_remote,
            "tls_configured": self.ingress.tls.is_some(),
            "auth_configured": self.ingress.auth.is_some(),
            "maintenance": dump::maintenance_dump(&self.maintenance),
            "watchdog": dump::watchdog_dump(&self.watchdog),
        })
    }

    /// Ingress-owned section, ready for the gRPC listener.
    pub fn ingress_config(&self) -> IngressConfig {
        self.ingress.clone()
    }

    /// Storage-owned section, ready for `Storage::open`.
    pub fn storage_config(&self) -> StorageConfig {
        // One graceful-shutdown budget shared by the ingress drain deadline
        // and the storage drain; keeps total shutdown time predictable (at
        // most twice this value). The budget lives on the ingress config;
        // storage takes its copy here.
        StorageConfig {
            shutdown_timeout: self.ingress.shutdown_timeout,
            ..self.storage.clone()
        }
    }
}

/// Human-facing URL for the Prometheus endpoint. Bare `":port"` and
/// wildcard binds are shown as loopback, which is where a local operator can
/// actually scrape them (the raw bind address is not a valid URL host).
pub fn metrics_display_url(address: &str) -> String {
    match expand_bind_address(address)
        .parse::<std::net::SocketAddr>()
        .ok()
        .filter(|addr| addr.ip().is_unspecified())
    {
        Some(addr) if addr.is_ipv4() => format!("http://127.0.0.1:{}/metrics", addr.port()),
        Some(addr) => format!("http://[::1]:{}/metrics", addr.port()),
        None => format!("http://{address}/metrics"),
    }
}

/// `true` when `listen_address` resolves to a loopback-only socket. Used for
/// the cleartext-exposure startup gate in [`Config::validate`].
pub fn is_loopback_listen(listen_address: &str) -> bool {
    expand_bind_address(listen_address)
        .parse::<std::net::SocketAddr>()
        .is_ok_and(|addr| addr.ip().is_loopback())
}
