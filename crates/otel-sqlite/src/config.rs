mod auth;
mod dump;
mod durability;
mod duration;
mod file;
mod maintenance;
mod tls;
mod validate;
mod watchdog;

#[cfg(test)]
mod tests;

pub(crate) use file::FileConfig;

use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result};
use otel_sqlite_core::storage::{DurabilityMode, InsertBatcherConfig, SyncMode};
use otel_sqlite_ingress::{
    AuthConfig, DEFAULT_LISTEN_ADDRESS, DEFAULT_MAX_ATTRIBUTE_KEY_BYTES,
    DEFAULT_MAX_ATTRIBUTE_VALUE_BYTES, DEFAULT_MAX_ATTRIBUTES_PER_RECORD, DEFAULT_MAX_BODY_BYTES,
    DEFAULT_MAX_CONCURRENT_STREAMS, DEFAULT_MAX_RECORDS_PER_REQUEST, DEFAULT_MAX_RECV_MSG_SIZE,
    DEFAULT_MAX_SCOPE_METADATA_EXPANSION_BYTES, DEFAULT_SHUTDOWN_TIMEOUT, IngressConfig,
    MAX_ATTRIBUTE_KEY_BYTES, MAX_ATTRIBUTE_VALUE_BYTES, MAX_ATTRIBUTES_PER_RECORD, MAX_BODY_BYTES,
    MAX_SCOPE_METADATA_EXPANSION_BYTES, TlsConfig, expand_bind_address,
};
use otel_sqlite_runtime::{MaintenanceConfig, WatchdogConfig};
use otel_sqlite_storage::StorageConfig;

/// Environment variable holding the path of an optional TOML config file.
///
/// The documented lowercase spelling (`otel-sqlite-cfg-path`) is honoured as
/// a fallback so either casing works on case-sensitive platforms.
pub const CONFIG_PATH_ENV_VAR: &str = "OTEL_SQLITE_CFG_PATH";
const CONFIG_PATH_ENV_VAR_LOWER: &str = "otel-sqlite-cfg-path";

pub const DEFAULT_INGEST_QUEUE_CAPACITY: usize = 50_000;
pub const MAX_INGEST_QUEUE_CAPACITY: usize = 1_000_000;
pub const MAX_GRPC_RECV_MSG_SIZE: usize = 256 * 1024 * 1024;
/// Upper bound accepted for `grpc_max_concurrent_streams` (M5). Streams are
/// the global inbound-parallelism multiplier, so values above this need an
/// explicit code change — not just a config edit — as justification that the
/// host has the memory headroom (`streams * grpc_max_recv_msg_size`).
pub const MAX_GRPC_CONCURRENT_STREAMS: u32 = 1_024;
pub const MAX_BATCH_RECORDS: usize = 100_000;
pub const MAX_RECORDS_PER_REQUEST: usize = 1_000_000;
/// Minimum accepted `max_db_bytes` quota: below 1 MiB the writer would evict
/// on nearly every batch (a fresh migrated database already exceeds it).
pub const MIN_DB_QUOTA_BYTES: u64 = 1024 * 1024;
pub const MAX_TIMEOUT: Duration = Duration::from_secs(60 * 60);
pub const MAX_RETENTION: Duration = Duration::from_secs(10 * 365 * 24 * 60 * 60);

#[derive(Debug)]
pub struct Config {
    pub listen_address: String,
    pub sqlite_path: PathBuf,
    /// Depth of the bounded channel between ingress handlers and the storage
    /// insert batcher.
    pub ingest_queue_capacity: usize,
    /// Depth of the bounded command queue between the insert batcher and the
    /// single SQLite writer.
    pub command_queue_capacity: usize,
    /// Storage insert-batch capacity (`max_batch_records`).
    pub batcher_max_batch_records: usize,
    /// Maximum age of the oldest buffered record before a partial batch is
    /// flushed (`max_batch_age`).
    pub batcher_max_batch_age: Duration,
    pub grpc_max_recv_msg_size: usize,
    pub grpc_max_concurrent_streams: u32,
    pub max_records_per_request: usize,
    /// Max attributes on one log record (H2). See ingress
    /// `DEFAULT_MAX_ATTRIBUTES_PER_RECORD`.
    pub max_attributes_per_record: usize,
    /// Max bytes for one attribute key (H2).
    pub max_attribute_key_bytes: usize,
    /// Max bytes for one attribute string/bytes value (H2).
    pub max_attribute_value_bytes: usize,
    /// Max bytes for a scalar log body (H2).
    pub max_body_bytes: usize,
    /// Aggregate scope-metadata expansion budget per request (C1): scope
    /// metadata bytes multiplied by member records, a conservative worst-case
    /// bound (mapped records share one scope allocation per group). See
    /// ingress `DEFAULT_MAX_SCOPE_METADATA_EXPANSION_BYTES`.
    pub max_scope_metadata_expansion_bytes: usize,
    /// Optional on-disk size quota for the SQLite database file, in bytes
    /// (`None` = unbounded). When set, the storage writer evicts the oldest
    /// rows before each insert batch once the file exceeds the quota.
    pub max_db_bytes: Option<u64>,
    pub shutdown_timeout: Duration,
    /// Periodic maintenance schedule (purge/checkpoint/optimize/vacuum),
    /// executed by the maintenance worker through the shared command queue.
    pub maintenance: MaintenanceConfig,
    /// Watchdog observation policy: evidence sampling, health thresholds and
    /// whether an unhealthy verdict halts ingestion for a clean restart.
    pub watchdog: WatchdogConfig,
    /// Ack policy for OTLP exports: `Commit` (default) releases each
    /// response only after the writer committed the accepted records;
    /// `Enqueue` acknowledges immediately at ingest-queue entry.
    pub durability_mode: DurabilityMode,
    /// SQLite `synchronous` level for the writer connection (`normal` keeps
    /// WAL checkpoint-time syncing; `full` fsyncs every commit).
    pub sqlite_synchronous: SyncMode,
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
    /// Server TLS material; `client_ca` present enables mandatory mTLS.
    pub tls: Option<TlsConfig>,
    /// Bearer-token auth; `Some` when `[auth] mode = "token"`.
    pub auth: Option<AuthConfig>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            listen_address: DEFAULT_LISTEN_ADDRESS.to_owned(),
            sqlite_path: PathBuf::from("otel-logs.db"),
            ingest_queue_capacity: DEFAULT_INGEST_QUEUE_CAPACITY,
            command_queue_capacity: otel_sqlite_storage::DEFAULT_COMMAND_QUEUE_CAPACITY,
            batcher_max_batch_records: otel_sqlite_core::storage::DEFAULT_MAX_INSERT_BATCH_RECORDS,
            batcher_max_batch_age: otel_sqlite_core::storage::DEFAULT_MAX_INSERT_BATCH_AGE,
            grpc_max_recv_msg_size: DEFAULT_MAX_RECV_MSG_SIZE,
            grpc_max_concurrent_streams: DEFAULT_MAX_CONCURRENT_STREAMS,
            max_records_per_request: DEFAULT_MAX_RECORDS_PER_REQUEST,
            max_attributes_per_record: DEFAULT_MAX_ATTRIBUTES_PER_RECORD,
            max_attribute_key_bytes: DEFAULT_MAX_ATTRIBUTE_KEY_BYTES,
            max_attribute_value_bytes: DEFAULT_MAX_ATTRIBUTE_VALUE_BYTES,
            max_body_bytes: DEFAULT_MAX_BODY_BYTES,
            max_scope_metadata_expansion_bytes: DEFAULT_MAX_SCOPE_METADATA_EXPANSION_BYTES,
            max_db_bytes: None,
            shutdown_timeout: DEFAULT_SHUTDOWN_TIMEOUT,
            maintenance: MaintenanceConfig::default(),
            watchdog: WatchdogConfig::default(),
            durability_mode: DurabilityMode::default(),
            sqlite_synchronous: SyncMode::default(),
            metrics_address: None,
            allow_remote_metrics: false,
            allow_insecure_remote: false,
            tls: None,
            auth: None,
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
    /// worker is started. Delegates per-section to [`validate`].
    pub fn validate(&self) -> Result<()> {
        validate::validate_config(self)
    }

    /// Machine-readable effective configuration for startup diagnostics.
    pub fn dump_json(&self) -> Result<String> {
        serde_json::to_string_pretty(&serde_json::json!({
            "listen_address": self.listen_address,
            "sqlite_path": self.sqlite_path,
            "ingest_queue_capacity": self.ingest_queue_capacity,
            "command_queue_capacity": self.command_queue_capacity,
            "batcher_max_batch_records": self.batcher_max_batch_records,
            "batcher_max_batch_age_ms": self.batcher_max_batch_age.as_millis(),
            "grpc_max_recv_msg_size": self.grpc_max_recv_msg_size,
            "grpc_max_concurrent_streams": self.grpc_max_concurrent_streams,
            "max_records_per_request": self.max_records_per_request,
            "max_attributes_per_record": self.max_attributes_per_record,
            "max_attribute_key_bytes": self.max_attribute_key_bytes,
            "max_attribute_value_bytes": self.max_attribute_value_bytes,
            "max_body_bytes": self.max_body_bytes,
            "max_scope_metadata_expansion_bytes": self.max_scope_metadata_expansion_bytes,
            "max_db_bytes": self.max_db_bytes,
            "shutdown_timeout_ms": self.shutdown_timeout.as_millis(),
            "durability_mode": format!("{:?}", self.durability_mode),
            "sqlite_synchronous": format!("{:?}", self.sqlite_synchronous),
            "metrics_address": self.metrics_address,
            "allow_remote_metrics": self.allow_remote_metrics,
            "allow_insecure_remote": self.allow_insecure_remote,
            "tls_configured": self.tls.is_some(),
            "auth_configured": self.auth.is_some(),
            "maintenance": dump::maintenance_dump(&self.maintenance),
            "watchdog": dump::watchdog_dump(&self.watchdog),
        }))
        .context("failed to serialize effective configuration")
    }

    pub fn ingress_config(&self) -> IngressConfig {
        IngressConfig {
            listen_address: self.listen_address.clone(),
            max_recv_msg_size: self.grpc_max_recv_msg_size,
            max_concurrent_streams: self.grpc_max_concurrent_streams,
            shutdown_timeout: self.shutdown_timeout,
            max_records_per_request: self.max_records_per_request,
            max_attributes_per_record: self.max_attributes_per_record,
            max_attribute_key_bytes: self.max_attribute_key_bytes,
            max_attribute_value_bytes: self.max_attribute_value_bytes,
            max_body_bytes: self.max_body_bytes,
            max_scope_metadata_expansion_bytes: self.max_scope_metadata_expansion_bytes,
            durability_mode: self.durability_mode,
            tls: self.tls.clone(),
            auth: self.auth.clone(),
        }
    }

    pub fn storage_config(&self) -> StorageConfig {
        StorageConfig {
            sqlite_path: self.sqlite_path.clone(),
            insert_batcher: InsertBatcherConfig::new(
                self.batcher_max_batch_records,
                self.batcher_max_batch_age,
            ),
            command_queue_capacity: self.command_queue_capacity,
            max_db_bytes: self.max_db_bytes,
            synchronous: self.sqlite_synchronous,
            startup_timeout: otel_sqlite_storage::DEFAULT_STARTUP_TIMEOUT,
            // One graceful-shutdown budget shared by the ingress drain
            // deadline and the storage drain; keeps total shutdown time
            // predictable (at most twice this value).
            shutdown_timeout: self.shutdown_timeout,
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
