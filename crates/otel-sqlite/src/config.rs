use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use otel_sqlite_core::storage::{CheckpointMode, DurabilityMode, InsertBatcherConfig, SyncMode};
use otel_sqlite_ingress::{
    AuthConfig, DEFAULT_LISTEN_ADDRESS, DEFAULT_MAX_ATTRIBUTE_KEY_BYTES,
    DEFAULT_MAX_ATTRIBUTE_VALUE_BYTES, DEFAULT_MAX_ATTRIBUTES_PER_RECORD, DEFAULT_MAX_BODY_BYTES,
    DEFAULT_MAX_BUCKETS_PER_POINT, DEFAULT_MAX_CONCURRENT_STREAMS, DEFAULT_MAX_EXEMPLARS_PER_POINT,
    DEFAULT_MAX_RECORDS_PER_REQUEST, DEFAULT_MAX_RECV_MSG_SIZE, DEFAULT_SHUTDOWN_TIMEOUT,
    IngressConfig, MAX_ATTRIBUTE_KEY_BYTES, MAX_ATTRIBUTE_VALUE_BYTES, MAX_ATTRIBUTES_PER_RECORD,
    MAX_BODY_BYTES, MAX_BUCKETS_PER_POINT, MAX_EXEMPLARS_PER_POINT, TlsConfig,
};
use otel_sqlite_runtime::{MaintenanceConfig, WatchdogConfig};
use otel_sqlite_storage::StorageConfig;
use serde::Deserialize;

/// Environment variable holding the path of an optional TOML config file.
///
/// The documented lowercase spelling (`otel-sqlite-cfg-path`) is honoured as
/// a fallback so either casing works on case-sensitive platforms.
pub const CONFIG_PATH_ENV_VAR: &str = "OTEL_SQLITE_CFG_PATH";
const CONFIG_PATH_ENV_VAR_LOWER: &str = "otel-sqlite-cfg-path";

/// Default admin address serving Prometheus `/metrics`.
pub const DEFAULT_METRICS_ADDRESS: &str = "127.0.0.1:8888";
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

#[derive(Debug, Clone)]
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
    /// Max attributes on one log record / metric point (H2). See ingress
    /// `DEFAULT_MAX_ATTRIBUTES_PER_RECORD`.
    pub max_attributes_per_record: usize,
    /// Max bytes for one attribute key (H2).
    pub max_attribute_key_bytes: usize,
    /// Max bytes for one attribute string/bytes value (H2).
    pub max_attribute_value_bytes: usize,
    /// Max bytes for a scalar log body (H2).
    pub max_body_bytes: usize,
    /// Max bucket/explicit-bound/quantile entries on one histogram point (H2).
    pub max_buckets_per_point: usize,
    /// Max exemplars on one metric point (H2).
    pub max_exemplars_per_point: usize,
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
    /// endpoint (TOML value `"off"`). Defaults to localhost-only.
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
            max_buckets_per_point: DEFAULT_MAX_BUCKETS_PER_POINT,
            max_exemplars_per_point: DEFAULT_MAX_EXEMPLARS_PER_POINT,
            max_db_bytes: None,
            shutdown_timeout: DEFAULT_SHUTDOWN_TIMEOUT,
            maintenance: MaintenanceConfig::default(),
            watchdog: WatchdogConfig::default(),
            durability_mode: DurabilityMode::default(),
            sqlite_synchronous: SyncMode::default(),
            metrics_address: Some(DEFAULT_METRICS_ADDRESS.to_owned()),
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
        let result = match Self::env_config_path() {
            Some(path) => Self::from_file(&path),
            None => Ok(Self::default()),
        }?;
        result.validate()?;
        Ok(result)
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
    /// worker is started.
    pub fn validate(&self) -> Result<()> {
        let listen = expand_bind_address(&self.listen_address)
            .parse::<std::net::SocketAddr>()
            .with_context(|| format!("invalid listen_address `{}`", self.listen_address))?;
        if listen.port() == 0 {
            bail!("listen_address must use a non-zero port");
        }
        // Fail closed on cleartext exposure: a non-loopback bind without TLS
        // lets anyone on the path read telemetry and bearer tokens (and
        // inject their own). Bearer auth alone does not satisfy this — the
        // token itself would travel in cleartext — so only a TLS identity or
        // the explicit isolated-network opt-in gets past here.
        if !is_loopback_listen(&self.listen_address) && self.tls.is_none() {
            if !self.allow_insecure_remote {
                bail!(
                    "listen_address `{}` is non-loopback but no [tls] is configured; \
                     telemetry and bearer tokens would travel in cleartext. Configure [tls] \
                     (see `otel-sqlite gen-certs`) or set allow_insecure_remote = true only \
                     for an isolated network you trust",
                    self.listen_address
                );
            }
            tracing::warn!(
                listen_address = self.listen_address.as_str(),
                "ingress binds a non-loopback address without TLS; traffic is cleartext",
            );
        }
        validate_range(
            "ingest_queue_capacity",
            self.ingest_queue_capacity,
            1,
            MAX_INGEST_QUEUE_CAPACITY,
        )?;
        validate_range(
            "command_queue_capacity",
            self.command_queue_capacity,
            1,
            MAX_INGEST_QUEUE_CAPACITY,
        )?;
        validate_range(
            "batcher_max_batch_records",
            self.batcher_max_batch_records,
            1,
            MAX_BATCH_RECORDS,
        )?;
        validate_range(
            "grpc_max_recv_msg_size",
            self.grpc_max_recv_msg_size,
            1024,
            MAX_GRPC_RECV_MSG_SIZE,
        )?;
        validate_range(
            "grpc_max_concurrent_streams",
            self.grpc_max_concurrent_streams,
            1,
            MAX_GRPC_CONCURRENT_STREAMS,
        )?;
        // Very large wire budgets multiply by the stream count into gigabytes
        // of worst-case inbound buffering; make sure operators raising them
        // past these marks see the cost in the logs (startup only).
        if self.grpc_max_recv_msg_size > 64 * 1024 * 1024 {
            tracing::warn!(
                grpc_max_recv_msg_size = self.grpc_max_recv_msg_size,
                grpc_max_concurrent_streams = self.grpc_max_concurrent_streams,
                "large grpc_max_recv_msg_size multiplies by the stream count into GiB-scale inbound buffering; raise host memory accordingly",
            );
        }
        validate_range(
            "max_records_per_request",
            self.max_records_per_request,
            1,
            MAX_RECORDS_PER_REQUEST,
        )?;
        validate_range(
            "max_attributes_per_record",
            self.max_attributes_per_record,
            1,
            MAX_ATTRIBUTES_PER_RECORD,
        )?;
        validate_range(
            "max_attribute_key_bytes",
            self.max_attribute_key_bytes,
            1,
            MAX_ATTRIBUTE_KEY_BYTES,
        )?;
        validate_range(
            "max_attribute_value_bytes",
            self.max_attribute_value_bytes,
            1,
            MAX_ATTRIBUTE_VALUE_BYTES,
        )?;
        validate_range("max_body_bytes", self.max_body_bytes, 1, MAX_BODY_BYTES)?;
        validate_range(
            "max_buckets_per_point",
            self.max_buckets_per_point,
            1,
            MAX_BUCKETS_PER_POINT,
        )?;
        validate_range(
            "max_exemplars_per_point",
            self.max_exemplars_per_point,
            1,
            MAX_EXEMPLARS_PER_POINT,
        )?;
        if let Some(quota) = self.max_db_bytes
            && quota < MIN_DB_QUOTA_BYTES
        {
            bail!("max_db_bytes must be at least {MIN_DB_QUOTA_BYTES} bytes (1 MiB); got {quota}");
        }
        validate_duration("batcher_max_batch_age", self.batcher_max_batch_age, false)?;
        validate_duration("shutdown_timeout", self.shutdown_timeout, false)?;
        validate_duration(
            "maintenance.retry_delay",
            self.maintenance.retry_delay,
            false,
        )?;
        validate_optional_duration(
            "maintenance.retention",
            self.maintenance.retention,
            MAX_RETENTION,
        )?;
        validate_optional_duration(
            "maintenance.metric_retention",
            self.maintenance.metric_retention,
            MAX_RETENTION,
        )?;
        validate_optional_duration(
            "maintenance.purge_interval",
            self.maintenance.purge_interval,
            MAX_TIMEOUT,
        )?;
        validate_optional_duration(
            "maintenance.checkpoint_interval",
            self.maintenance.checkpoint_interval,
            MAX_TIMEOUT,
        )?;
        validate_optional_duration(
            "maintenance.optimize_interval",
            self.maintenance.optimize_interval,
            MAX_RETENTION,
        )?;
        validate_optional_duration(
            "maintenance.vacuum_interval",
            self.maintenance.vacuum_interval,
            MAX_RETENTION,
        )?;
        validate_optional_duration(
            "maintenance.rebuild_fts_interval",
            self.maintenance.rebuild_fts_interval,
            MAX_RETENTION,
        )?;
        validate_duration("watchdog.tick_interval", self.watchdog.tick_interval, false)?;
        validate_duration(
            "watchdog.degraded_after",
            self.watchdog.degraded_after,
            false,
        )?;
        validate_duration(
            "watchdog.unhealthy_after",
            self.watchdog.unhealthy_after,
            false,
        )?;
        validate_duration(
            "watchdog.ack_stall_after",
            self.watchdog.ack_stall_after,
            false,
        )?;
        if self.watchdog.degraded_after >= self.watchdog.unhealthy_after {
            bail!("watchdog.degraded_after must be less than unhealthy_after");
        }
        if !self.watchdog.queue_pressure_ratio.is_finite()
            || !(0.0..=1.0).contains(&self.watchdog.queue_pressure_ratio)
            || self.watchdog.queue_pressure_ratio == 0.0
        {
            bail!("watchdog.queue_pressure_ratio must be finite and in the range (0, 1]");
        }
        if let Some(address) = &self.metrics_address {
            let parsed = expand_bind_address(address)
                .parse::<std::net::SocketAddr>()
                .with_context(|| format!("invalid metrics_address `{address}`"))?;
            if parsed.port() == 0 {
                bail!("metrics_address must use a non-zero port");
            }
            if !parsed.ip().is_loopback() && !self.allow_remote_metrics {
                bail!(
                    "metrics_address `{address}` is non-loopback; set allow_remote_metrics = true only when a protected metrics exposure is intended"
                );
            }
        }
        if let Some(tls) = &self.tls {
            readable_file("tls.cert", &tls.cert)?;
            readable_file("tls.key", &tls.key)?;
            if let Some(ca) = &tls.client_ca {
                readable_file("tls.client_ca", ca)?;
            }
        }
        if let Some(auth) = &self.auth {
            readable_file("auth.token_file", &auth.token_file)?;
        }
        Ok(())
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
            "max_db_bytes": self.max_db_bytes,
            "shutdown_timeout_ms": self.shutdown_timeout.as_millis(),
            "durability_mode": format!("{:?}", self.durability_mode),
            "sqlite_synchronous": format!("{:?}", self.sqlite_synchronous),
            "metrics_address": self.metrics_address,
            "allow_remote_metrics": self.allow_remote_metrics,
            "allow_insecure_remote": self.allow_insecure_remote,
            "tls_configured": self.tls.is_some(),
            "auth_configured": self.auth.is_some(),
            "maintenance": format!("{:?}", self.maintenance),
            "watchdog": format!("{:?}", self.watchdog),
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
            max_buckets_per_point: self.max_buckets_per_point,
            max_exemplars_per_point: self.max_exemplars_per_point,
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
            retention: None,
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

/// Mirror of [`Config`] where every field is optional; `None` means "keep
/// the built-in default".
#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct FileConfig {
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
    max_buckets_per_point: Option<usize>,
    max_exemplars_per_point: Option<usize>,
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
    fn apply_to(self, config: &mut Config) -> Result<()> {
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
        if let Some(value) = self.max_buckets_per_point {
            config.max_buckets_per_point = value;
        }
        if let Some(value) = self.max_exemplars_per_point {
            config.max_exemplars_per_point = value;
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

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct TlsSection {
    cert: Option<String>,
    key: Option<String>,
    client_ca: Option<String>,
}

impl TlsSection {
    fn apply_to(self) -> Result<TlsConfig> {
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

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct AuthSection {
    mode: Option<String>,
    token_file: Option<String>,
}

impl AuthSection {
    fn apply_to(self) -> Result<Option<AuthConfig>> {
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

/// Expands a bare `":port"` into `"0.0.0.0:port"` so the result always
/// parses as a [`std::net::SocketAddr`]; any other form is returned unchanged.
pub fn expand_bind_address(address: &str) -> String {
    if address.starts_with(':') {
        format!("0.0.0.0{address}")
    } else {
        address.to_owned()
    }
}

/// Parses an optional listen address; `"off"`/empty disable the endpoint.
/// Bare `":port"` binds all interfaces, mirroring `listen_address` handling.
fn parse_optional_address(raw: &str, key: &str) -> Result<Option<String>> {
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

fn validate_range<T>(key: &str, value: T, minimum: T, maximum: T) -> Result<()>
where
    T: Copy + Ord + std::fmt::Display,
{
    if value < minimum || value > maximum {
        bail!("config key `{key}` must be in the range {minimum}..={maximum}; got {value}");
    }
    Ok(())
}

fn validate_duration(key: &str, value: Duration, allow_zero: bool) -> Result<()> {
    if (!allow_zero && value.is_zero()) || value > MAX_TIMEOUT {
        let minimum = if allow_zero { "0" } else { "greater than 0" };
        bail!(
            "config key `{key}` must be {minimum} and no more than {MAX_TIMEOUT:?}; got {value:?}"
        );
    }
    Ok(())
}

fn validate_optional_duration(key: &str, value: Option<Duration>, maximum: Duration) -> Result<()> {
    if value.is_some_and(|value| value.is_zero() || value > maximum) {
        bail!(
            "config key `{key}` must be greater than 0 and no more than {maximum:?}; got {value:?}"
        );
    }
    Ok(())
}

fn readable_file(key: &str, path: &Path) -> Result<()> {
    std::fs::File::open(path).with_context(|| {
        format!(
            "config key `{key}` must point to a readable file: {}",
            path.display()
        )
    })?;
    // Private keys and bearer tokens must not be world-readable; public certs
    // are fine to skip. Warn (don't fail) to avoid breaking existing setups.
    if matches!(key, "tls.key" | "auth.token_file") {
        warn_if_world_readable(key, path);
    }
    Ok(())
}

/// Best-effort permission warning for secret files (unix only).
fn warn_if_world_readable(key: &str, path: &Path) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if let Ok(mode) = std::fs::metadata(path).map(|metadata| metadata.permissions().mode())
            && mode & 0o044 != 0
        {
            tracing::warn!(
                key,
                path = %path.display(),
                mode = format!("{mode:o}"),
                "secret file is readable beyond its owner; chmod 600 it",
            );
        }
    }
    #[cfg(not(unix))]
    {
        let _ = (key, path);
    }
}

/// `true` when `listen_address` resolves to a loopback-only socket. Used for
/// the cleartext-exposure startup gate in [`Config::validate`].
pub fn is_loopback_listen(listen_address: &str) -> bool {
    expand_bind_address(listen_address)
        .parse::<std::net::SocketAddr>()
        .is_ok_and(|addr| addr.ip().is_loopback())
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct DurabilitySection {
    mode: Option<String>,
    synchronous: Option<String>,
}

impl DurabilitySection {
    fn apply_to(self, config: &mut Config) -> Result<()> {
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

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct MaintenanceSection {
    retention: Option<RawRetention>,
    purge_interval: Option<RawDuration>,
    checkpoint_interval: Option<RawDuration>,
    checkpoint_mode: Option<String>,
    optimize_interval: Option<RawDuration>,
    vacuum_interval: Option<RawDuration>,
    rebuild_fts_interval: Option<RawDuration>,
    retry_delay: Option<RawDuration>,
}

impl MaintenanceSection {
    fn apply_to(self, config: &mut MaintenanceConfig) -> Result<()> {
        apply_retention(config, self.retention);
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

/// Retention windows as written in the config file.
///
/// One uniform window applies to every signal:
///
/// ```toml
/// [maintenance]
/// retention = "7d"
/// ```
///
/// Alternatively, per-signal windows prune each signal on its own schedule;
/// keys left out keep their current value (default or previously applied):
///
/// ```toml
/// [maintenance.retention]
/// logs = "7d"
/// metrics = "24h"
/// ```
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(untagged)]
enum RawRetention {
    /// One window for every signal.
    Uniform(RawDuration),
    /// Per-signal windows.
    PerSignal {
        logs: Option<RawDuration>,
        metrics: Option<RawDuration>,
    },
}

fn apply_retention(config: &mut MaintenanceConfig, raw: Option<RawRetention>) {
    let Some(raw) = raw else { return };
    match raw {
        RawRetention::Uniform(RawDuration::Enabled(window)) => {
            config.retention = Some(window);
            config.metric_retention = Some(window);
        }
        // Explicit opt-out ("off") disables both signals at once.
        RawRetention::Uniform(RawDuration::Disabled) => {
            config.retention = None;
            config.metric_retention = None;
        }
        RawRetention::PerSignal { logs, metrics } => {
            apply_optional_duration(&mut config.retention, logs);
            apply_optional_duration(&mut config.metric_retention, metrics);
        }
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct WatchdogSection {
    tick_interval: Option<RawDuration>,
    degraded_after: Option<RawDuration>,
    unhealthy_after: Option<RawDuration>,
    ack_stall_after: Option<RawDuration>,
    queue_pressure_ratio: Option<f32>,
    halt_on_unhealthy: Option<bool>,
}

impl WatchdogSection {
    fn apply_to(self, config: &mut WatchdogConfig) -> Result<()> {
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
        if let Some(ratio) = self.queue_pressure_ratio {
            config.queue_pressure_ratio = ratio;
        }
        if let Some(halt) = self.halt_on_unhealthy {
            config.halt_on_unhealthy = halt;
        }
        Ok(())
    }
}

/// A duration knob as written in the config file.
///
/// Accepted forms: a bare integer (interpreted as seconds) or a string with
/// an explicit unit such as `"250ms"`, `"10s"`, `"5m"`, `"2h"`, `"1d"`.
/// The markers `"off"`, `"none"`, `"never"`, `"disabled"` and the empty
/// string mean "explicitly disabled" for knobs that support switching off.
#[derive(Debug, Clone, Copy)]
enum RawDuration {
    Enabled(Duration),
    Disabled,
}

impl RawDuration {
    fn parse(text: &str) -> std::result::Result<Self, String> {
        let trimmed = text.trim();
        if trimmed.is_empty()
            || trimmed.eq_ignore_ascii_case("off")
            || trimmed.eq_ignore_ascii_case("none")
            || trimmed.eq_ignore_ascii_case("never")
            || trimmed.eq_ignore_ascii_case("disabled")
        {
            return Ok(Self::Disabled);
        }
        let split_at = trimmed
            .find(|c: char| !(c.is_ascii_digit() || c == '.'))
            .ok_or_else(|| format!("duration `{text}` lacks a unit (e.g. \"30s\")"))?;
        let (number, unit) = trimmed.split_at(split_at);
        let amount: f64 = number
            .trim()
            .parse()
            .map_err(|_| format!("duration `{text}` does not start with a number"))?;
        if amount < 0.0 {
            return Err(format!("duration `{text}` must not be negative"));
        }
        let unit = unit.trim();
        let scale_secs = if unit.eq_ignore_ascii_case("ms")
            || unit.eq_ignore_ascii_case("millis")
            || unit.eq_ignore_ascii_case("millisecond")
            || unit.eq_ignore_ascii_case("milliseconds")
        {
            0.001
        } else if unit.eq_ignore_ascii_case("s")
            || unit.eq_ignore_ascii_case("sec")
            || unit.eq_ignore_ascii_case("secs")
            || unit.eq_ignore_ascii_case("second")
            || unit.eq_ignore_ascii_case("seconds")
        {
            1.0
        } else if unit.eq_ignore_ascii_case("m")
            || unit.eq_ignore_ascii_case("min")
            || unit.eq_ignore_ascii_case("mins")
            || unit.eq_ignore_ascii_case("minute")
            || unit.eq_ignore_ascii_case("minutes")
        {
            60.0
        } else if unit.eq_ignore_ascii_case("h")
            || unit.eq_ignore_ascii_case("hr")
            || unit.eq_ignore_ascii_case("hrs")
            || unit.eq_ignore_ascii_case("hour")
            || unit.eq_ignore_ascii_case("hours")
        {
            3_600.0
        } else if unit.eq_ignore_ascii_case("d")
            || unit.eq_ignore_ascii_case("day")
            || unit.eq_ignore_ascii_case("days")
        {
            86_400.0
        } else {
            return Err(format!("unknown duration unit `{unit}` in `{text}`"));
        };
        Ok(Self::Enabled(Duration::from_millis(
            (amount * scale_secs * 1_000.0).round() as u64,
        )))
    }
}

impl<'de> Deserialize<'de> for RawDuration {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Repr {
            Seconds(u64),
            Text(String),
        }
        match Repr::deserialize(deserializer)? {
            Repr::Seconds(seconds) => Ok(Self::Enabled(Duration::from_secs(seconds))),
            Repr::Text(text) => Self::parse(&text).map_err(serde::de::Error::custom),
        }
    }
}

fn apply_required_duration(
    target: &mut Duration,
    raw: Option<RawDuration>,
    key: &str,
) -> Result<()> {
    let Some(raw) = raw else { return Ok(()) };
    match raw {
        RawDuration::Enabled(value) => *target = value,
        RawDuration::Disabled => {
            bail!("config key `{key}` requires a duration; \"off\" is not accepted")
        }
    }
    Ok(())
}

fn apply_optional_duration(target: &mut Option<Duration>, raw: Option<RawDuration>) {
    let Some(raw) = raw else { return };
    match raw {
        RawDuration::Enabled(value) => *target = Some(value),
        // Explicit opt-out ("off") vs. missing key (keeps the default).
        RawDuration::Disabled => *target = None,
    }
}

fn parse_checkpoint_mode(text: &str) -> Result<CheckpointMode> {
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
        bail!(
            "unknown checkpoint_mode `{trimmed}` (expected passive|full|restart|truncate)"
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn file_config(toml_text: &str) -> FileConfig {
        toml::from_str(toml_text).expect("valid config TOML")
    }

    fn temp_config_path(tag: &str) -> PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let unique = COUNTER.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "otel-sqlite-config-{tag}-{}-{unique}.toml",
            std::process::id()
        ))
    }

    #[test]
    fn empty_overlay_keeps_defaults() {
        let mut config = Config::default();
        file_config("").apply_to(&mut config).expect("applies");
        assert_eq!(config.listen_address, DEFAULT_LISTEN_ADDRESS);
        assert!(
            is_loopback_listen(&config.listen_address),
            "default listen_address must be loopback-only (H1), got {}",
            config.listen_address
        );
        assert_eq!(config.sqlite_path, PathBuf::from("otel-logs.db"));
        assert_eq!(config.ingest_queue_capacity, DEFAULT_INGEST_QUEUE_CAPACITY);
        assert_eq!(
            config.command_queue_capacity,
            otel_sqlite_storage::DEFAULT_COMMAND_QUEUE_CAPACITY
        );
        assert_eq!(config.batcher_max_batch_age, Duration::from_millis(10));
        assert_eq!(
            config.grpc_max_concurrent_streams,
            DEFAULT_MAX_CONCURRENT_STREAMS
        );
        assert_eq!(
            config.max_records_per_request,
            DEFAULT_MAX_RECORDS_PER_REQUEST
        );
        assert_eq!(config.shutdown_timeout, DEFAULT_SHUTDOWN_TIMEOUT);
        assert_eq!(
            config.ingress_config().max_records_per_request,
            DEFAULT_MAX_RECORDS_PER_REQUEST
        );
        config.validate().expect("built-in defaults are valid");
    }

    #[test]
    fn top_level_keys_override_defaults() {
        let mut config = Config::default();
        file_config(
            r#"
            listen_address = "[::1]:4317"
            sqlite_path = "data/logs.db"
            ingest_queue_capacity = 10
            command_queue_capacity = 20
            batcher_max_batch_records = 42
            batcher_max_batch_age = "250ms"
             grpc_max_recv_msg_size = 1024
             grpc_max_concurrent_streams = 8
             max_records_per_request = 777
             max_attributes_per_record = 111
             max_attribute_key_bytes = 222
             max_attribute_value_bytes = 333
             max_body_bytes = 444
             max_buckets_per_point = 555
             max_exemplars_per_point = 666
             shutdown_timeout = 90
            "#,
        )
        .apply_to(&mut config)
        .expect("applies");
        assert_eq!(config.listen_address, "[::1]:4317");
        assert_eq!(config.sqlite_path, PathBuf::from("data/logs.db"));
        assert_eq!(config.ingest_queue_capacity, 10);
        assert_eq!(config.command_queue_capacity, 20);
        assert_eq!(config.batcher_max_batch_records, 42);
        assert_eq!(config.batcher_max_batch_age, Duration::from_millis(250));
        assert_eq!(config.grpc_max_recv_msg_size, 1024);
        assert_eq!(config.grpc_max_concurrent_streams, 8);
        assert_eq!(config.max_records_per_request, 777);
        assert_eq!(config.max_attributes_per_record, 111);
        assert_eq!(config.max_attribute_key_bytes, 222);
        assert_eq!(config.max_attribute_value_bytes, 333);
        assert_eq!(config.max_body_bytes, 444);
        assert_eq!(config.max_buckets_per_point, 555);
        assert_eq!(config.max_exemplars_per_point, 666);
        assert_eq!(config.shutdown_timeout, Duration::from_secs(90));
    }

    #[test]
    fn size_quota_parses_validates_and_reaches_storage() {
        let mut config = Config::default();
        assert_eq!(config.max_db_bytes, None, "unbounded by default");
        file_config("max_db_bytes = 1073741824\n")
            .apply_to(&mut config)
            .expect("applies");
        assert_eq!(config.max_db_bytes, Some(1_073_741_824));
        config.validate().expect("1 GiB quota is valid");
        assert_eq!(config.storage_config().max_db_bytes, Some(1_073_741_824));
        let dump = config.dump_json().expect("dumps");
        assert!(dump.contains("max_db_bytes"), "dump must expose the quota");

        // Below 1 MiB the writer would evict on nearly every batch: reject.
        let tiny = Config {
            max_db_bytes: Some(1024),
            ..Config::default()
        };
        assert!(tiny.validate().is_err(), "sub-MiB quotas must be rejected");
    }

    #[test]
    fn duration_formats_are_accepted() {
        let mut config = Config::default();
        file_config(
            r#"
            batcher_max_batch_age = "250ms"
            shutdown_timeout = "1h"
            [watchdog]
            tick_interval = "2m"
            unhealthy_after = "1d"
            "#,
        )
        .apply_to(&mut config)
        .expect("applies");
        assert_eq!(config.batcher_max_batch_age, Duration::from_millis(250));
        assert_eq!(config.shutdown_timeout, Duration::from_secs(3_600));
        assert_eq!(config.watchdog.tick_interval, Duration::from_secs(120));
        assert_eq!(config.watchdog.unhealthy_after, Duration::from_secs(86_400));
    }

    #[test]
    fn nested_sections_merge_field_wise() {
        let mut config = Config::default();
        file_config(
            r#"
            [maintenance]
            checkpoint_mode = "TRUNCATE"
            retention = "168h"
            vacuum_interval = "off"

            [watchdog]
            queue_pressure_ratio = 0.9
            halt_on_unhealthy = false
            "#,
        )
        .apply_to(&mut config)
        .expect("applies");

        assert_eq!(config.maintenance.checkpoint_mode, CheckpointMode::Truncate);
        assert_eq!(
            config.maintenance.retention,
            Some(Duration::from_secs(7 * 24 * 3_600))
        );
        // A uniform retention window covers both signals.
        assert_eq!(
            config.maintenance.metric_retention,
            Some(Duration::from_secs(7 * 24 * 3_600))
        );
        assert_eq!(config.maintenance.vacuum_interval, None);
        // Untouched siblings keep their defaults.
        assert_eq!(
            config.maintenance.purge_interval,
            Some(Duration::from_secs(15 * 60))
        );
        assert_eq!(
            config.maintenance.checkpoint_interval,
            Some(Duration::from_secs(5 * 60))
        );
        assert!((config.watchdog.queue_pressure_ratio - 0.9).abs() < f32::EPSILON);
        assert!(!config.watchdog.halt_on_unhealthy);
        assert_eq!(config.watchdog.tick_interval, Duration::from_secs(5));
    }

    #[test]
    fn retention_accepts_uniform_and_per_signal_forms() {
        // Uniform: one window for every signal (also accepts bare seconds).
        let mut config = Config::default();
        file_config("[maintenance]\nretention = 60")
            .apply_to(&mut config)
            .expect("applies");
        assert_eq!(config.maintenance.retention, Some(Duration::from_secs(60)));
        assert_eq!(
            config.maintenance.metric_retention,
            Some(Duration::from_secs(60))
        );

        // Per-signal table: each signal gets its own window.
        let mut config = Config::default();
        file_config(
            r#"
            [maintenance.retention]
            logs = "7d"
            metrics = "24h"
            "#,
        )
        .apply_to(&mut config)
        .expect("applies");
        assert_eq!(
            config.maintenance.retention,
            Some(Duration::from_secs(7 * 86_400))
        );
        assert_eq!(
            config.maintenance.metric_retention,
            Some(Duration::from_secs(86_400))
        );

        // Per-signal table with only one key: the other keeps its default.
        let mut config = Config::default();
        file_config("[maintenance.retention]\nmetrics = \"1h\"")
            .apply_to(&mut config)
            .expect("applies");
        assert_eq!(config.maintenance.retention, None);
        assert_eq!(
            config.maintenance.metric_retention,
            Some(Duration::from_secs(3_600))
        );

        // Explicit opt-out disables both signals at once.
        let mut config = Config::default();
        file_config("[maintenance]\nretention = \"off\"")
            .apply_to(&mut config)
            .expect("applies");
        assert_eq!(config.maintenance.retention, None);
        assert_eq!(config.maintenance.metric_retention, None);

        // Inline-table form is equivalent to the section form.
        let mut config = Config::default();
        file_config("[maintenance]\nretention = { logs = \"1d\", metrics = \"2d\" }")
            .apply_to(&mut config)
            .expect("applies");
        assert_eq!(
            config.maintenance.retention,
            Some(Duration::from_secs(86_400))
        );
        assert_eq!(
            config.maintenance.metric_retention,
            Some(Duration::from_secs(2 * 86_400))
        );
    }

    #[test]
    fn rebuild_fts_scheduling_is_opt_in() {
        let mut config = Config::default();
        file_config("").apply_to(&mut config).expect("applies");
        assert_eq!(config.maintenance.rebuild_fts_interval, None);

        file_config("[maintenance]\nrebuild_fts_interval = \"7d\"")
            .apply_to(&mut config)
            .expect("applies");
        assert_eq!(
            config.maintenance.rebuild_fts_interval,
            Some(Duration::from_secs(7 * 86_400))
        );
    }

    #[test]
    fn mandatory_duration_rejects_off() {
        let mut config = Config::default();
        let result = file_config("shutdown_timeout = \"off\"").apply_to(&mut config);
        assert!(result.is_err());
    }

    #[test]
    fn durability_section_defaults_to_commit_and_normal_sync() {
        let mut config = Config::default();
        file_config("").apply_to(&mut config).expect("applies");
        assert_eq!(config.durability_mode, DurabilityMode::Commit);
        assert_eq!(config.sqlite_synchronous, SyncMode::Normal);

        file_config(
            r#"
            [durability]
            mode = "enqueue"
            synchronous = "full"
            "#,
        )
        .apply_to(&mut config)
        .expect("applies");
        assert_eq!(config.durability_mode, DurabilityMode::Enqueue);
        assert_eq!(config.sqlite_synchronous, SyncMode::Full);
        // The knobs flow through to the per-crate configs.
        assert_eq!(
            config.ingress_config().durability_mode,
            DurabilityMode::Enqueue
        );
        assert_eq!(config.storage_config().synchronous, SyncMode::Full);
    }

    #[test]
    fn durability_section_is_case_insensitive_and_rejects_unknown_values() {
        let mut config = Config::default();
        file_config("[durability]\nmode = \"Commit\"")
            .apply_to(&mut config)
            .expect("applies");
        assert_eq!(config.durability_mode, DurabilityMode::Commit);

        let mut config = Config::default();
        let result = file_config("[durability]\nmode = \"durable\"").apply_to(&mut config);
        assert!(result.is_err());

        let mut config = Config::default();
        let result = file_config("[durability]\nsynchronous = \"off\"").apply_to(&mut config);
        assert!(result.is_err());
    }

    #[test]
    fn tls_and_auth_sections_parse_and_validate() {
        let mut config = Config::default();
        file_config("").apply_to(&mut config).expect("applies");
        assert!(config.tls.is_none());
        assert!(config.auth.is_none());

        // mTLS + token auth together.
        file_config(
            r#"
            [tls]
            cert = "/certs/server.pem"
            key = "/certs/server.key"
            client_ca = "/certs/ca.pem"

            [auth]
            mode = "token"
            token_file = "/etc/otel.txt"
            "#,
        )
        .apply_to(&mut config)
        .expect("applies");
        let tls = config.tls.as_ref().expect("tls set");
        assert_eq!(tls.cert, PathBuf::from("/certs/server.pem"));
        assert_eq!(tls.client_ca, Some(PathBuf::from("/certs/ca.pem")));
        let auth = config.auth.as_ref().expect("auth set");
        assert_eq!(auth.token_file, PathBuf::from("/etc/otel.txt"));

        // Server TLS without mTLS is valid too.
        let mut config = Config::default();
        file_config("[tls]\ncert = \"s.pem\"\nkey = \"s.key\"")
            .apply_to(&mut config)
            .expect("applies");
        assert!(config.tls.as_ref().expect("tls").client_ca.is_none());

        // [tls] without cert/key is a config error.
        for incomplete in ["[tls]\nkey = \"k\"", "[tls]\ncert = \"c\""] {
            let mut config = Config::default();
            assert!(
                file_config(incomplete).apply_to(&mut config).is_err(),
                "incomplete [tls] must be rejected: {incomplete}"
            );
        }

        // mode=token requires token_file; none + token_file contradicts;
        // unknown modes are rejected.
        let mut config = Config::default();
        assert!(
            file_config("[auth]\nmode = \"token\"")
                .apply_to(&mut config)
                .is_err()
        );
        let mut config = Config::default();
        assert!(
            file_config("[auth]\ntoken_file = \"t\"")
                .apply_to(&mut config)
                .is_err(),
            "\"none\" with token_file must be rejected"
        );
        let mut config = Config::default();
        assert!(
            file_config("[auth]\nmode = \"oauth\"")
                .apply_to(&mut config)
                .is_err()
        );
    }

    #[test]
    fn metrics_address_defaults_on_localhost_and_can_be_disabled() {
        let mut config = Config::default();
        file_config("").apply_to(&mut config).expect("applies");
        assert_eq!(
            config.metrics_address.as_deref(),
            Some(DEFAULT_METRICS_ADDRESS)
        );

        file_config("metrics_address = \"0.0.0.0:9100\"\nallow_remote_metrics = true")
            .apply_to(&mut config)
            .expect("applies");
        assert_eq!(config.metrics_address.as_deref(), Some("0.0.0.0:9100"));

        // Bare ":port" is valid and binds all interfaces.
        file_config("metrics_address = \":9464\"")
            .apply_to(&mut config)
            .expect("applies");
        assert_eq!(config.metrics_address.as_deref(), Some(":9464"));

        for off in ["off", "disabled", ""] {
            let mut config = Config::default();
            let toml_text = if off.is_empty() {
                "metrics_address = \"\"".to_owned()
            } else {
                format!("metrics_address = \"{off}\"")
            };
            file_config(&toml_text)
                .apply_to(&mut config)
                .expect("applies");
            assert_eq!(config.metrics_address, None, "value {off:?} disables");
        }

        let mut config = Config::default();
        let result = file_config("metrics_address = \"not-an-address\"").apply_to(&mut config);
        assert!(result.is_err(), "invalid address must be rejected");
    }

    #[test]
    fn validation_rejects_unsafe_or_out_of_range_values() {
        macro_rules! assert_invalid {
            ($key:literal, $config:expr) => {
                let error = $config.validate().expect_err("invalid config must fail");
                assert!(error.to_string().contains($key), "{}: {error}", $key);
            };
        }
        assert_invalid!(
            "listen_address",
            Config {
                listen_address: ":0".to_owned(),
                ..Config::default()
            }
        );
        assert_invalid!(
            "ingest_queue_capacity",
            Config {
                ingest_queue_capacity: 0,
                ..Config::default()
            }
        );
        assert_invalid!(
            "command_queue_capacity",
            Config {
                command_queue_capacity: MAX_INGEST_QUEUE_CAPACITY + 1,
                ..Config::default()
            }
        );
        assert_invalid!(
            "batcher_max_batch_records",
            Config {
                batcher_max_batch_records: 0,
                ..Config::default()
            }
        );
        assert_invalid!(
            "grpc_max_recv_msg_size",
            Config {
                grpc_max_recv_msg_size: 512,
                ..Config::default()
            }
        );
        assert_invalid!(
            "grpc_max_concurrent_streams",
            Config {
                grpc_max_concurrent_streams: 0,
                ..Config::default()
            }
        );
        assert_invalid!(
            "grpc_max_concurrent_streams",
            Config {
                grpc_max_concurrent_streams: MAX_GRPC_CONCURRENT_STREAMS + 1,
                ..Config::default()
            }
        );
        assert_invalid!(
            "max_records_per_request",
            Config {
                max_records_per_request: MAX_RECORDS_PER_REQUEST + 1,
                ..Config::default()
            }
        );
        assert_invalid!(
            "max_attributes_per_record",
            Config {
                max_attributes_per_record: MAX_ATTRIBUTES_PER_RECORD + 1,
                ..Config::default()
            }
        );
        assert_invalid!(
            "max_attribute_key_bytes",
            Config {
                max_attribute_key_bytes: MAX_ATTRIBUTE_KEY_BYTES + 1,
                ..Config::default()
            }
        );
        assert_invalid!(
            "max_attribute_value_bytes",
            Config {
                max_attribute_value_bytes: MAX_ATTRIBUTE_VALUE_BYTES + 1,
                ..Config::default()
            }
        );
        assert_invalid!(
            "max_body_bytes",
            Config {
                max_body_bytes: MAX_BODY_BYTES + 1,
                ..Config::default()
            }
        );
        assert_invalid!(
            "max_buckets_per_point",
            Config {
                max_buckets_per_point: MAX_BUCKETS_PER_POINT + 1,
                ..Config::default()
            }
        );
        assert_invalid!(
            "max_exemplars_per_point",
            Config {
                max_exemplars_per_point: MAX_EXEMPLARS_PER_POINT + 1,
                ..Config::default()
            }
        );
        assert_invalid!(
            "shutdown_timeout",
            Config {
                shutdown_timeout: Duration::ZERO,
                ..Config::default()
            }
        );

        assert_invalid!(
            "allow_remote_metrics",
            Config {
                metrics_address: Some("0.0.0.0:8888".to_owned()),
                ..Config::default()
            }
        );

        assert!(
            Config {
                watchdog: WatchdogConfig {
                    queue_pressure_ratio: f32::NAN,
                    ..WatchdogConfig::default()
                },
                ..Config::default()
            }
            .validate()
            .is_err()
        );

        let watchdog = WatchdogConfig::default();
        assert!(
            Config {
                watchdog: WatchdogConfig {
                    degraded_after: watchdog.unhealthy_after,
                    ..watchdog
                },
                ..Config::default()
            }
            .validate()
            .is_err()
        );
    }

    #[test]
    fn validation_requires_readable_tls_and_auth_files() {
        let dir = tempfile::tempdir().expect("temp directory");
        let cert = dir.path().join("server.pem");
        let key = dir.path().join("server.key");
        let ca = dir.path().join("ca.pem");
        let token = dir.path().join("tokens.txt");
        for path in [&cert, &key, &ca, &token] {
            std::fs::write(path, "test").expect("write test file");
        }

        let mut config = Config {
            tls: Some(TlsConfig {
                cert: cert.clone(),
                key: key.clone(),
                client_ca: Some(ca),
            }),
            auth: Some(AuthConfig { token_file: token }),
            ..Config::default()
        };
        config
            .validate()
            .expect("readable security files are valid");

        config.tls.as_mut().expect("tls").key = dir.path().join("missing.key");
        let error = config.validate().expect_err("missing TLS key must fail");
        assert!(error.to_string().contains("tls.key"));
    }

    #[test]
    fn effective_dump_contains_runtime_values() {
        let config = Config {
            max_records_per_request: 321,
            metrics_address: None,
            ..Config::default()
        };
        let dump = config.dump_json().expect("serializes");
        assert!(dump.contains("\"max_records_per_request\": 321"));
        assert!(dump.contains("\"metrics_address\": null"));
        assert!(dump.contains("\"allow_remote_metrics\": false"));
        assert!(dump.contains("\"allow_insecure_remote\": false"));
    }

    #[test]
    fn non_loopback_listen_requires_tls_or_explicit_opt_in() {
        // Loopback binds stay usable without TLS (the default).
        Config {
            listen_address: "127.0.0.1:4317".to_owned(),
            ..Config::default()
        }
        .validate()
        .expect("loopback without TLS is valid");

        // A wildcard bind without TLS fails closed with a prescriptive error.
        let error = Config {
            listen_address: "0.0.0.0:4317".to_owned(),
            ..Config::default()
        }
        .validate()
        .expect_err("cleartext remote bind must fail");
        assert!(
            error.to_string().contains("allow_insecure_remote"),
            "unexpected error: {error}"
        );

        // The explicit isolated-network opt-in restores the old behaviour.
        Config {
            listen_address: "0.0.0.0:4317".to_owned(),
            allow_insecure_remote: true,
            ..Config::default()
        }
        .validate()
        .expect("explicit opt-in is valid");

        // A TLS identity satisfies the gate without the opt-in.
        let dir = tempfile::tempdir().expect("temp directory");
        let cert = dir.path().join("server.pem");
        let key = dir.path().join("server.key");
        std::fs::write(&cert, "test").expect("write test file");
        std::fs::write(&key, "test").expect("write test file");
        Config {
            listen_address: "0.0.0.0:4317".to_owned(),
            tls: Some(TlsConfig {
                cert,
                key,
                client_ca: None,
            }),
            ..Config::default()
        }
        .validate()
        .expect("remote bind with TLS is valid");
    }

    #[test]
    fn bare_port_expands_to_wildcard() {
        assert_eq!(expand_bind_address(":4317"), "0.0.0.0:4317");
        assert_eq!(expand_bind_address("127.0.0.1:9"), "127.0.0.1:9");
        assert_eq!(expand_bind_address("[::1]:80"), "[::1]:80");
    }

    #[test]
    fn loopback_classification_matches_bound_interface() {
        assert!(is_loopback_listen("127.0.0.1:4317"));
        assert!(is_loopback_listen("[::1]:4317"));
        // Bare ":port" and 0.0.0.0 bind every interface, not just loopback.
        assert!(!is_loopback_listen(":4317"));
        assert!(!is_loopback_listen("0.0.0.0:4317"));
        // Unparseable addresses never count as loopback.
        assert!(!is_loopback_listen("not-an-address"));
    }

    #[test]
    fn unknown_keys_are_rejected() {
        assert!(toml::from_str::<FileConfig>("no_such_key = 1").is_err());
        assert!(toml::from_str::<FileConfig>("[watchdog]\nnope = true").is_err());
    }

    #[test]
    fn malformed_values_are_rejected() {
        assert!(toml::from_str::<FileConfig>("shutdown_timeout = \"5 parsecs\"").is_err());
        assert!(toml::from_str::<FileConfig>("shutdown_timeout = \"abc\"").is_err());
        assert!(toml::from_str::<FileConfig>("shutdown_timeout = \"-30s\"").is_err());
        // Unknown checkpoint modes parse as plain strings; they are rejected
        // when the file is applied to the runtime configuration.
        let file = toml::from_str::<FileConfig>("[maintenance]\ncheckpoint_mode = \"aggressive\"")
            .expect("string field parses");
        let mut config = Config::default();
        assert!(file.apply_to(&mut config).is_err());
    }

    #[test]
    fn from_file_overrides_defaults_and_keeps_rest() {
        let path = temp_config_path("e2e");
        std::fs::write(
            &path,
            r#"
            listen_address = ":1234"
            sqlite_path = "/tmp/otel.db"
            shutdown_timeout = "45s"
            allow_insecure_remote = true

            [maintenance]
            retention = "7d"
            "#,
        )
        .expect("write temp config");
        let config = Config::from_file(&path);
        let _ = std::fs::remove_file(&path);

        let config = config.expect("valid config file");
        assert_eq!(config.listen_address, ":1234");
        assert!(config.allow_insecure_remote);
        assert_eq!(config.sqlite_path, PathBuf::from("/tmp/otel.db"));
        assert_eq!(config.shutdown_timeout, Duration::from_secs(45));
        assert_eq!(
            config.maintenance.retention,
            Some(Duration::from_secs(7 * 86_400))
        );
        assert_eq!(config.ingest_queue_capacity, 50_000);
    }

    #[test]
    fn from_file_missing_reports_the_path() {
        let result = Config::from_file(Path::new("definitely-missing-cfg.toml"));
        let error = result.expect_err("missing file must fail");
        assert!(error.to_string().contains("definitely-missing-cfg.toml"));
    }
}
