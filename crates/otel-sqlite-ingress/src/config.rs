use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use otel_sqlite_core::storage::DurabilityMode;

use crate::error::IngressError;

/// Server-side TLS material. `client_ca` present ⇒ mandatory mTLS: only
/// clients presenting a certificate signed by that CA are accepted.
#[derive(Debug, Clone)]
pub struct TlsConfig {
    pub cert: PathBuf,
    pub key: PathBuf,
    pub client_ca: Option<PathBuf>,
}

/// Bearer-token authentication. The file holds one token per line; every
/// line is valid, which makes rotation "append new, migrate, remove old"
/// without restarting.
#[derive(Debug, Clone)]
pub struct AuthConfig {
    pub token_file: PathBuf,
}

/// Default ingress bind: loopback-only. Operators that must accept remote
/// traffic set an explicit non-loopback `listen_address` together with
/// `[tls]`/`[auth]`; a bare `":port"` still expands to all interfaces for
/// backwards compatibility (see `socket_addr`), but it is never the default.
pub const DEFAULT_LISTEN_ADDRESS: &str = "127.0.0.1:4317";
pub const DEFAULT_MAX_RECV_MSG_SIZE: usize = 16 * 1024 * 1024;
pub const DEFAULT_MAX_CONCURRENT_STREAMS: u32 = 256;
pub const DEFAULT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);
pub const DEFAULT_MAX_RECORDS_PER_REQUEST: usize = 100_000;
/// Per-record attribute cap: bounds the `attributes`/`metadata`/`filtered_attributes`
/// vectors that ride on a single log record or metric data point (resource and
/// scope attribute vectors are each held to the same bound). The gRPC message
/// size already bounds the request total; this bounds the per-record CPU/alloc
/// an attacker can force with one record inside that budget.
pub const DEFAULT_MAX_ATTRIBUTES_PER_RECORD: usize = 1_000;
/// Max bytes for a single attribute key (`KeyValue.key`, metric names, scope
/// names, etc. are held to the value bound below where applicable).
pub const DEFAULT_MAX_ATTRIBUTE_KEY_BYTES: usize = 512;
/// Max bytes for a single attribute string/bytes value.
pub const DEFAULT_MAX_ATTRIBUTE_VALUE_BYTES: usize = 64 * 1024;
/// Max bytes for a scalar log body (`StringValue`/`BytesValue` rendered as text).
/// Structured bodies (`ArrayValue`/`KvlistValue`) are bounded by the attribute
/// byte bounds plus `MAX_NESTING_DEPTH` instead.
pub const DEFAULT_MAX_BODY_BYTES: usize = 1024 * 1024;
/// Max `bucket_counts`/`explicit_bounds`/`quantile_values` entries on one
/// histogram-family data point (also covers each exponential-bucket side).
pub const DEFAULT_MAX_BUCKETS_PER_POINT: usize = 10_000;
/// Max exemplars attached to one metric data point.
pub const DEFAULT_MAX_EXEMPLARS_PER_POINT: usize = 100;

/// Operator-tunable upper bounds accepted by `Config::validate` (generous;
/// the wire budget `grpc_max_recv_msg_size` remains the total-request backstop).
pub const MAX_ATTRIBUTES_PER_RECORD: usize = 100_000;
pub const MAX_ATTRIBUTE_KEY_BYTES: usize = 16_384;
pub const MAX_ATTRIBUTE_VALUE_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_BODY_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_BUCKETS_PER_POINT: usize = 1_000_000;
pub const MAX_EXEMPLARS_PER_POINT: usize = 10_000;

#[derive(Debug, Clone)]
pub struct IngressConfig {
    pub listen_address: String,
    pub max_recv_msg_size: usize,
    pub max_concurrent_streams: u32,
    pub shutdown_timeout: Duration,
    pub max_records_per_request: usize,
    /// See `DEFAULT_MAX_ATTRIBUTES_PER_RECORD`.
    pub max_attributes_per_record: usize,
    /// See `DEFAULT_MAX_ATTRIBUTE_KEY_BYTES`.
    pub max_attribute_key_bytes: usize,
    /// See `DEFAULT_MAX_ATTRIBUTE_VALUE_BYTES`.
    pub max_attribute_value_bytes: usize,
    /// See `DEFAULT_MAX_BODY_BYTES`.
    pub max_body_bytes: usize,
    /// See `DEFAULT_MAX_BUCKETS_PER_POINT`.
    pub max_buckets_per_point: usize,
    /// See `DEFAULT_MAX_EXEMPLARS_PER_POINT`.
    pub max_exemplars_per_point: usize,
    /// When to acknowledge OTLP export requests. `Commit` (the default)
    /// holds each response until the storage writer has committed the
    /// accepted records; `Enqueue` acknowledges as soon as they entered the
    /// bounded ingest queue (acknowledged records may be lost on a crash).
    pub durability_mode: DurabilityMode,
    pub tls: Option<TlsConfig>,
    pub auth: Option<AuthConfig>,
}

impl Default for IngressConfig {
    fn default() -> Self {
        Self {
            listen_address: DEFAULT_LISTEN_ADDRESS.to_owned(),
            max_recv_msg_size: DEFAULT_MAX_RECV_MSG_SIZE,
            max_concurrent_streams: DEFAULT_MAX_CONCURRENT_STREAMS,
            shutdown_timeout: DEFAULT_SHUTDOWN_TIMEOUT,
            max_records_per_request: DEFAULT_MAX_RECORDS_PER_REQUEST,
            max_attributes_per_record: DEFAULT_MAX_ATTRIBUTES_PER_RECORD,
            max_attribute_key_bytes: DEFAULT_MAX_ATTRIBUTE_KEY_BYTES,
            max_attribute_value_bytes: DEFAULT_MAX_ATTRIBUTE_VALUE_BYTES,
            max_body_bytes: DEFAULT_MAX_BODY_BYTES,
            max_buckets_per_point: DEFAULT_MAX_BUCKETS_PER_POINT,
            max_exemplars_per_point: DEFAULT_MAX_EXEMPLARS_PER_POINT,
            durability_mode: DurabilityMode::default(),
            tls: None,
            auth: None,
        }
    }
}

impl IngressConfig {
    pub fn socket_addr(&self) -> Result<SocketAddr, IngressError> {
        if let Ok(addr) = self.listen_address.parse::<SocketAddr>() {
            return Ok(addr);
        }

        format!("0.0.0.0{}", self.listen_address)
            .parse()
            .map_err(|source| IngressError::InvalidListenAddress {
                listen_address: self.listen_address.clone(),
                source,
            })
    }
}
