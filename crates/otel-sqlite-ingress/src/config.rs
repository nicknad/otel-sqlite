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
/// Default cap on concurrent OTLP exports. Applied twice: as the HTTP/2
/// `max_concurrent_streams` per connection, and as a process-wide semaphore
/// shared by the logs and metrics services (see `grpc::run`), so total
/// in-flight exports never exceed this even across many connections.
/// Worst-case in-flight request bytes are roughly
/// `max_concurrent_streams * max_recv_msg_size` (64 * 16 MiB = 1 GiB at the
/// defaults, before JSON/FTS/fingerprint expansion), so raise it only
/// together with memory headroom. Per-request CPU/alloc inside that budget
/// is bounded separately by the per-record caps below plus the mapping
/// timeout.
pub const DEFAULT_MAX_CONCURRENT_STREAMS: u32 = 64;
pub const DEFAULT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);
/// Upper bound on how long one export handler waits for its proto→model
/// mapping task (M5). A wedged or hostile mapping job must fail its RPC
/// instead of holding a stream slot (and a process-wide concurrency permit)
/// forever; the handler answers `UNAVAILABLE` so exporters retry.
///
/// Tokio cannot cancel a `spawn_blocking` task: on timeout the request future
/// is released (freeing its stream slot and permit so retries can proceed),
/// but the blocking task itself runs to completion in the background.
pub const MAPPING_TIMEOUT: Duration = Duration::from_secs(30);
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
/// Aggregate scope-metadata expansion budget per request: the estimated model
/// bytes of one `ScopeLogs`/`ScopeMetrics` metadata block (name + version +
/// schema URL + attributes) multiplied by the number of member records.
///
/// Mapped records share one `LogScope` per group, so this product is a
/// conservative worst-case bound rather than the actual mapped footprint: it
/// still rejects a small scope block repeated across the 100k-record cap that
/// would have forced multi-GiB mappings (and as many duplicated JSON bytes in
/// SQLite) under the old per-record copy model. Defaults to one wire budget
/// (`DEFAULT_MAX_RECV_MSG_SIZE`), so scope expansion can never exceed the
/// memory the request could already have used on the wire.
pub const DEFAULT_MAX_SCOPE_METADATA_EXPANSION_BYTES: usize = 16 * 1024 * 1024;

/// Operator-tunable upper bounds accepted by `Config::validate` (generous;
/// the wire budget `grpc_max_recv_msg_size` remains the total-request backstop).
pub const MAX_ATTRIBUTES_PER_RECORD: usize = 100_000;
pub const MAX_ATTRIBUTE_KEY_BYTES: usize = 16_384;
pub const MAX_ATTRIBUTE_VALUE_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_BODY_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_SCOPE_METADATA_EXPANSION_BYTES: usize = 256 * 1024 * 1024;

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
    /// See `DEFAULT_MAX_SCOPE_METADATA_EXPANSION_BYTES`.
    pub max_scope_metadata_expansion_bytes: usize,
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
            max_scope_metadata_expansion_bytes: DEFAULT_MAX_SCOPE_METADATA_EXPANSION_BYTES,
            durability_mode: DurabilityMode::default(),
            tls: None,
            auth: None,
        }
    }
}

impl IngressConfig {
    /// Parses [`IngressConfig::listen_address`] into a socket address.
    ///
    /// Accepts only `IP:port` or a bare `:port` (which binds all
    /// interfaces as `0.0.0.0:port`); hostnames are not resolved.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn socket_addr_accepts_ip_and_expands_bare_port_to_all_interfaces() {
        let ip = IngressConfig {
            listen_address: "127.0.0.1:4317".to_owned(),
            ..IngressConfig::default()
        };
        assert_eq!(
            ip.socket_addr().expect("valid ip:port"),
            "127.0.0.1:4317".parse().unwrap()
        );

        let bare = IngressConfig {
            listen_address: ":4317".to_owned(),
            ..IngressConfig::default()
        };
        assert_eq!(
            bare.socket_addr().expect("bare port expands"),
            "0.0.0.0:4317".parse().unwrap(),
            "a bare `:port` must bind all interfaces"
        );
    }

    #[test]
    fn socket_addr_error_names_hostnames_unsupported_and_bare_port_semantics() {
        let config = IngressConfig {
            listen_address: "localhost:4317".to_owned(),
            ..IngressConfig::default()
        };
        let message = config
            .socket_addr()
            .expect_err("hostnames are not supported")
            .to_string();
        assert!(
            message.contains("hostnames are not supported"),
            "unexpected error: {message}"
        );
        assert!(
            message.contains("bare `:port` binds all interfaces"),
            "unexpected error: {message}"
        );
    }
}
