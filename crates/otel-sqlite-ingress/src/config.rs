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

pub const DEFAULT_LISTEN_ADDRESS: &str = ":4317";
pub const DEFAULT_MAX_RECV_MSG_SIZE: usize = 16 * 1024 * 1024;
pub const DEFAULT_MAX_CONCURRENT_STREAMS: u32 = 256;
pub const DEFAULT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);
pub const DEFAULT_MAX_RECORDS_PER_REQUEST: usize = 100_000;

#[derive(Debug, Clone)]
pub struct IngressConfig {
    pub listen_address: String,
    pub max_recv_msg_size: usize,
    pub max_concurrent_streams: u32,
    pub shutdown_timeout: Duration,
    pub max_records_per_request: usize,
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
