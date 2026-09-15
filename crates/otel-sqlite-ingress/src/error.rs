use std::net::AddrParseError;

use tonic::transport::Error as TransportError;

#[derive(Debug, thiserror::Error)]
pub enum IngressError {
    #[error(
        "invalid listen address `{listen_address}`: use an IP:port tuple (e.g. `127.0.0.1:4317`); a bare `:port` binds all interfaces, and hostnames are not supported"
    )]
    InvalidListenAddress {
        listen_address: String,
        #[source]
        source: AddrParseError,
    },

    #[error("gRPC transport failure")]
    Transport(#[from] TransportError),

    #[error("TLS setup failed: {0}")]
    Tls(String),

    #[error("authentication setup failed: {0}")]
    Auth(String),

    #[error("cannot map OTLP request: {0}")]
    Mapping(String),
}
