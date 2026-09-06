//! gRPC server bootstrap for the OTLP ingress.
//!
//! `serve` wires the logs and metrics services to the bounded ingest queue
//! and blocks until the OS shutdown signal arrives (or an external halt
//! signal fires, see [`serve_with_shutdown`]), with a drain deadline so
//! in-flight exports finish but a hung connection cannot delay shutdown
//! indefinitely. Enqueue policy lives in `crate::enqueue`, signal handling in
//! `crate::shutdown`.

use std::net::SocketAddr;
use std::sync::Arc;

use otel_sqlite_core::storage::CommitLedger;
use tokio::sync::watch;
use tonic::transport::{Certificate, Identity, Server, ServerTlsConfig};

use crate::IngestSender;
use crate::ServingCheck;
use crate::config::{IngressConfig, TlsConfig};
use crate::error::IngressError;
use crate::health::{new_health, spawn_health_monitor};
use crate::logs::LogsIngress;
use crate::mapping::pb::collector::logs::v1::logs_service_server::LogsServiceServer;
use crate::mapping::pb::collector::metrics::v1::metrics_service_server::MetricsServiceServer;
use crate::metrics::MetricsIngress;
use crate::shutdown::shutdown_signal;

pub async fn serve(
    config: IngressConfig,
    queue: IngestSender,
    commit: Arc<CommitLedger>,
    readiness: Arc<dyn ServingCheck>,
) -> Result<(), IngressError> {
    run(config, queue, commit, readiness, None).await
}

/// Like [`serve`], but additionally stops — with the same graceful drain and
/// deadline semantics as an OS signal — when `halt` transitions to `true`.
///
/// This is how the runtime watchdog stops ingestion after an unhealthy
/// verdict without owning or manipulating the server itself.
pub async fn serve_with_shutdown(
    config: IngressConfig,
    queue: IngestSender,
    halt: watch::Receiver<bool>,
    commit: Arc<CommitLedger>,
    readiness: Arc<dyn ServingCheck>,
) -> Result<(), IngressError> {
    run(config, queue, commit, readiness, Some(halt)).await
}

async fn run(
    config: IngressConfig,
    queue: IngestSender,
    commit: Arc<CommitLedger>,
    readiness: Arc<dyn ServingCheck>,
    external_halt: Option<watch::Receiver<bool>>,
) -> Result<(), IngressError> {
    let addr: SocketAddr = config.socket_addr()?;
    let config = Arc::new(config);

    // TLS is transport-level; mTLS (client_ca present) additionally makes
    // every connection authenticate with a CA-signed client certificate.
    let tls_config = match &config.tls {
        Some(tls) => Some(server_tls_config(tls)?),
        None => None,
    };

    // Bearer-token auth guards the OTLP services only; the health service
    // below stays unauthenticated so probes and load balancers work. The
    // interceptor is always installed (no-op without a token file) so both
    // services keep one concrete type.
    let auth_interceptor =
        crate::auth::bearer_interceptor(config.auth.as_ref().map(|auth| auth.token_file.as_path()))
            .map_err(|error| IngressError::Auth(error.to_string()))?;

    let logs_service = tonic::codegen::InterceptedService::new(
        LogsServiceServer::new(LogsIngress::new(
            queue.clone(),
            Arc::clone(&config),
            Arc::clone(&commit),
        ))
        // The OTLP spec expects receivers to accept gzip and the OpenTelemetry
        // Collector's exporters send it by default; without this the request
        // is rejected as a permanent `Unimplemented` error and data is lost.
        .accept_compressed(tonic::codec::CompressionEncoding::Gzip)
        .max_decoding_message_size(config.max_recv_msg_size),
        auth_interceptor.clone(),
    );
    let metrics_service = tonic::codegen::InterceptedService::new(
        MetricsServiceServer::new(MetricsIngress::new(queue, Arc::clone(&config), commit))
            .accept_compressed(tonic::codec::CompressionEncoding::Gzip)
            .max_decoding_message_size(config.max_recv_msg_size),
        auth_interceptor,
    );

    // grpc.health.v1: statuses track `readiness` once per second and flip to
    // NOT_SERVING on shutdown before the drain deadline starts.
    let (health_reporter, health_service) = new_health().await;

    let mut server = Server::builder().max_concurrent_streams(config.max_concurrent_streams);
    if let Some(tls_config) = tls_config {
        server = server
            .tls_config(tls_config)
            .map_err(|source| IngressError::Tls(source.to_string()))?;
    }

    let (signal_tx, mut signal_rx) = watch::channel(false);
    match external_halt {
        Some(mut external) => {
            tokio::spawn(async move {
                tokio::select! {
                    () = shutdown_signal() => {},
                    () = wait_for_halt(&mut external) => {},
                }
                let _ = signal_tx.send(true);
            });
        }
        None => {
            tokio::spawn(async move {
                shutdown_signal().await;
                let _ = signal_tx.send(true);
            });
        }
    }

    spawn_health_monitor(health_reporter, readiness, signal_rx.clone());

    let mut drain_deadline = signal_rx.clone();
    tokio::select! {
        result = server
            .add_service(health_service)
            .add_service(logs_service)
            .add_service(metrics_service)
            .serve_with_shutdown(addr, async move {
                let _ = signal_rx.changed().await;
            }) =>
        {
            result?;
        }
        () = async move {
            let _ = drain_deadline.changed().await;
            tokio::time::sleep(config.shutdown_timeout).await;
        } => {}
    }

    Ok(())
}

/// Loads the server identity (and optional client CA) from disk.
///
/// Key material lives in `Zeroizing` buffers so our copies are cleared on
/// drop. (The TLS stack necessarily retains its own copy for the server's
/// lifetime; this bounds *our* heap residue, not rustls's.)
fn server_tls_config(tls: &TlsConfig) -> Result<ServerTlsConfig, IngressError> {
    let read = |path: &std::path::Path| {
        std::fs::read(path)
            .map(zeroize::Zeroizing::new)
            .map_err(|source| {
                IngressError::Tls(format!("cannot read {}: {source}", path.display()))
            })
    };
    let cert = read(&tls.cert)?;
    let key = read(&tls.key)?;
    let mut config =
        ServerTlsConfig::new().identity(Identity::from_pem(cert.as_slice(), key.as_slice()));
    if let Some(ca_path) = &tls.client_ca {
        let ca = read(ca_path)?;
        config = config.client_ca_root(Certificate::from_pem(ca.as_slice()));
    }
    Ok(config)
}

/// Resolves when the watched flag becomes `true`, or the sender is dropped.
async fn wait_for_halt(halt: &mut watch::Receiver<bool>) {
    loop {
        if halt.changed().await.is_err() || *halt.borrow_and_update() {
            break;
        }
    }
}
