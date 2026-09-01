//! Security and protocol-compatibility verification (P1-3).
//!
//! Proves the OTLP ingress's security and protocol contract against the REAL
//! [`serve_with_shutdown`] wiring — gzip acceptance, message-size limits, the
//! gRPC health service, TLS/mTLS and bearer-token interception — exactly as a
//! production process would serve it:
//!
//! * server-certificate hostname/SAN validation rejects a wrong name;
//! * expired server and client certificates are rejected on validity, even
//!   when signed by a trusted CA;
//! * missing and invalid bearer tokens are rejected over the wire with
//!   `Unauthenticated` (rotation and revocation are covered deterministically
//!   by the `TokenFileVault` unit tests in `auth.rs`, which drive the same
//!   hot-reload logic the live interceptor uses — a 5 s reload interval makes
//!   a second-long integration wait untenable, so those stay at unit level);
//! * a client certificate signed by the WRONG CA is rejected;
//! * plaintext h2c against a TLS listener never completes an export;
//! * the gRPC health service stays unauthenticated while the OTLP services
//!   stay guarded, and the statuses track the readiness source;
//! * gzip-compressed OTLP requests (the OpenTelemetry Collector exporter's
//!   default) are accepted and processed;
//! * requests above `max_recv_msg_size` are rejected with `OutOfRange`;
//! * semantically malformed OTLP inputs return clean gRPC errors and never
//!   panic the server (arbitrary-byte decode is additionally fuzzed by
//!   `fuzz/logs_ingress_export` and `fuzz/metrics_ingress_export`);
//! * bearer credentials never appear in logs, metrics, gRPC status text, or
//!   the SQLite database files.
//!
//! [`serve_with_shutdown`]: otel_sqlite_ingress::serve_with_shutdown

// Integration tests may assert with unwrap(); production code must not.
#![allow(clippy::unwrap_used)]

use std::io::Write;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock, Mutex, OnceLock};
use std::time::Duration;

use metrics::{
    Counter, CounterFn, Gauge, GaugeFn, Histogram, HistogramFn, Key, KeyName, Metadata, Recorder,
    SharedString, Unit,
};
use otel_sqlite_core::storage::{CommitLedger, DurabilityMode, IngestMessage, InsertBatcherConfig};
use otel_sqlite_ingress::mapping::pb::collector::logs::v1::{
    ExportLogsServiceRequest, logs_service_client::LogsServiceClient,
};
use otel_sqlite_ingress::mapping::pb::common::v1::AnyValue;
use otel_sqlite_ingress::mapping::pb::common::v1::any_value::Value as AnyValueKind;
use otel_sqlite_ingress::mapping::pb::logs::v1::{
    LogRecord as ProtoLogRecord, ResourceLogs, ScopeLogs,
};
use otel_sqlite_ingress::{
    AuthConfig, IngestReceiver, IngestSender, IngressConfig, IngressError, ServingCheck, TlsConfig,
    channel,
};
use otel_sqlite_storage::{Storage, StorageConfig};
use rcgen::{CertificateParams, ExtendedKeyUsagePurpose, KeyPair, KeyUsagePurpose};
use tokio::sync::watch;
use tonic::transport::{Certificate as TlsCertificate, Channel, ClientTlsConfig, Identity};
use tonic::{Request, codec::CompressionEncoding, codegen::http::Uri};
use tonic_health::pb::HealthCheckRequest;
use tonic_health::pb::health_check_response::ServingStatus;
use tonic_health::pb::health_client::HealthClient;

// ---------------------------------------------------------------------------
// PKI + config helpers
// ---------------------------------------------------------------------------

/// A tempdir holding PEM material. [`Pki::generate`] builds the real CA +
/// server + client, [`Pki::generate_rogue`] a separate root that signs only a
/// client certificate — the "wrong CA" case.
struct Pki {
    dir: tempfile::TempDir,
}

impl Pki {
    fn new() -> Self {
        Self {
            dir: tempfile::tempdir().unwrap(),
        }
    }

    fn path(&self, name: &str) -> PathBuf {
        self.dir.path().join(name)
    }

    fn write(&self, name: &str, contents: &str) {
        std::fs::write(self.path(name), contents).unwrap();
    }

    fn generate_ca(name: &str) -> (rcgen::Certificate, rcgen::KeyPair) {
        let key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, name);
        params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        params.key_usages = vec![
            KeyUsagePurpose::DigitalSignature,
            KeyUsagePurpose::KeyCertSign,
            KeyUsagePurpose::CrlSign,
        ];
        let cert = params.self_signed(&key).unwrap();
        (cert, key)
    }

    fn sign_server(
        ca_cert: &rcgen::Certificate,
        ca_key: &KeyPair,
        hosts: &[&str],
    ) -> (String, String) {
        let key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, "otel-sqlite test server");
        params.subject_alt_names = hosts
            .iter()
            .map(|host| {
                if let Ok(ip) = host.parse::<std::net::IpAddr>() {
                    rcgen::SanType::IpAddress(ip)
                } else {
                    rcgen::SanType::DnsName((*host).try_into().unwrap())
                }
            })
            .collect();
        // Dual purpose like `gen-certs`: the server identity doubles as the
        // mTLS identity for the built-in healthcheck probe.
        params.extended_key_usages = vec![
            ExtendedKeyUsagePurpose::ServerAuth,
            ExtendedKeyUsagePurpose::ClientAuth,
        ];
        params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        let cert = params.signed_by(&key, ca_cert, ca_key).unwrap();
        (cert.pem(), key.serialize_pem())
    }

    fn sign_client(ca_cert: &rcgen::Certificate, ca_key: &KeyPair, name: &str) -> (String, String) {
        let key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(vec![name.to_owned()]).unwrap();
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, name);
        params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
        params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        let cert = params.signed_by(&key, ca_cert, ca_key).unwrap();
        (cert.pem(), key.serialize_pem())
    }

    /// Real PKI: a private CA signs a server (SANs `localhost`/`127.0.0.1`)
    /// and a client identity.
    fn generate() -> Self {
        let pki = Self::new();
        let (ca_cert, ca_key) = Self::generate_ca("test CA");
        let (server_pem, server_key) =
            Self::sign_server(&ca_cert, &ca_key, &["localhost", "127.0.0.1"]);
        let (client_pem, client_key) = Self::sign_client(&ca_cert, &ca_key, "collector-a");
        pki.write("ca.pem", &ca_cert.pem());
        pki.write("server.pem", &server_pem);
        pki.write("server.key", &server_key);
        pki.write("client.pem", &client_pem);
        pki.write("client.key", &client_key);
        pki
    }

    /// A DIFFERENT root CA that signs only a client certificate — the wrong-CA
    /// case. Its `ca.pem` must never appear in the server's `client_ca`.
    fn generate_rogue() -> Self {
        let pki = Self::new();
        let (ca_cert, ca_key) = Self::generate_ca("rogue CA");
        let (client_pem, client_key) = Self::sign_client(&ca_cert, &ca_key, "imposter");
        pki.write("ca.pem", &ca_cert.pem());
        pki.write("client.pem", &client_pem);
        pki.write("client.key", &client_key);
        pki
    }

    /// A PKI whose SERVER certificate is signed by the real CA but expired
    /// years ago (valid 2000-01-01 → 2001-01-01). A client that trusts the CA
    /// must still reject the identity on validity, not just on trust.
    fn generate_expired_server() -> Self {
        let pki = Self::new();
        let (ca_cert, ca_key) = Self::generate_ca("expired-server CA");
        let key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, "otel-sqlite expired server");
        params.subject_alt_names = vec![rcgen::SanType::DnsName("localhost".try_into().unwrap())];
        params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
        params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        params.not_before = rcgen::date_time_ymd(2000, 1, 1);
        params.not_after = rcgen::date_time_ymd(2001, 1, 1);
        let cert = params.signed_by(&key, &ca_cert, &ca_key).unwrap();
        pki.write("ca.pem", &ca_cert.pem());
        pki.write("server.pem", &cert.pem());
        pki.write("server.key", &key.serialize_pem());
        pki
    }

    /// A PKI whose CLIENT certificate is signed by the real CA but expired
    /// years ago — the mTLS server must refuse it at the handshake.
    fn generate_expired_client() -> Self {
        let pki = Self::new();
        let (ca_cert, ca_key) = Self::generate_ca("expired-client CA");
        let (server_pem, server_key) =
            Self::sign_server(&ca_cert, &ca_key, &["localhost", "127.0.0.1"]);
        let key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(vec!["old-client".to_owned()]).unwrap();
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, "old-client");
        params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
        params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        params.not_before = rcgen::date_time_ymd(2000, 1, 1);
        params.not_after = rcgen::date_time_ymd(2001, 1, 1);
        let client_cert = params.signed_by(&key, &ca_cert, &ca_key).unwrap();
        pki.write("ca.pem", &ca_cert.pem());
        pki.write("server.pem", &server_pem);
        pki.write("server.key", &server_key);
        pki.write("client.pem", &client_cert.pem());
        pki.write("client.key", &key.serialize_pem());
        pki
    }
}

fn token_file(dir: &Path, tokens: &[&str]) -> PathBuf {
    let path = dir.join("tokens.txt");
    std::fs::write(&path, tokens.join("\n")).unwrap();
    path
}

fn export_request(body: &str) -> ExportLogsServiceRequest {
    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            scope_logs: vec![ScopeLogs {
                log_records: vec![ProtoLogRecord {
                    severity_text: "INFO".to_owned(),
                    body: Some(AnyValue {
                        value: Some(AnyValueKind::StringValue(format!("record-{body}"))),
                    }),
                    ..ProtoLogRecord::default()
                }],
                ..ScopeLogs::default()
            }],
            ..ResourceLogs::default()
        }],
    }
}

fn bearer(token: &str, body: &str) -> Request<ExportLogsServiceRequest> {
    let mut request = Request::new(export_request(body));
    request
        .metadata_mut()
        .insert("authorization", format!("Bearer {token}").parse().unwrap());
    request
}

fn uri_for(addr: SocketAddr, tls: bool) -> Uri {
    let scheme = if tls { "https" } else { "http" };
    format!("{scheme}://{addr}").parse().expect("valid uri")
}

fn free_loopback_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    listener.local_addr().expect("local address").port()
}

async fn wait_listening(addr: SocketAddr) {
    for _ in 0..100 {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("server never started listening on {addr}");
}

/// A plaintext (no-TLS, no-auth) ingress config bound to a concrete port.
fn plain_config(port: u16, durability_mode: DurabilityMode) -> IngressConfig {
    IngressConfig {
        listen_address: format!("127.0.0.1:{port}"),
        durability_mode,
        ..IngressConfig::default()
    }
}

/// TLS + mandatory mTLS + bearer-token config bound to a concrete port.
fn secured_config(pki: &Pki, tokens: &Path, port: u16) -> IngressConfig {
    IngressConfig {
        listen_address: format!("127.0.0.1:{port}"),
        durability_mode: DurabilityMode::Enqueue,
        tls: Some(TlsConfig {
            cert: pki.path("server.pem"),
            key: pki.path("server.key"),
            client_ca: Some(pki.path("ca.pem")),
        }),
        auth: Some(AuthConfig {
            token_file: tokens.to_path_buf(),
        }),
        ..IngressConfig::default()
    }
}

/// Readiness sources for the health service.
struct AlwaysServing;
impl ServingCheck for AlwaysServing {
    fn is_serving(&self) -> bool {
        true
    }
}

struct ReadinessFlag(Arc<AtomicBool>);
impl ServingCheck for ReadinessFlag {
    fn is_serving(&self) -> bool {
        self.0.load(Ordering::Relaxed)
    }
}

// ---------------------------------------------------------------------------
// Server bootstrap helper
// ---------------------------------------------------------------------------

/// A live `serve_with_shutdown` instance on a loopback port.
struct BootedServer {
    addr: SocketAddr,
    halt: watch::Sender<bool>,
    serve_task: tokio::task::JoinHandle<Result<(), IngressError>>,
    /// Kept alive so the ingest channel stays connected; also lets tests
    /// assert what the handlers actually enqueued.
    receiver: IngestReceiver,
}

impl BootedServer {
    /// Everything the handlers enqueued so far (logs/metrics chunks and any
    /// control commands), in order.
    fn drained(&self) -> Vec<IngestMessage> {
        self.receiver.try_iter().collect()
    }

    /// Graceful shutdown: halt → drain → serve task returns.
    async fn shutdown(self) {
        let _ = self.halt.send(true);
        tokio::time::timeout(Duration::from_secs(10), self.serve_task)
            .await
            .expect("server drains within the timeout")
            .expect("serve task panicked")
            .expect("serve_with_shutdown must exit cleanly");
    }
}

/// Boots the REAL production wiring ([`serve_with_shutdown`]: gzip
/// acceptance, message-size limits, health service, interceptors) on an
/// ephemeral loopback port. The `receiver` is moved in so the ingest channel
/// stays connected for the server's lifetime.
async fn boot(
    config: IngressConfig,
    queue: IngestSender,
    receiver: IngestReceiver,
    commit: Arc<CommitLedger>,
    readiness: Arc<dyn ServingCheck>,
) -> BootedServer {
    let addr: SocketAddr = config
        .listen_address
        .parse()
        .expect("concrete listen address");
    let (halt_tx, halt_rx) = watch::channel(false);
    let serve_task = tokio::spawn(async move {
        otel_sqlite_ingress::serve_with_shutdown(config, queue, halt_rx, commit, readiness).await
    });
    wait_listening(addr).await;
    BootedServer {
        addr,
        halt: halt_tx,
        serve_task,
        receiver,
    }
}

/// A plaintext tonic client channel to the booted server.
async fn client_channel(addr: SocketAddr) -> Channel {
    Channel::from_shared(format!("http://{addr}"))
        .expect("valid endpoint")
        .connect()
        .await
        .expect("client connects")
}

/// Drives one export over TLS with the given client TLS configuration and
/// reports whether the FULL call (handshake + RPC) succeeded. A rejected
/// handshake, a connect timeout, or a failed RPC all count as `false` — the
/// common shape of every "this identity must be refused" assertion.
async fn export_over_tls(addr: SocketAddr, tls_config: ClientTlsConfig) -> bool {
    let connected = tokio::time::timeout(
        Duration::from_secs(5),
        Channel::builder(uri_for(addr, true))
            .tls_config(tls_config)
            .unwrap()
            .connect(),
    )
    .await;
    match connected {
        Ok(Ok(channel)) => {
            let mut client = LogsServiceClient::new(channel);
            let rpc = tokio::time::timeout(
                Duration::from_secs(5),
                client.export(Request::new(export_request("tls-probe"))),
            )
            .await;
            matches!(rpc, Ok(Ok(_)))
        }
        _ => false,
    }
}

// ---------------------------------------------------------------------------
// Capture substrate for the credential-leak assertions
// ---------------------------------------------------------------------------

static LOG_BUF: LazyLock<Arc<Mutex<Vec<u8>>>> = LazyLock::new(|| Arc::new(Mutex::new(Vec::new())));
static METRIC_SINK: LazyLock<Arc<Mutex<Vec<String>>>> =
    LazyLock::new(|| Arc::new(Mutex::new(Vec::new())));

#[derive(Clone)]
struct SharedWriter {
    buf: Arc<Mutex<Vec<u8>>>,
}

impl Write for SharedWriter {
    fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
        self.buf.lock().unwrap().extend_from_slice(data);
        Ok(data.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl tracing_subscriber::fmt::MakeWriter<'_> for SharedWriter {
    type Writer = SharedWriter;
    fn make_writer(&self) -> Self::Writer {
        self.clone()
    }
}

/// No-op cells: the recorder only needs to observe metric names/labels.
struct NoopCounter;
impl CounterFn for NoopCounter {
    fn increment(&self, _: u64) {}
    fn absolute(&self, _: u64) {}
}
struct NoopGauge;
impl GaugeFn for NoopGauge {
    fn increment(&self, _: f64) {}
    fn decrement(&self, _: f64) {}
    fn set(&self, _: f64) {}
}
struct NoopHistogram;
impl HistogramFn for NoopHistogram {
    fn record(&self, _: f64) {}
}

/// Captures every metric key and label into [`METRIC_SINK`].
struct CredentialScrubRecorder {
    sink: Arc<Mutex<Vec<String>>>,
}

impl CredentialScrubRecorder {
    fn record(&self, key: &Key) {
        let mut sink = self.sink.lock().unwrap();
        sink.push(key.name().to_owned());
        for label in key.labels() {
            sink.push(label.key().to_owned());
            sink.push(label.value().to_owned());
        }
    }
}

impl Recorder for CredentialScrubRecorder {
    fn describe_counter(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn describe_gauge(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn describe_histogram(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}

    fn register_counter(&self, key: &Key, _: &Metadata<'_>) -> Counter {
        self.record(key);
        Counter::from_arc(Arc::new(NoopCounter))
    }
    fn register_gauge(&self, key: &Key, _: &Metadata<'_>) -> Gauge {
        self.record(key);
        Gauge::from_arc(Arc::new(NoopGauge))
    }
    fn register_histogram(&self, key: &Key, _: &Metadata<'_>) -> Histogram {
        self.record(key);
        Histogram::from_arc(Arc::new(NoopHistogram))
    }
}

/// Installs the global tracing subscriber and metrics recorder exactly once.
/// Both are process-global; the `OnceLock` guards make repeated calls no-ops.
fn install_capture_subscribers() {
    static TRACING: OnceLock<()> = OnceLock::new();
    static METRICS: OnceLock<()> = OnceLock::new();
    TRACING.get_or_init(|| {
        let _ = tracing_subscriber::fmt()
            .with_writer(SharedWriter {
                buf: Arc::clone(&*LOG_BUF),
            })
            .with_ansi(false)
            .with_max_level(tracing::Level::TRACE)
            .try_init();
    });
    METRICS.get_or_init(|| {
        metrics::set_global_recorder(CredentialScrubRecorder {
            sink: Arc::clone(&*METRIC_SINK),
        })
        .expect("metrics recorder installed once");
    });
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn server_certificate_hostname_san_validation_rejects_wrong_names() {
    let pki = Pki::generate();
    let config = IngressConfig {
        listen_address: format!("127.0.0.1:{}", free_loopback_port()),
        durability_mode: DurabilityMode::Enqueue,
        tls: Some(TlsConfig {
            cert: pki.path("server.pem"),
            key: pki.path("server.key"),
            client_ca: None,
        }),
        ..IngressConfig::default()
    };
    let (queue, receiver) = channel(64);
    let server = boot(
        config,
        queue,
        receiver,
        Arc::new(CommitLedger::new()),
        Arc::new(AlwaysServing),
    )
    .await;
    let ca = TlsCertificate::from_pem(std::fs::read(pki.path("ca.pem")).unwrap());

    // The correct SAN (`localhost`) verifies and exports succeed.
    let good = Channel::builder(uri_for(server.addr, true))
        .tls_config(
            ClientTlsConfig::new()
                .ca_certificate(ca.clone())
                .domain_name("localhost"),
        )
        .unwrap()
        .connect()
        .await
        .expect("valid hostname connects");
    let mut good_client = LogsServiceClient::new(good);
    let response = good_client
        .export(Request::new(export_request("san-ok")))
        .await
        .expect("export with the correct SAN succeeds");
    assert!(response.into_inner().partial_success.is_none());

    // A hostname the certificate does NOT cover must fail verification,
    // either during connect or on the first RPC.
    let succeeded = export_over_tls(
        server.addr,
        ClientTlsConfig::new()
            .ca_certificate(ca)
            .domain_name("attacker.invalid"),
    )
    .await;
    assert!(
        !succeeded,
        "a client using a hostname absent from the server SANs must never export"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn missing_and_invalid_bearer_tokens_are_rejected_over_the_wire() {
    let pki = Pki::generate();
    let tokens = token_file(pki.dir.path(), &["good-token"]);
    let config = IngressConfig {
        listen_address: format!("127.0.0.1:{}", free_loopback_port()),
        durability_mode: DurabilityMode::Enqueue,
        auth: Some(AuthConfig {
            token_file: tokens.clone(),
        }),
        ..IngressConfig::default()
    };
    let (queue, receiver) = channel(64);
    let server = boot(
        config,
        queue,
        receiver,
        Arc::new(CommitLedger::new()),
        Arc::new(AlwaysServing),
    )
    .await;

    let mut client = LogsServiceClient::new(client_channel(server.addr).await);

    // Valid token: accepted.
    let response = client
        .export(bearer("good-token", "valid"))
        .await
        .expect("valid token accepted");
    assert!(response.into_inner().partial_success.is_none());

    // Invalid token: rejected and the presented value is not echoed back.
    let status = client
        .export(bearer("bad-token", "invalid"))
        .await
        .expect_err("invalid token rejected");
    assert_eq!(status.code(), tonic::Code::Unauthenticated);
    assert!(
        !status.message().contains("bad-token"),
        "the gRPC status must not echo the presented token"
    );

    // Missing header entirely.
    let status = client
        .export(Request::new(export_request("missing")))
        .await
        .expect_err("missing token rejected");
    assert_eq!(status.code(), tonic::Code::Unauthenticated);

    // Rotation / revocation semantics live in the `TokenFileVault` unit tests
    // (auth.rs): the interceptor here calls the exact same `verify` path.

    server.shutdown().await;
}

#[tokio::test]
async fn client_certificate_signed_by_the_wrong_ca_is_rejected() {
    let pki = Pki::generate(); // real CA the server trusts
    let rogue = Pki::generate_rogue(); // unrelated root signs the client

    let config = IngressConfig {
        listen_address: format!("127.0.0.1:{}", free_loopback_port()),
        durability_mode: DurabilityMode::Enqueue,
        tls: Some(TlsConfig {
            cert: pki.path("server.pem"),
            key: pki.path("server.key"),
            client_ca: Some(pki.path("ca.pem")),
        }),
        ..IngressConfig::default()
    };
    let (queue, receiver) = channel(64);
    let server = boot(
        config,
        queue,
        receiver,
        Arc::new(CommitLedger::new()),
        Arc::new(AlwaysServing),
    )
    .await;

    // The client trusts the real CA (for the server cert) but presents an
    // identity signed by the rogue CA. The server must refuse the handshake.
    let succeeded = export_over_tls(
        server.addr,
        ClientTlsConfig::new()
            .ca_certificate(TlsCertificate::from_pem(
                std::fs::read(pki.path("ca.pem")).unwrap(),
            ))
            .domain_name("localhost")
            .identity(Identity::from_pem(
                std::fs::read(rogue.path("client.pem")).unwrap(),
                std::fs::read(rogue.path("client.key")).unwrap(),
            )),
    )
    .await;
    assert!(
        !succeeded,
        "a client certificate signed by an untrusted CA must never connect"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn expired_server_certificate_is_rejected_by_clients() {
    let pki = Pki::generate_expired_server();
    let config = IngressConfig {
        listen_address: format!("127.0.0.1:{}", free_loopback_port()),
        durability_mode: DurabilityMode::Enqueue,
        tls: Some(TlsConfig {
            cert: pki.path("server.pem"),
            key: pki.path("server.key"),
            client_ca: None,
        }),
        ..IngressConfig::default()
    };
    let (queue, receiver) = channel(64);
    let server = boot(
        config,
        queue,
        receiver,
        Arc::new(CommitLedger::new()),
        Arc::new(AlwaysServing),
    )
    .await;

    // The client trusts the CA, but the leaf is expired (valid only during
    // 2000-2001): rustls must reject it on validity, not trust.
    let succeeded = export_over_tls(
        server.addr,
        ClientTlsConfig::new()
            .ca_certificate(TlsCertificate::from_pem(
                std::fs::read(pki.path("ca.pem")).unwrap(),
            ))
            .domain_name("localhost"),
    )
    .await;
    assert!(
        !succeeded,
        "an expired server certificate must never be trusted by a client"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn expired_client_certificate_is_rejected_by_mtls() {
    let pki = Pki::generate_expired_client();
    let config = IngressConfig {
        listen_address: format!("127.0.0.1:{}", free_loopback_port()),
        durability_mode: DurabilityMode::Enqueue,
        tls: Some(TlsConfig {
            cert: pki.path("server.pem"),
            key: pki.path("server.key"),
            client_ca: Some(pki.path("ca.pem")),
        }),
        ..IngressConfig::default()
    };
    let (queue, receiver) = channel(64);
    let server = boot(
        config,
        queue,
        receiver,
        Arc::new(CommitLedger::new()),
        Arc::new(AlwaysServing),
    )
    .await;

    // The client presents an identity signed by the trusted CA, but it
    // expired years ago: the mTLS handshake must refuse it.
    let succeeded = export_over_tls(
        server.addr,
        ClientTlsConfig::new()
            .ca_certificate(TlsCertificate::from_pem(
                std::fs::read(pki.path("ca.pem")).unwrap(),
            ))
            .domain_name("localhost")
            .identity(Identity::from_pem(
                std::fs::read(pki.path("client.pem")).unwrap(),
                std::fs::read(pki.path("client.key")).unwrap(),
            )),
    )
    .await;
    assert!(
        !succeeded,
        "an expired client certificate must never pass the mTLS handshake"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn plaintext_h2c_never_completes_against_a_tls_listener() {
    let pki = Pki::generate();
    let config = IngressConfig {
        listen_address: format!("127.0.0.1:{}", free_loopback_port()),
        durability_mode: DurabilityMode::Enqueue,
        tls: Some(TlsConfig {
            cert: pki.path("server.pem"),
            key: pki.path("server.key"),
            client_ca: None,
        }),
        ..IngressConfig::default()
    };
    let (queue, receiver) = channel(64);
    let server = boot(
        config,
        queue,
        receiver,
        Arc::new(CommitLedger::new()),
        Arc::new(AlwaysServing),
    )
    .await;

    // TCP connects, but an HTTP/2 session can never come up against rustls:
    // any outcome other than a completed export proves h2c is unusable.
    let plain = Channel::builder(uri_for(server.addr, false))
        .connect()
        .await
        .expect("tcp connects");
    let mut client = LogsServiceClient::new(plain);
    let outcome = tokio::time::timeout(
        Duration::from_secs(5),
        client.export(Request::new(export_request("plaintext"))),
    )
    .await;
    assert!(
        !matches!(outcome, Ok(Ok(_))),
        "plaintext must not be accepted by a TLS-only listener"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn health_stays_unauthenticated_under_token_auth_and_tracks_readiness() {
    let pki = Pki::generate();
    let tokens = token_file(pki.dir.path(), &["good-token"]);
    let config = secured_config(&pki, &tokens, free_loopback_port());
    let flag = Arc::new(AtomicBool::new(true));
    let (queue, receiver) = channel(64);
    let server = boot(
        config,
        queue,
        receiver,
        Arc::new(CommitLedger::new()),
        Arc::new(ReadinessFlag(Arc::clone(&flag))),
    )
    .await;

    // Under mTLS the probe must present a valid client identity, but NO
    // bearer token: the health service stays deliberately unauthenticated.
    let channel = Channel::builder(uri_for(server.addr, true))
        .tls_config(
            ClientTlsConfig::new()
                .ca_certificate(TlsCertificate::from_pem(
                    std::fs::read(pki.path("ca.pem")).unwrap(),
                ))
                .domain_name("localhost")
                .identity(Identity::from_pem(
                    std::fs::read(pki.path("client.pem")).unwrap(),
                    std::fs::read(pki.path("client.key")).unwrap(),
                )),
        )
        .unwrap()
        .connect()
        .await
        .expect("mTLS client connects");

    let mut health = HealthClient::new(channel.clone());
    let status = health
        .check(Request::new(HealthCheckRequest {
            service: String::new(),
        }))
        .await
        .expect("health check without a token responds")
        .into_inner()
        .status;
    assert_eq!(
        ServingStatus::try_from(status).unwrap(),
        ServingStatus::Serving,
        "health must remain unauthenticated while OTLP stays guarded"
    );

    // Same connection, no token: the OTLP export is still refused.
    let mut logs = LogsServiceClient::new(channel);
    let err = logs
        .export(Request::new(export_request("no-token")))
        .await
        .expect_err("OTLP without a token is rejected even when health is open");
    assert_eq!(err.code(), tonic::Code::Unauthenticated);

    // Per-service health names track the same state.
    let status = health
        .check(Request::new(HealthCheckRequest {
            service: "opentelemetry.proto.collector.logs.v1.LogsService".to_owned(),
        }))
        .await
        .expect("logs service health responds")
        .into_inner()
        .status;
    assert_eq!(
        ServingStatus::try_from(status).unwrap(),
        ServingStatus::Serving
    );

    // The readiness source drives the status within one poll interval.
    flag.store(false, Ordering::Relaxed);
    let flipped_off = tokio::time::timeout(Duration::from_secs(4), async {
        loop {
            let status = health
                .check(Request::new(HealthCheckRequest {
                    service: String::new(),
                }))
                .await
                .expect("health check responds")
                .into_inner()
                .status;
            if ServingStatus::try_from(status).unwrap() == ServingStatus::NotServing {
                break;
            }
            tokio::time::sleep(Duration::from_millis(150)).await;
        }
    })
    .await;
    assert!(
        flipped_off.is_ok(),
        "health must flip NOT_SERVING when the pipeline dies"
    );

    flag.store(true, Ordering::Relaxed);
    let flipped_on = tokio::time::timeout(Duration::from_secs(4), async {
        loop {
            let status = health
                .check(Request::new(HealthCheckRequest {
                    service: String::new(),
                }))
                .await
                .expect("health check responds")
                .into_inner()
                .status;
            if ServingStatus::try_from(status).unwrap() == ServingStatus::Serving {
                break;
            }
            tokio::time::sleep(Duration::from_millis(150)).await;
        }
    })
    .await;
    assert!(
        flipped_on.is_ok(),
        "health must return to SERVING with a healthy pipeline"
    );

    // Shutdown: the halt path flips everything NOT_SERVING and closes the
    // listener. The mid-drain NOT_SERVING transition itself is asserted
    // deterministically by the `health` unit test
    // (`monitor_flips_statuses_with_the_readiness_source`).
    let addr = server.addr;
    server.shutdown().await;
    assert!(
        tokio::net::TcpStream::connect(addr).await.is_err(),
        "the listener must be gone after graceful shutdown"
    );
}

#[tokio::test]
async fn gzip_compressed_otlp_requests_are_accepted_and_processed() {
    let config = plain_config(free_loopback_port(), DurabilityMode::Enqueue);
    let (queue, receiver) = channel(64);
    let server = boot(
        config,
        queue,
        receiver,
        Arc::new(CommitLedger::new()),
        Arc::new(AlwaysServing),
    )
    .await;

    // The OpenTelemetry Collector's OTLP exporter compresses with gzip by
    // default; the real serve wiring accepts it via `accept_compressed`.
    let channel = client_channel(server.addr).await;
    let mut client = LogsServiceClient::new(channel).send_compressed(CompressionEncoding::Gzip);
    let response = client
        .export(Request::new(export_request("gzip-body")))
        .await
        .expect("gzip-compressed export accepted");
    assert!(response.into_inner().partial_success.is_none());

    // Prove the decompressed request was actually mapped and handed off.
    let enqueued_records: usize = server
        .drained()
        .iter()
        .map(|message| match message {
            IngestMessage::Logs(chunk) => chunk.records.len(),
            _ => 0,
        })
        .sum();
    assert!(
        enqueued_records >= 1,
        "the decompressed request must reach the ingest queue"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn requests_above_max_recv_msg_size_are_rejected_with_out_of_range() {
    let config = IngressConfig {
        listen_address: format!("127.0.0.1:{}", free_loopback_port()),
        durability_mode: DurabilityMode::Enqueue,
        max_recv_msg_size: 4096,
        ..IngressConfig::default()
    };
    let (queue, receiver) = channel(64);
    let server = boot(
        config,
        queue,
        receiver,
        Arc::new(CommitLedger::new()),
        Arc::new(AlwaysServing),
    )
    .await;

    let mut client = LogsServiceClient::new(client_channel(server.addr).await);

    // A request whose protobuf encoding exceeds the limit is refused at the
    // transport boundary.
    let oversized = export_request(&"x".repeat(16 * 1024));
    let status = client
        .export(Request::new(oversized))
        .await
        .expect_err("oversized request rejected");
    assert_eq!(
        status.code(),
        tonic::Code::OutOfRange,
        "tonic maps an over-limit message to OutOfRange, got {status:?}"
    );

    // The server stays healthy and a small request still succeeds.
    let response = client
        .export(Request::new(export_request("small")))
        .await
        .expect("a request within the limit is accepted after a rejected one");
    assert!(response.into_inner().partial_success.is_none());

    server.shutdown().await;
}

#[tokio::test]
async fn malformed_otlp_payloads_return_clean_errors_and_never_panic_the_server() {
    let config = plain_config(free_loopback_port(), DurabilityMode::Enqueue);
    let (queue, receiver) = channel(64);
    let server = boot(
        config,
        queue,
        receiver,
        Arc::new(CommitLedger::new()),
        Arc::new(AlwaysServing),
    )
    .await;

    let mut client = LogsServiceClient::new(client_channel(server.addr).await);

    // Semantically malformed payload: a wrong-length trace id must surface as
    // INVALID_ARGUMENT (all-or-nothing, nothing enqueued), never a panic.
    // Arbitrary-byte protobuf decode is additionally fuzzed by
    // `fuzz/logs_ingress_export` / `fuzz/metrics_ingress_export`, which assert
    // the no-panic invariant across the whole decode→map→enqueue path.
    let malformed = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            scope_logs: vec![ScopeLogs {
                log_records: vec![ProtoLogRecord {
                    trace_id: vec![1, 2],
                    ..ProtoLogRecord::default()
                }],
                ..ScopeLogs::default()
            }],
            ..ResourceLogs::default()
        }],
    };
    let status = client
        .export(Request::new(malformed))
        .await
        .expect_err("malformed request rejected");
    assert_eq!(status.code(), tonic::Code::InvalidArgument);

    // The server must keep serving after hostile input.
    let response = client
        .export(Request::new(export_request("after-malformed")))
        .await
        .expect("server keeps serving after malformed input");
    assert!(response.into_inner().partial_success.is_none());

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bearer_credentials_never_escape_into_logs_metrics_status_or_database() {
    install_capture_subscribers();

    // The whole point of this test is that the credential never leaks into
    // any observable sink, so it is deliberately a fake, clearly-labelled
    // value, not a real secret.
    // nosemgrep: generic.hardcoded-secret
    let token = "super-secret-token-9f2c";
    let marker = "credential-scan-marker-body";
    let dir = tempfile::tempdir().unwrap();
    let tokens = token_file(dir.path(), &[token]);
    let addr = format!("127.0.0.1:{}", free_loopback_port());

    // Real durable pipeline behind the secured ingress so committed records
    // (and the WAL) land on disk before we scan them.
    let (queue, receiver) = channel(64);
    let mut storage = Storage::open(
        receiver,
        StorageConfig {
            sqlite_path: dir.path().join("creds.db"),
            insert_batcher: InsertBatcherConfig::new(16, Duration::from_millis(10)),
            ..StorageConfig::default()
        },
    )
    .unwrap();
    let commit = storage.commit_ledger();
    let config = IngressConfig {
        listen_address: addr.clone(),
        durability_mode: DurabilityMode::Commit,
        auth: Some(AuthConfig { token_file: tokens }),
        ..IngressConfig::default()
    };

    let (halt_tx, halt_rx) = watch::channel(false);
    let serve_task = tokio::spawn(async move {
        otel_sqlite_ingress::serve_with_shutdown(
            config,
            queue,
            halt_rx,
            commit,
            Arc::new(AlwaysServing),
        )
        .await
    });
    let addr: SocketAddr = addr.parse().unwrap();
    wait_listening(addr).await;

    let mut client = LogsServiceClient::new(client_channel(addr).await);

    // A valid export whose commit is durable before the response returns.
    let response = client
        .export(bearer(token, marker))
        .await
        .expect("authorized export accepted");
    assert!(response.into_inner().partial_success.is_none());

    // An unauthorized one, whose status text must not echo the token.
    let presented = "attacker-token-77";
    let status = client
        .export(bearer(presented, "unauthorized"))
        .await
        .expect_err("unauthorized export rejected");
    assert_eq!(status.code(), tonic::Code::Unauthenticated);
    assert!(
        !status.message().contains(presented) && !status.message().contains(token),
        "gRPC status must not echo any presented credential"
    );

    // Graceful shutdown, then scan the closed database files.
    let _ = halt_tx.send(true);
    tokio::time::timeout(Duration::from_secs(10), serve_task)
        .await
        .expect("serve drains")
        .expect("serve task joined")
        .expect("serve_with_shutdown exits cleanly");
    storage
        .join_timeout(Duration::from_secs(30))
        .expect("storage drains");

    let mut found_marker = false;
    // Scan only the SQLite database and its sidecars, not the token file
    // (which legitimately contains the credential).
    for name in ["creds.db", "creds.db-wal", "creds.db-shm"] {
        let path = dir.path().join(name);
        if !path.is_file() {
            continue;
        }
        let bytes = std::fs::read(&path).unwrap();
        let text = String::from_utf8_lossy(&bytes);
        if text.contains(marker) {
            found_marker = true;
        }
        assert!(
            !text.contains(token),
            "bearer token leaked into {}",
            path.display()
        );
    }
    assert!(
        found_marker,
        "sanity check: the committed marker must be present in the database files"
    );

    let logs = String::from_utf8_lossy(&LOG_BUF.lock().unwrap()).into_owned();
    assert!(
        !logs.contains(token),
        "bearer token leaked into tracing logs"
    );

    let metric_labels = METRIC_SINK.lock().unwrap();
    assert!(
        !metric_labels.iter().any(|entry| entry.contains(token)),
        "bearer token leaked into metric names or labels"
    );
}
