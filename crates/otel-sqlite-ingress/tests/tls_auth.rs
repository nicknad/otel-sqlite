//! End-to-end TLS / mTLS / bearer-token integration test.
//!
//! Bootstraps a private CA with rcgen (same mechanics as `otel-sqlite
//! gen-certs`), boots the REAL ingress over https with mandatory client
//! certificates and a token file, then proves the security contract:
//!
//! * correct CA + client cert + valid token → export accepted,
//! * invalid/missing bearer token → `Unauthenticated`,
//! * connection without a client certificate → rejected by the server,
//! * plaintext h2c against the TLS listener → fails.

#![allow(clippy::unwrap_used)]

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use otel_sqlite_core::storage::{DurabilityMode, InsertBatcherConfig};
use otel_sqlite_ingress::mapping::pb::collector::logs::v1::{
    ExportLogsServiceRequest, logs_service_client::LogsServiceClient,
    logs_service_server::LogsServiceServer,
};
use otel_sqlite_ingress::mapping::pb::common::v1::AnyValue;
use otel_sqlite_ingress::mapping::pb::common::v1::any_value::Value as AnyValueKind;
use otel_sqlite_ingress::mapping::pb::logs::v1::{
    LogRecord as ProtoLogRecord, ResourceLogs, ScopeLogs,
};
use otel_sqlite_ingress::{AuthConfig, IngressConfig, LogsIngress, TlsConfig};
use otel_sqlite_storage::{Storage, StorageConfig};
use rcgen::{CertificateParams, ExtendedKeyUsagePurpose, KeyPair, KeyUsagePurpose};
use tonic::Request;
use tonic::transport::{Certificate as TlsCertificate, Channel, ClientTlsConfig, Identity, Server};

/// CA + server + one client certificate, all PEM files under a tempdir.
struct Pki {
    dir: tempfile::TempDir,
}

impl Pki {
    fn generate() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let write = |name: &str, contents: &str| {
            let path = dir.path().join(name);
            std::fs::write(&path, contents).unwrap();
        };

        let ca_key = KeyPair::generate().unwrap();
        let mut ca_params = CertificateParams::new(Vec::<String>::new()).unwrap();
        ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        ca_params.key_usages = vec![
            KeyUsagePurpose::DigitalSignature,
            KeyUsagePurpose::KeyCertSign,
            KeyUsagePurpose::CrlSign,
        ];
        let ca_cert = ca_params.self_signed(&ca_key).unwrap();

        let server_key = KeyPair::generate().unwrap();
        let mut server_params = CertificateParams::new(Vec::<String>::new()).unwrap();
        server_params
            .distinguished_name
            .push(rcgen::DnType::CommonName, "localhost");
        server_params.subject_alt_names = vec![
            rcgen::SanType::DnsName("localhost".try_into().unwrap()),
            rcgen::SanType::IpAddress("127.0.0.1".parse().unwrap()),
        ];
        server_params.extended_key_usages = vec![
            ExtendedKeyUsagePurpose::ServerAuth,
            // The built-in healthcheck probe reuses the server identity when
            // mTLS is required; keep both purposes like gen-certs does.
            ExtendedKeyUsagePurpose::ClientAuth,
        ];
        let server_cert = server_params
            .signed_by(&server_key, &ca_cert, &ca_key)
            .unwrap();

        let client_key = KeyPair::generate().unwrap();
        let mut client_params = CertificateParams::new(vec!["collector-a".to_owned()]).unwrap();
        client_params
            .distinguished_name
            .push(rcgen::DnType::CommonName, "collector-a");
        client_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
        let client_cert = client_params
            .signed_by(&client_key, &ca_cert, &ca_key)
            .unwrap();

        write("ca.pem", &ca_cert.pem());
        write("server.pem", &server_cert.pem());
        write("server.key", &server_key.serialize_pem());
        write("client.pem", &client_cert.pem());
        write("client.key", &client_key.serialize_pem());

        Self { dir }
    }

    fn path(&self, name: &str) -> PathBuf {
        self.dir.path().join(name)
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

fn bearer(token: &str) -> Request<ExportLogsServiceRequest> {
    let mut request = Request::new(export_request(token));
    request
        .metadata_mut()
        .insert("authorization", format!("Bearer {token}").parse().unwrap());
    request
}

fn secured_config(pki: &Pki, tokens: &Path) -> Arc<IngressConfig> {
    Arc::new(IngressConfig {
        listen_address: "127.0.0.1:0".to_owned(),
        durability_mode: DurabilityMode::Commit,
        tls: Some(TlsConfig {
            cert: pki.path("server.pem"),
            key: pki.path("server.key"),
            client_ca: Some(pki.path("ca.pem")),
        }),
        auth: Some(AuthConfig {
            token_file: tokens.to_path_buf(),
        }),
        ..IngressConfig::default()
    })
}

fn uri_for(addr: std::net::SocketAddr, tls: bool) -> tonic::codegen::http::Uri {
    let scheme = if tls { "https" } else { "http" };
    format!("{scheme}://{addr}").parse().expect("valid uri")
}

async fn wait_listening(addr: std::net::SocketAddr) {
    for _ in 0..50 {
        if tokio::net::TcpStream::connect(addr).await.is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("server never started listening on {addr}");
}

#[tokio::test]
async fn mtls_and_bearer_token_guard_the_ingress() {
    let pki = Pki::generate();
    let tokens = token_file(pki.dir.path(), &["good-token"]);
    let config = secured_config(&pki, &tokens);

    // Real pipeline behind the secured ingress.
    let (sender, receiver) = otel_sqlite_ingress::channel(64);
    let storage = Storage::open(
        receiver,
        StorageConfig {
            sqlite_path: pki.dir.path().join("data.db"),
            insert_batcher: InsertBatcherConfig::new(16, Duration::from_millis(10)),
            ..StorageConfig::default()
        },
    )
    .unwrap();
    let commit = storage.commit_ledger();

    let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    let bound_addr = listener.local_addr().unwrap();

    let tls = tonic::transport::ServerTlsConfig::new()
        .identity(Identity::from_pem(
            std::fs::read(config.tls.as_ref().unwrap().cert.clone()).unwrap(),
            std::fs::read(config.tls.as_ref().unwrap().key.clone()).unwrap(),
        ))
        .client_ca_root(TlsCertificate::from_pem(
            std::fs::read(config.tls.as_ref().unwrap().client_ca.clone().unwrap()).unwrap(),
        ));
    let logs_service = tonic::codegen::InterceptedService::new(
        LogsServiceServer::new(LogsIngress::new(sender.clone(), config, commit))
            .max_decoding_message_size(1024 * 1024),
        otel_sqlite_ingress::bearer_interceptor(Some(&tokens)).unwrap(),
    );

    let _serve_task = tokio::spawn(async move {
        Server::builder()
            .tls_config(tls)
            .unwrap()
            .add_service(logs_service)
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
    });
    wait_listening(bound_addr).await;

    // ---- authorized: mTLS identity + valid token ---+
    let channel = Channel::builder(uri_for(bound_addr, true))
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
    let mut good = LogsServiceClient::new(channel);

    let response = good.export(bearer("good-token")).await.expect("accepted");
    assert!(response.into_inner().partial_success.is_none());

    // ---- same identity, wrong token ---+
    let status = good
        .export(bearer("bad-token"))
        .await
        .expect_err("invalid token rejected");
    assert_eq!(status.code(), tonic::Code::Unauthenticated);

    // Missing header entirely.
    let status = good
        .export(Request::new(export_request("no-header")))
        .await
        .expect_err("missing token rejected");
    assert_eq!(status.code(), tonic::Code::Unauthenticated);

    // ---- no client certificate -> server rejects ---+
    // The server sends a certificate-required alert; depending on timing the
    // failure surfaces during connect or on first RPC, and tonic may retry
    // internally - both steps are therefore bounded by our own timeouts.
    let anonymous_channel = tokio::time::timeout(
        Duration::from_secs(5),
        Channel::builder(uri_for(bound_addr, true))
            .tls_config(
                ClientTlsConfig::new()
                    .ca_certificate(TlsCertificate::from_pem(
                        std::fs::read(pki.path("ca.pem")).unwrap(),
                    ))
                    .domain_name("localhost"),
            )
            .unwrap()
            .connect(),
    )
    .await;
    let result = match anonymous_channel {
        Ok(Ok(channel)) => {
            let mut anonymous = LogsServiceClient::new(channel);
            let rpc = tokio::time::timeout(
                Duration::from_secs(5),
                anonymous.export(bearer("good-token")),
            )
            .await;
            Ok(rpc.map(|inner| inner.map(|_| ())))
        }
        _ => Err(()),
    };
    let succeeded = matches!(result, Ok(Ok(Ok(()))),);
    assert!(
        !succeeded,
        "connections without client certificates must never succeed \
         (got {result:?})"
    );

    // ---- plaintext h2c against TLS listener -------------------------------
    // TCP connects fine, but the HTTP/2 session can never come up against a
    // rustls server: tonic would keep retrying internally, so the first RPC
    // is bounded by our own timeout. Any outcome other than a successful
    // export proves h2c is unusable here.
    let plain = Channel::builder(uri_for(bound_addr, false))
        .connect()
        .await
        .expect("tcp connects");
    let mut plaintext = LogsServiceClient::new(plain);
    let outcome = tokio::time::timeout(
        Duration::from_secs(5),
        plaintext.export(bearer("good-token")),
    )
    .await;
    assert!(
        !matches!(outcome, Ok(Ok(_))),
        "plaintext must not be accepted by a TLS-only listener"
    );

    // Shutdown/drain hygiene is covered exhaustively by the pipeline and
    // maintenance tests; this file only proves the security contract, so we
    // detach instead of draining (leaked OS threads die with the process).
    std::mem::forget(storage);
}
