//! Docker HEALTHCHECK probe: runs `grpc.health.v1.Check` against the local
//! ingress listener and exits 0 only on `SERVING`.
//!
//! TLS-aware: when the loaded config enables `[tls]`, the probe uses https,
//! trusts the configured CA (falling back to the leaf certificate for
//! self-signed setups) and, under mTLS, presents the server's own identity
//! (its certificate carries clientAuth for exactly this purpose).
//! `OTEL_SQLITE_HEALTHCHECK_ENDPOINT` overrides the derived address. Every
//! failure prints a reason to stderr and exits 1.

use anyhow::{Context, Result};

use crate::config::Config;

pub(crate) async fn run_healthcheck() -> Result<()> {
    // Config may be unreadable in exotic setups; fall back to plain defaults
    // so the probe still works for the common no-TLS case.
    let config = Config::from_env().unwrap_or_default();
    let endpoint = std::env::var("OTEL_SQLITE_HEALTHCHECK_ENDPOINT")
        .ok()
        .filter(|endpoint| !endpoint.trim().is_empty())
        .unwrap_or_else(|| derive_healthcheck_endpoint(&config));
    let tls = build_probe_tls(&config);
    let result = probe_health(&endpoint, tls.as_ref()).await;
    match result {
        Ok(tonic_health::pb::health_check_response::ServingStatus::Serving) => {
            println!("healthcheck: SERVING ({endpoint})");
            Ok(())
        }
        Ok(status) => {
            eprintln!("healthcheck: not serving ({status:?}) at {endpoint}");
            std::process::exit(1);
        }
        Err(error) => {
            eprintln!("healthcheck: failed against {endpoint}: {error:#}");
            std::process::exit(1);
        }
    }
}

fn derive_healthcheck_endpoint(config: &Config) -> String {
    let scheme = config.tls.as_ref().map_or("http", |_| "https");
    let listen = config.listen_address.trim();
    // Dial side: a wildcard or all-interfaces bind is reachable via loopback.
    // IPv6 forms included: `[::]:port` is the v6 wildcard just as `0.0.0.0:`
    // is the v4 one, and a bare `:port` expands the same way.
    let host_port = if let Some(port) = listen.strip_prefix(':') {
        format!("127.0.0.1:{port}")
    } else if let Some(rest) = listen.strip_prefix("0.0.0.0:") {
        format!("127.0.0.1:{rest}")
    } else if let Some(rest) = listen.strip_prefix("[::]:") {
        format!("127.0.0.1:{rest}")
    } else {
        listen.to_owned()
    };
    format!("{scheme}://{host_port}")
}

#[derive(Debug)]
struct ProbeTls {
    ca_pem: Vec<u8>,
    domain_name: String,
    identity: Option<(Vec<u8>, Vec<u8>)>,
}

fn build_probe_tls(config: &Config) -> Option<ProbeTls> {
    let tls = config.tls.as_ref()?;
    let ca_path = tls.client_ca.as_ref().unwrap_or(&tls.cert);
    let ca_pem = std::fs::read(ca_path)
        .with_context(|| format!("read healthcheck CA {}", ca_path.display()))
        .ok()?;
    let domain_name = healthcheck_domain_name(&derive_healthcheck_endpoint(config));
    let identity = if tls.client_ca.is_some() {
        let cert = std::fs::read(&tls.cert).ok()?;
        let key = std::fs::read(&tls.key).ok()?;
        Some((cert, key))
    } else {
        None
    };
    Some(ProbeTls {
        ca_pem,
        domain_name,
        identity,
    })
}

/// Host part of a derived `http(s)://host:port` endpoint for TLS server-name
/// verification. Bracket-aware so IPv6 loopbacks (`[::1]:4317` → `::1`)
/// verify against their IP SAN instead of degrading to garbage (`[`) or the
/// `localhost` fallback.
fn healthcheck_domain_name(endpoint: &str) -> String {
    let host_port = endpoint
        .trim_start_matches("https://")
        .trim_start_matches("http://");
    if let Some(bracketed) = host_port.strip_prefix('[') {
        return bracketed
            .split(']')
            .next()
            .expect("str::split always yields one item")
            .to_owned();
    }
    host_port
        .split(':')
        .next()
        .expect("str::split always yields one item")
        .to_owned()
}

async fn probe_health(
    endpoint: &str,
    tls: Option<&ProbeTls>,
) -> anyhow::Result<tonic_health::pb::health_check_response::ServingStatus> {
    use tonic::transport::Endpoint;
    use tonic_health::pb::health_check_response::ServingStatus;
    use tonic_health::pb::health_client::HealthClient;

    let mut endpoint_builder = Endpoint::from_shared(endpoint.to_owned())
        .with_context(|| format!("invalid healthcheck endpoint `{endpoint}`"))?
        .connect_timeout(std::time::Duration::from_secs(2))
        .timeout(std::time::Duration::from_secs(2));
    if endpoint.starts_with("https://") {
        let tls = tls.context("https endpoint but no TLS config could be derived")?;
        let mut client_tls = tonic::transport::ClientTlsConfig::new()
            .ca_certificate(tonic::transport::Certificate::from_pem(&tls.ca_pem))
            .domain_name(tls.domain_name.clone());
        if let Some((cert, key)) = &tls.identity {
            client_tls = client_tls.identity(tonic::transport::Identity::from_pem(cert, key));
        }
        endpoint_builder = endpoint_builder
            .tls_config(client_tls)
            .with_context(|| format!("invalid TLS settings for `{endpoint}`"))?;
    }
    let channel = endpoint_builder
        .connect()
        .await
        .with_context(|| format!("cannot reach {endpoint}"))?;
    let mut client = HealthClient::new(channel);
    let response = client
        .check(tonic::Request::new(tonic_health::pb::HealthCheckRequest {
            service: String::new(),
        }))
        .await
        .context("health check rpc failed")?
        .into_inner();
    Ok(ServingStatus::try_from(response.status).unwrap_or(ServingStatus::Unknown))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config_with(listen_address: &str, tls: bool) -> Config {
        Config {
            listen_address: listen_address.to_owned(),
            tls: tls.then(|| otel_sqlite_ingress::TlsConfig {
                cert: "server.pem".into(),
                key: "server.key".into(),
                client_ca: None,
            }),
            ..Config::default()
        }
    }

    #[test]
    fn wildcard_binds_probe_loopback() {
        for listen in [":4317", "0.0.0.0:4317", "[::]:4317"] {
            assert_eq!(
                derive_healthcheck_endpoint(&config_with(listen, false)),
                "http://127.0.0.1:4317",
                "listen {listen}"
            );
            assert_eq!(
                derive_healthcheck_endpoint(&config_with(listen, true)),
                "https://127.0.0.1:4317",
                "listen {listen} with TLS"
            );
        }
    }

    #[test]
    fn loopback_binds_probe_themselves() {
        assert_eq!(
            derive_healthcheck_endpoint(&config_with("127.0.0.1:4317", false)),
            "http://127.0.0.1:4317"
        );
        assert_eq!(
            derive_healthcheck_endpoint(&config_with("[::1]:4317", false)),
            "http://[::1]:4317"
        );
    }

    #[test]
    fn domain_name_handles_ipv6_brackets() {
        assert_eq!(
            healthcheck_domain_name("https://127.0.0.1:4317"),
            "127.0.0.1"
        );
        assert_eq!(healthcheck_domain_name("https://[::1]:4317"), "::1");
        assert_eq!(
            healthcheck_domain_name("https://otel.internal:4317"),
            "otel.internal"
        );
    }
}
