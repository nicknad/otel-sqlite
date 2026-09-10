//! otel-sqlite-stack-verify: end-to-end verifier for the full container
//! stack in `tests/docker/compose.stack-e2e.yml`:
//!
//! ```text
//!   verify --HTTP--> nginx (frontend + reverse proxy)
//!                      |-- /        -> static frontend
//!                      `-- /api/*   -> backend container
//!   nginx access log --> otelcol filelog ─┐
//!   backend OTLP/HTTP  -> otelcol otlp   ─┴-> otel-sqlite sidecar -> SQLite
//!                                                            │
//!                                                            └-> explorer (read-only UI)
//! ```
//!
//! Drives HTTP traffic through nginx, then polls the sink's SQLite
//! database (shared volume, read-only) until both telemetry sources have
//! landed: nginx access-log lines via the collector's `filelog` receiver,
//! and backend log records exported over OTLP.
//!
//! Runs inside the compose network as the `verify` service; its exit code
//! is the stack's verdict (`docker compose up --exit-code-from verify`).

// Verifier reports progress and results on stdout by design; assertions
// fail via bail!() with context instead of panicking.
#![allow(clippy::print_stdout, clippy::print_stderr)]

use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

use anyhow::{Context as _, Result, bail};

/// Marker embedded verbatim in the frontend page; proves the response body
// came from the static site and not an error page.
const FRONTEND_MARKER: &str = "otel-sqlite-stack-e2e";

/// Marker from the explorer's `<title>`; proves the rendered page came from
/// the explorer UI (and not an error response).
const EXPLORER_MARKER: &str = "Log Explorer";

#[derive(Debug, Clone)]
struct Settings {
    /// Base URL of the nginx frontend ("http://nginx" inside compose).
    frontend_url: String,
    /// Base URL of the explorer UI ("http://explorer:8080/" inside compose).
    explorer_url: String,
    /// Total requests driven through nginx (alternating / and /api/N).
    requests: usize,
    /// Sink database on the shared volume.
    db_path: PathBuf,
    /// How long to wait for all records to reach SQLite before failing.
    settle_timeout: Duration,
}

impl Settings {
    fn from_env() -> Self {
        Self {
            frontend_url: env_or("STACK_FRONTEND_URL", "http://nginx"),
            explorer_url: env_or("STACK_EXPLORER_URL", "http://explorer:8080/"),
            requests: env_parse("STACK_REQUESTS", 24).max(4),
            db_path: PathBuf::from(env_or("STACK_DB_PATH", "/data/otel-logs.db")),
            settle_timeout: Duration::from_secs(env_parse("STACK_SETTLE_SECS", 90)),
        }
    }
}

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key)
        .ok()
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| default.to_owned())
}

fn env_parse<T: FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

/// `(host, port, path)` split out of an `http://host[:port][/path]` URL.
fn split_url(url: &str) -> Result<(String, u16, String)> {
    let rest = url
        .strip_prefix("http://")
        .with_context(|| format!("only plain-http URLs are supported, got {url}"))?;
    let (authority, path) = match rest.split_once('/') {
        Some((authority, path)) => (authority, format!("/{path}")),
        None => (rest, "/".to_owned()),
    };
    let (host, port) = match authority.rsplit_once(':') {
        Some((host, port)) => (
            host.to_owned(),
            port.parse::<u16>()
                .with_context(|| format!("invalid port in {url}"))?,
        ),
        None => (authority.to_owned(), 80),
    };
    Ok((host, port, path))
}

async fn wait_ready(url: &str, timeout: Duration) -> Result<()> {
    let (host, port, _) = split_url(url)?;
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if tokio::net::TcpStream::connect((host.as_str(), port))
            .await
            .is_ok()
        {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            bail!("{url} not reachable within {timeout:?}");
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// Minimal HTTP/1.1 GET over a raw TCP stream (`Connection: close`, so the
/// response ends at EOF) - no HTTP client dependency needed for this.
async fn http_get(url: &str) -> Result<(u16, String)> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let (host, port, path) = split_url(url)?;
    let mut stream = tokio::net::TcpStream::connect((host.as_str(), port))
        .await
        .with_context(|| format!("connect {url}"))?;
    let request = format!(
        "GET {path} HTTP/1.1\r\nHost: {host}\r\nUser-Agent: otel-sqlite-stack-verify\r\nConnection: close\r\n\r\n"
    );
    stream.write_all(request.as_bytes()).await?;
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).await?;
    let text = String::from_utf8_lossy(&raw).into_owned();
    let status = text
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse::<u16>().ok())
        .with_context(|| format!("malformed HTTP response from {url}: {text:.80}"))?;
    let body = text
        .split_once("\r\n\r\n")
        .map(|(_, body)| body.to_owned())
        .unwrap_or_default();
    Ok((status, body))
}

/// One request with retries; constrained containers may briefly stall.
async fn get_with_retry(url: &str, attempts: usize) -> Result<(u16, String)> {
    let mut last = None;
    for _ in 0..attempts {
        match http_get(url).await {
            Ok(pair) => return Ok(pair),
            Err(error) => last = Some(error),
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    Err(last.unwrap_or_else(|| anyhow::anyhow!("request failed without an error")))
}

#[derive(Debug, Clone, Copy)]
struct TrafficStats {
    frontend_hits: usize,
    api_hits: usize,
}

async fn drive_traffic(settings: &Settings) -> Result<TrafficStats> {
    let mut traffic = TrafficStats {
        frontend_hits: 0,
        api_hits: 0,
    };
    for index in 0..settings.requests {
        // Alternate: half the traffic exercises the static frontend, half
        // the reverse-proxied backend API - each gets a unique path.
        if index % 2 == 0 {
            let url = settings.frontend_url.clone();
            let (code, body) = get_with_retry(&url, 3).await?;
            if code != 200 || !body.contains(FRONTEND_MARKER) {
                bail!("frontend GET returned status {code}, marker missing");
            }
            traffic.frontend_hits += 1;
        } else {
            let url = format!("{}/api/orders/{index}", settings.frontend_url);
            let (code, body) = get_with_retry(&url, 3).await?;
            if code != 200 || !body.contains("\"status\":\"ok\"") {
                bail!("api GET {url} returned status {code}, unexpected body");
            }
            traffic.api_hits += 1;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Ok(traffic)
}

#[derive(Debug, Clone, Copy)]
struct SinkCounts {
    backend: i64,
    nginx: i64,
    zero_observed: i64,
    total: i64,
}

/// Read-only snapshot of what has landed in the sink database so far.
fn query_counts(db_path: &std::path::Path) -> Result<SinkCounts> {
    let connection =
        rusqlite::Connection::open_with_flags(db_path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .with_context(|| format!("open {} read-only", db_path.display()))?;

    let count = |sql: &str| -> Result<i64> {
        Ok(connection.query_row(sql, [], |row| row.get::<_, i64>(0))?)
    };

    Ok(SinkCounts {
        // Backend records carry resource attribute service.name=stack-backend.
        backend: count("SELECT COUNT(*) FROM logs WHERE service_name = 'stack-backend'")?,
        // filelog receiver tags each line with the source file attribute.
        nginx: count("SELECT COUNT(*) FROM logs WHERE attributes_json LIKE '%log.file.name%'")?,
        // Both telemetry producers stamp observed time; the sink must never
        // store zeros (see backend.py / otelcol timestamp parsing).
        zero_observed: count("SELECT COUNT(*) FROM log_event WHERE observed_timestamp_ns = 0")?,
        total: count("SELECT COUNT(*) FROM log_event")?,
    })
}

async fn await_persistence(settings: &Settings, stats: TrafficStats) -> Result<SinkCounts> {
    let db_path = settings.db_path.clone();
    let expected_backend = i64::try_from(stats.api_hits)?;
    let expected_nginx = i64::try_from(stats.frontend_hits + stats.api_hits)?;
    let deadline = tokio::time::Instant::now() + settings.settle_timeout;

    loop {
        let counts = {
            let db_path = db_path.clone();
            tokio::task::spawn_blocking(move || query_counts(&db_path)).await??
        };

        let backend_ok = counts.backend >= expected_backend;
        let nginx_ok = counts.nginx >= expected_nginx;
        if backend_ok && nginx_ok && counts.zero_observed == 0 {
            return Ok(counts);
        }

        if tokio::time::Instant::now() >= deadline {
            bail!(
                "sink did not converge within {:?}\n\
                 expected: backend >= {expected_backend}, nginx >= {expected_nginx}, zero-observed == 0\n\
                 observed: {:?}",
                settings.settle_timeout,
                counts
            );
        }
        println!(
            "waiting for sink... backend {}/{} nginx {}/{} total {}",
            counts.backend, expected_backend, counts.nginx, expected_nginx, counts.total
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let settings = Settings::from_env();
    println!(
        "stack verify: {} requests via {}, explorer {}, db {}",
        settings.requests,
        settings.frontend_url,
        settings.explorer_url,
        settings.db_path.display()
    );

    wait_ready(&settings.frontend_url, Duration::from_secs(60)).await?;
    println!("frontend reachable");

    let stats = drive_traffic(&settings).await?;
    println!(
        "traffic driven: {} frontend hits, {} api hits",
        stats.frontend_hits, stats.api_hits
    );

    let counts = await_persistence(&settings, stats).await?;

    println!("PASS: nginx access logs persisted ({}) ", counts.nginx);
    println!("PASS: backend OTLP logs persisted ({})", counts.backend);
    println!(
        "PASS: no zero observed_timestamp_ns rows across {} events",
        counts.total
    );

    // Explorer sidecar: must be up and render its index page (HTTP 200)
    // against the now-settled sink database.
    let (code, body) = get_with_retry(&settings.explorer_url, 5).await?;
    if code != 200 || !body.contains(EXPLORER_MARKER) {
        bail!(
            "explorer GET {} returned status {code}, marker missing",
            settings.explorer_url
        );
    }
    println!(
        "PASS: explorer index served with 200 ({})",
        settings.explorer_url
    );

    Ok(())
}
