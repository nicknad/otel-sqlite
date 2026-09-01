//! Integration smoke test: drives one short real E2E scenario against the
//! embedded production pipeline and validates SQLite correctness.
//!
//! Uses an ephemeral loopback port and a temporary data directory; the whole
//! run stays in-process, exercising the identical code path as
//! `otel-sqlite-e2e --scenario baseline`.

// Integration tests may assert with unwrap(); production code must not.
#![allow(clippy::unwrap_used)]

use std::sync::Arc;
use std::time::Duration;

use otel_sqlite_e2e::metrics::TelemetryHandle;
use otel_sqlite_e2e::params::{Overrides, RunContext};
use otel_sqlite_e2e::scenarios;
use otel_sqlite_e2e::server::EmbeddedServer;

/// Finds a free loopback port by binding and releasing a listener.
fn free_loopback_port() -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    let addr = listener.local_addr().expect("local address");
    drop(listener);
    format!("127.0.0.1:{}", addr.port())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn baseline_scenario_persists_every_record() {
    let telemetry = TelemetryHandle::install();

    let data_dir = tempfile::tempdir().expect("temp data dir");
    let listen = free_loopback_port();
    let server = Arc::new(
        EmbeddedServer::start(&listen, data_dir.path())
            .await
            .expect("embedded pipeline starts"),
    );

    let context = RunContext {
        endpoint: server.endpoint().to_owned(),
        db_path: server.db_path().to_path_buf(),
        seed: 7,
        body_size: 64,
        attributes_per_record: 1,
        resource_count: 2,
        warmup_secs: 1,
        request_timeout: Duration::from_secs(10),
        drain_timeout: Duration::from_secs(30),
        telemetry,
        server: Some(server.clone()),
    };

    let overrides = Overrides {
        duration_secs: Some(3),
        clients: Some(2),
        records_per_request: Some(100),
        offered_records_per_second: Some(2_000),
    };
    let steps = scenarios::baseline(&overrides);

    let report = scenarios::run_scenario(&context, "baseline-smoke", steps, false)
        .await
        .expect("scenario completes");

    assert_eq!(report.steps.len(), 1);
    let step = &report.steps[0];

    // Correctness: every generated record persisted exactly once.
    assert!(
        step.correctness.passed,
        "{:?}",
        step.correctness.failure_reasons
    );
    assert_eq!(step.correctness.missing_sequences, 0);
    assert_eq!(step.correctness.duplicate_sequences, 0);
    assert_eq!(step.record_id_mismatches, 0);
    assert_eq!(
        step.correctness.generated, step.correctness.rows_found,
        "generated == persisted for a below-capacity workload"
    );

    // No rejections expected at this modest offered rate.
    assert_eq!(step.results.records_rejected, 0);
    assert_eq!(step.queue.queue_full_events, 0);

    // Graceful shutdown drains the batcher/writer before the temp dir drops.
    drop(context);
    drop(report);
    let server = Arc::try_unwrap(server)
        .map_err(|_| "server still referenced")
        .expect("unique server reference after context drop");
    server.shutdown().await.expect("graceful shutdown");
}
