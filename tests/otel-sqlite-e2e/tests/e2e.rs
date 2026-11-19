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

use otel_sqlite_e2e::db;
use otel_sqlite_e2e::metrics::TelemetryHandle;
use otel_sqlite_e2e::params::{Overrides, RunContext};
use otel_sqlite_e2e::scenarios;
use otel_sqlite_e2e::server::{EmbeddedServer, RuntimePolicy};
use otel_sqlite_ingress::mapping::pb::collector::logs::v1::ExportLogsServiceRequest;
use otel_sqlite_ingress::mapping::pb::collector::logs::v1::logs_service_client::LogsServiceClient;
use otel_sqlite_ingress::mapping::pb::common::v1::AnyValue;
use otel_sqlite_ingress::mapping::pb::common::v1::any_value::Value as AnyValueKind;
use otel_sqlite_ingress::mapping::pb::logs::v1::{
    LogRecord as ProtoLogRecord, ResourceLogs, ScopeLogs,
};
use otel_sqlite_runtime::{MaintenanceConfig, PurgeSchedule};
use otel_sqlite_test_support::wait_until;
use tonic::Request;

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

/// One OTLP export whose records carry explicit timestamps.
fn export_request(records: &[(&str, i64)]) -> ExportLogsServiceRequest {
    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            scope_logs: vec![ScopeLogs {
                log_records: records
                    .iter()
                    .map(|(body, time)| ProtoLogRecord {
                        time_unix_nano: u64::try_from(*time).expect("positive timestamp"),
                        observed_time_unix_nano: u64::try_from(*time).expect("positive timestamp"),
                        severity_text: "INFO".to_owned(),
                        body: Some(AnyValue {
                            value: Some(AnyValueKind::StringValue((*body).to_owned())),
                        }),
                        ..ProtoLogRecord::default()
                    })
                    .collect(),
                ..ScopeLogs::default()
            }],
            ..ResourceLogs::default()
        }],
    }
}

fn unix_nano_now() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock after the epoch")
            .as_nanos(),
    )
    .expect("timestamp fits i64")
}

/// The real maintenance worker's purge cadence through the real assembly.
///
/// The embedded harness used to assemble the pipeline by hand and ran neither
/// the maintenance worker nor the watchdog, so no e2e scenario could observe
/// a scheduled purge or a watchdog verdict. It now boots the real assembly
/// (`otel_sqlite::Pipeline`) with one `RuntimePolicy` knob: retention
/// pruning is scheduled every 250 ms with a one-hour window, so records
/// timestamped a week ago expire immediately while the current record
/// survives every prune. Watchdog evidence (no halt on a healthy pipeline)
/// rides along on the same run.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn maintenance_purge_cadence_runs_through_the_real_assembly() {
    let data_dir = tempfile::tempdir().expect("temp data dir");
    let listen = free_loopback_port();

    let policy = RuntimePolicy {
        maintenance: MaintenanceConfig {
            purge: Some(PurgeSchedule {
                retention: Duration::from_secs(3_600),
                interval: Duration::from_millis(250),
            }),
            ..MaintenanceConfig::default()
        },
        ..RuntimePolicy::default()
    };
    let server = EmbeddedServer::start_with(&listen, data_dir.path(), policy)
        .await
        .expect("embedded pipeline starts");

    let now = unix_nano_now();
    let week_ago = now - 7 * 24 * 3_600_000_000_000;
    let mut client = LogsServiceClient::connect(server.endpoint().to_owned())
        .await
        .expect("client connects");
    client
        .export(Request::new(export_request(&[
            ("expired-a", week_ago),
            ("expired-b", week_ago - 1),
            ("current", now),
        ])))
        .await
        .expect("export accepted");

    // Durability mode is the production default (commit): the OK response
    // already proves all three records are durable in SQLite.
    assert_eq!(
        db::count_all_log_events(server.db_path()).expect("row count"),
        3,
        "all three records must be persisted before any prune"
    );

    // The scheduled prunes delete only the expired rows.
    let db_path = server.db_path().to_path_buf();
    let purged = tokio::task::spawn_blocking(move || {
        wait_until(Duration::from_secs(15), || {
            db::count_all_log_events(&db_path).is_ok_and(|count| count == 1)
        })
    })
    .await
    .expect("purge poll task panicked");
    assert!(
        purged,
        "the real maintenance worker must purge the expired records through the shared command queue"
    );

    // Maintenance evidence on the stats surface: at least one scheduled run
    // executed through the single writer.
    let stats = server.stats().expect("pipeline alive");
    assert!(
        stats.maintenance_runs >= 1,
        "scheduled maintenance must have executed through the writer"
    );
    // Watchdog evidence: the real observer never halted this healthy pipeline.
    assert!(
        !server.watchdog_halted().expect("pipeline alive"),
        "the real watchdog must not halt a healthy pipeline"
    );

    let final_db_path = server.db_path().to_path_buf();
    server.shutdown().await.expect("graceful shutdown");
    // The current record survives the drain.
    assert_eq!(
        db::count_all_log_events(&final_db_path).expect("row count"),
        1
    );
}
