//! Fuzz target: the OTLP logs export handler.
//!
//! Exercises the full ingress path for untrusted bytes: protobuf decode
//! (inside prost/tonic), record-count validation, proto->model mapping and
//! channel hand-off, exactly as reached via
//! `/opentelemetry.proto.collector.logs.v1.LogsService/Export`.
//!
//! Crash invariants: any input that decodes must never panic; a rejected
//! request surfaces as `Status::invalid_argument`, never as an abort.

#![no_main]

use std::sync::{Arc, LazyLock};

use libfuzzer_sys::fuzz_target;
use otel_sqlite_core::storage::{CommitLedger, DurabilityMode, IngestMessage};
use otel_sqlite_ingress::pb::collector::logs::v1::{
    ExportLogsServiceRequest, logs_service_server::LogsService,
};
use otel_sqlite_ingress::{IngressConfig, LogsIngress};
use prost::Message as _;

/// One shared current-thread runtime for all iterations: `export()` is async
/// only because of the tonic service trait; the handler body is synchronous.
static RUNTIME: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime for fuzz harness")
});

fuzz_target!(|data: &[u8]| {
    // Undecodable input dies inside tonic with `Status::internal` on the
    // transport; here it is simply not a valid iteration.
    let Ok(request) = ExportLogsServiceRequest::decode(data) else {
        return;
    };

    let (sender, receiver) = otel_sqlite_ingress::channel(16);
    let config = Arc::new(IngressConfig {
        max_records_per_request: 100_000,
        // No storage pipeline runs inside the harness: in the default
        // `Commit` mode every accepted request would wait forever on a
        // durable-ack that never arrives. Acknowledge on enqueue instead so
        // the decode->validate->map->enqueue surface is what is exercised.
        durability_mode: DurabilityMode::Enqueue,
        ..IngressConfig::default()
    });
    let ingress = LogsIngress::new(sender, config, Arc::new(CommitLedger::new()));

    let response = RUNTIME.block_on(ingress.export(tonic::Request::new(request)));

    // All-or-nothing admission invariant: an accepted request carries no
    // partial success, and a rejected request surfaces as an error status with
    // nothing enqueued — so a retrying client can never double-insert. Drained
    // chunks must be non-empty (map_chunks filters empty groups).
    if let Ok(response) = response {
        assert!(
            response.into_inner().partial_success.is_none(),
            "all-or-nothing admission never reports partial success"
        );
    }

    let mut drained_records = 0_usize;
    for command in receiver.try_iter() {
        match command {
            IngestMessage::Logs(chunk) => {
                assert!(!chunk.records.is_empty());
                drained_records += chunk.records.len();
            }
            IngestMessage::Metrics(_) => {
                panic!("logs handler must never enqueue metric chunks")
            }
            // Control commands carry no records and are never produced by
            // OTLP handlers; tolerated for exhaustiveness.
            IngestMessage::Flush | IngestMessage::Checkpoint(_) | IngestMessage::Maintenance(_) => {}
        }
    }
    assert!(
        drained_records <= 100_000,
        "accepted {drained_records} records exceeds max_records_per_request"
    );
});
