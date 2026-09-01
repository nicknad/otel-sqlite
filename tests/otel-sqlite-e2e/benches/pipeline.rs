//! End-to-end benchmark of the ingestion pipeline boundary:
//!
//! ```text
//! OTLP ExportLogsServiceRequest
//!     │
//!     ▼
//! LogsIngress::export   (validation + OTLP mapping + enqueue)
//!     │
//!     ▼
//! bounded ingest channel ──▶ InsertBatcher ──▶ bounded WriteCommand queue
//!     │
//!     ▼
//! single SQLite writer thread ──▶ SQLite commit   <- measured until here
//! ```
//!
//! The gRPC transport layer (TCP/tonic framing) is intentionally excluded;
//! it belongs to the load-generator tier (`otel-sqlite-e2e` binary / k6).
//! Each iteration exports one full OTLP request through the real ingress
//! handler, forces a Flush barrier, and waits until every record is committed.
//!
//! Run with `cargo bench -p otel-sqlite-e2e --bench pipeline`.

use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use otel_sqlite_core::storage::IngestMessage;
use otel_sqlite_e2e::generator::WorkloadSpec;
use otel_sqlite_e2e::server::production_defaults;
use otel_sqlite_ingress::LogsIngress;
use otel_sqlite_ingress::mapping::pb::collector::logs::v1::logs_service_server::LogsService;
use otel_sqlite_storage::Storage;
use tempfile::TempDir;

/// Busy-waits until the writer has committed `target` records total.
fn wait_written(storage: &Storage, target: u64) {
    let deadline = Instant::now() + Duration::from_secs(60);
    while storage.stats().records_written < target {
        assert!(
            Instant::now() < deadline,
            "writer did not reach {target} records"
        );
        std::hint::spin_loop();
    }
}

fn bench_ingest_to_sqlite(c: &mut Criterion) {
    let mut group = c.benchmark_group("e2e_ingest_to_sqlite");
    group.sample_size(20);
    group.warm_up_time(Duration::from_secs(2));
    group.measurement_time(Duration::from_secs(15));

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    let dir = TempDir::new().expect("temp dir");
    let (sender, receiver) =
        otel_sqlite_ingress::channel(production_defaults::INGEST_QUEUE_CAPACITY);
    let mut storage = Storage::open(
        receiver,
        production_defaults::storage_config(dir.path().join("otel-logs.db")),
    )
    .expect("storage opens");
    let ingress = LogsIngress::new(
        sender.clone(),
        Arc::new(production_defaults::ingress_config(
            "127.0.0.1:0".to_owned(),
        )),
        storage.commit_ledger(),
    );

    // Production-shaped workload: 512-byte bodies, 8 attributes, spread over
    // 4 resources (so multi-origin batcher behaviour is exercised).
    let spec = WorkloadSpec {
        run_id: format!("criterion-{}", unix_nano()),
        seed: 0x00C0_FFEE,
        body_size: 512,
        attributes_per_record: 8,
        resource_count: 4,
    };

    // Warm-up outside any timed region: absorbs lazy writer startup (schema
    // migration, FTS init) and first-use costs of the ingress path.
    {
        let target = storage.stats().records_written + 1;
        let warm = tonic::Request::new(spec.render_request(0, 1));
        runtime
            .block_on(ingress.export(warm))
            .expect("warm-up export");
        sender.send(IngestMessage::Flush).expect("batcher alive");
        wait_written(&storage, target);
    }

    for size in [100usize, 1_000, 5_000] {
        group.throughput(Throughput::Elements(size as u64));
        let next_seq = AtomicU64::new(0);
        group.bench_function(format!("request_records_{size}"), |b| {
            b.iter_batched(
                || {
                    let start = next_seq.fetch_add(size as u64, Ordering::Relaxed);
                    tonic::Request::new(spec.render_request(start, size))
                },
                |request| {
                    let target = storage.stats().records_written + size as u64;
                    let response = runtime
                        .block_on(ingress.export(request))
                        .expect("export accepted");
                    black_box(response.into_inner());
                    sender.send(IngestMessage::Flush).expect("batcher alive");
                    wait_written(&storage, target);
                },
                BatchSize::LargeInput,
            );
        });
    }

    // Close every ingest sender (the handler holds one too), then let the
    // batcher drain and the writer shut down cleanly.
    drop(ingress);
    drop(sender);
    storage.join().expect("writer drains and exits");
    drop(dir);

    group.finish();
}

fn unix_nano() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or_default()
}

criterion_group!(benches, bench_ingest_to_sqlite);
criterion_main!(benches);
