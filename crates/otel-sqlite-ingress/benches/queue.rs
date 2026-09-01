//! Microbenchmarks for the message queue boundary between ingress and storage
//! (the admission-gated [`otel_sqlite_ingress::IngestSender`] wrapper over
//! crossbeam):
//!
//! * `enqueue_dequeue_uncontended`: single-threaded `try_send` + `recv` round
//!   trip (the per-request cost ingress pays when the queue is healthy),
//! * `enqueue_1p1c`: one bench thread producing, one consumer thread draining
//!   — sustained send throughput under real contention,
//! * `dequeue_1p1c`: one producer thread keeping the queue fed, bench thread
//!   consuming.
//!
//! Concurrent numbers include OS scheduling; treat them as sanity checks and
//! complement with a dedicated load generator for sustained-load claims.
//!
//! Run with `cargo bench -p otel-sqlite-ingress --bench queue`.

use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;

use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use otel_sqlite_core::model::LogRecord;
use otel_sqlite_core::storage::{BatchOrigin, IngestMessage, LogChunk};
use otel_sqlite_ingress::channel;

const QUEUE_CAPACITY: usize = 4_096;
/// Messages handed to the queue per measured iteration in the concurrent
/// benches.
const MESSAGES_PER_ITERATION: usize = 256;

fn chunk_message(seq: usize, records_per_chunk: usize) -> IngestMessage {
    IngestMessage::Logs(LogChunk {
        origin: BatchOrigin::default(),
        records: (0..records_per_chunk)
            .map(|offset| LogRecord {
                body: format!("seq={}", seq * records_per_chunk + offset),
                ..LogRecord::default()
            })
            .collect(),
        commit_seq: 0,
    })
}

/// Single-threaded round trip through a bounded channel: what an OTLP handler
/// pays per request when no other thread contends on the queue.
fn bench_enqueue_dequeue_uncontended(c: &mut Criterion) {
    let mut group = c.benchmark_group("message_queue");

    let (sender, receiver) = channel(QUEUE_CAPACITY);
    for records_per_chunk in [1usize, 10, 100] {
        group.throughput(Throughput::Elements(records_per_chunk as u64));
        group.bench_function(format!("enqueue_dequeue_chunk_{records_per_chunk}"), |b| {
            b.iter_batched(
                || chunk_message(0, records_per_chunk),
                |message| {
                    sender.try_send(message).expect("bounded above workload");
                    black_box(receiver.recv().expect("same process"));
                },
                BatchSize::PerIteration,
            );
        });
    }
    drop(sender);

    group.finish();
}

/// Sustained enqueue throughput: the bench thread sends while a dedicated
/// consumer drains. Once the backlog exceeds capacity `send` blocks, so this
/// converges to true end-to-end channel throughput rather than raw send cost.
fn bench_enqueue_concurrent(c: &mut Criterion) {
    const CHUNK_RECORDS: usize = 10;
    let mut group = c.benchmark_group("message_queue");

    group.throughput(Throughput::Elements(
        (MESSAGES_PER_ITERATION * CHUNK_RECORDS) as u64,
    ));
    group.bench_function(format!("enqueue_1p1c_chunk_{CHUNK_RECORDS}"), |b| {
        let (sender, receiver) = channel(QUEUE_CAPACITY);
        let consumer = thread::spawn(move || {
            // Blocking recv: zero polling overhead; exits on the sentinel.
            while let Ok(message) = receiver.recv() {
                if matches!(message, IngestMessage::Flush) {
                    break;
                }
            }
        });

        b.iter_batched(
            || {
                (0..MESSAGES_PER_ITERATION)
                    .map(|seq| chunk_message(seq, CHUNK_RECORDS))
                    .collect::<Vec<_>>()
            },
            |messages| {
                for message in messages {
                    sender.send(message).expect("consumer alive");
                }
            },
            BatchSize::LargeInput,
        );

        // Sentinel: drain everything enqueued so far, then stop.
        let _ = sender.send(IngestMessage::Flush);
        consumer.join().expect("consumer joins");
        drop(sender);
    });

    group.finish();
}

/// Sustained dequeue throughput: a dedicated producer keeps the bounded queue
/// fed and the bench thread measures how fast messages can be taken off it.
fn bench_dequeue_concurrent(c: &mut Criterion) {
    const CHUNK_RECORDS: usize = 10;
    let mut group = c.benchmark_group("message_queue");

    group.throughput(Throughput::Elements(
        (MESSAGES_PER_ITERATION * CHUNK_RECORDS) as u64,
    ));
    group.bench_function(format!("dequeue_1p1c_chunk_{CHUNK_RECORDS}"), |b| {
        let (sender, receiver) = channel(QUEUE_CAPACITY);
        let stop = Arc::new(AtomicBool::new(false));
        let producer = {
            let stop = Arc::clone(&stop);
            let sender = sender.clone();
            thread::spawn(move || {
                let mut seq = 0usize;
                while !stop.load(Ordering::Relaxed) {
                    if sender.send(chunk_message(seq, CHUNK_RECORDS)).is_err() {
                        break;
                    }
                    seq += 1;
                }
            })
        };

        b.iter(|| {
            for _ in 0..MESSAGES_PER_ITERATION {
                black_box(receiver.recv().expect("producer alive"));
            }
        });

        // Unblock a producer parked on a full queue, then shut it down.
        while receiver.try_recv().is_ok() {}
        stop.store(true, Ordering::Relaxed);
        drop(sender);
        producer.join().expect("producer joins");
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_enqueue_dequeue_uncontended,
    bench_enqueue_concurrent,
    bench_dequeue_concurrent
);
criterion_main!(benches);
