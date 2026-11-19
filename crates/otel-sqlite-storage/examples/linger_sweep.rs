//! Commit-linger dose-response probe over the real writer pipeline.
//!
//! Bypasses OTLP and the insert batcher: independent sender threads place
//! one-record insert commands directly on the writer's command queue with
//! Poisson inter-arrival, and a single waiter resolves each command's
//! commit-ack latency from the durable watermark. Because senders never
//! wait for their own acks, the arrival process cannot couple to the commit
//! cycle - closed-loop ack convoys (see `docs/performance.md`) would
//! otherwise re-synchronize traffic and mask the window's effect.
//!
//! Usage:
//!
//! ```text
//! cargo run --release -p otel-sqlite-storage --example linger_sweep -- \
//!     --rate 300 --duration-secs 10 --lingers 0,250us,500us,1ms,2ms,5ms
//! ```
//!
//! `--sync full` makes every commit an fsync, so `txn/s` doubles as the
//! fsync frequency. `--senders` spreads the Poisson process across threads.

// Probe binaries report their measurements on stdout by design.
#![allow(clippy::print_stdout, clippy::print_stderr)]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use crossbeam_channel::unbounded;
use otel_sqlite_core::model::{AttributeValue, LogRecord, Severity};
use otel_sqlite_core::storage::{
    BatchOrigin, LogWriteBatch, RecordSegment, SyncMode, WriteBatch, WriteCommand,
};
use otel_sqlite_core::unix_nano_now;
use otel_sqlite_storage::{Storage, StorageConfig};

/// One measured linger setting.
struct Args {
    rate: f64,
    duration: Duration,
    senders: usize,
    sync: SyncMode,
    lingers: Vec<Duration>,
}

fn main() {
    let args = parse_args();
    println!(
        "linger sweep: rate {} rec/s, {} senders, {:?} window, synchronous={}\n",
        args.rate,
        args.senders,
        args.duration,
        args.sync.as_str()
    );
    println!(
        "{:<10} {:>8} {:>8} {:>8} {:>8} {:>8} {:>7} {:>9} {:>8}",
        "linger", "acks/s", "p50ms", "p95ms", "p99ms", "maxms", "txns", "rec/txn", "txn/s"
    );
    for linger in &args.lingers {
        let row = run_once(*linger, &args);
        println!(
            "{:<10} {:>8.1} {:>8.2} {:>8.2} {:>8.2} {:>8.2} {:>7} {:>9.2} {:>8.1}",
            label(*linger),
            row.acks_per_s,
            row.p50_ms,
            row.p95_ms,
            row.p99_ms,
            row.max_ms,
            row.txns,
            row.rec_per_txn,
            row.txns_per_s
        );
    }
}

struct Row {
    acks_per_s: f64,
    p50_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
    max_ms: f64,
    txns: u64,
    rec_per_txn: f64,
    txns_per_s: f64,
}

fn label(linger: Duration) -> String {
    if linger.is_zero() {
        "off".to_owned()
    } else {
        format!("{}us", linger.as_micros())
    }
}

fn run_once(linger: Duration, args: &Args) -> Row {
    let dir = tempfile::tempdir().expect("temp dir");
    let (ingest_tx, ingest_rx) = unbounded();
    let config = StorageConfig {
        sqlite_path: dir.path().join("linger.db"),
        group_commit_linger: linger,
        synchronous: args.sync,
        ..StorageConfig::default()
    };
    let mut storage = Storage::open(ingest_rx, config).expect("pipeline opens");
    let ledger = storage.commit_ledger();
    let producer = storage.producer();
    let stop = Arc::new(AtomicBool::new(false));
    let (sent_tx, sent_rx) = unbounded::<(u64, Instant)>();

    // Poisson arrivals: each sender sleeps an exponential interval with mean
    // `senders / rate`, so the aggregate process is Poisson at `rate`.
    let mean = Duration::from_secs_f64(args.senders as f64 / args.rate);
    let mut senders = Vec::with_capacity(args.senders);
    for _ in 0..args.senders {
        let producer = producer.clone();
        let ledger = Arc::clone(&ledger);
        let stop = Arc::clone(&stop);
        let sent_tx = sent_tx.clone();
        senders.push(std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                let u = rand::random::<f64>().max(f64::MIN_POSITIVE);
                std::thread::sleep(mean.mul_f64(-u.ln()));
                let seq = ledger.issue();
                let sent = Instant::now();
                let command = WriteCommand::InsertLogs(LogWriteBatch::new(
                    BatchOrigin::default(),
                    WriteBatch::from_vec(vec![sample_record(seq)]),
                    vec![RecordSegment {
                        commit_seq: seq,
                        records: 1,
                        complete: true,
                    }],
                ));
                if producer.send(command).is_err() {
                    break;
                }
                let _ = sent_tx.send((seq, sent));
            }
        }));
    }

    // The waiter resolves each ticket against the contiguous watermark - the
    // same evidence a durable OTLP ack waits on. Senders never block on it,
    // so arrivals stay decoupled from the commit cycle.
    let waiter = {
        let ledger = Arc::clone(&ledger);
        let sent_rx = sent_rx;
        std::thread::spawn(move || {
            let mut latencies: Vec<f64> = Vec::new();
            for (seq, sent) in sent_rx {
                let deadline = sent + Duration::from_secs(60);
                while ledger.watermark().committed_through < seq {
                    assert!(
                        Instant::now() <= deadline,
                        "ticket {seq} never settled within 60s"
                    );
                    std::hint::spin_loop();
                }
                latencies.push(sent.elapsed().as_secs_f64() * 1000.0);
            }
            latencies
        })
    };

    std::thread::sleep(args.duration);
    stop.store(true, Ordering::Relaxed);
    for sender in senders {
        sender.join().expect("sender thread finishes");
    }
    drop(sent_tx);
    let mut latencies = waiter.join().expect("waiter thread finishes");

    // Drain order: the direct producers first, then the (never used) ingest
    // side, so the queue closes and the writer commits what it holds.
    drop(producer);
    drop(ingest_tx);
    storage.join().expect("writer drains and exits");

    let stats = storage.stats();
    let txns = stats.transactions_committed;
    let records = stats.records_written;
    let measured_s = args.duration.as_secs_f64();
    latencies.sort_by(f64::total_cmp);
    Row {
        acks_per_s: latencies.len() as f64 / measured_s,
        p50_ms: percentile(&latencies, 0.50),
        p95_ms: percentile(&latencies, 0.95),
        p99_ms: percentile(&latencies, 0.99),
        max_ms: latencies.last().copied().unwrap_or(f64::NAN),
        txns,
        rec_per_txn: records as f64 / txns.max(1) as f64,
        txns_per_s: txns as f64 / measured_s,
    }
}

fn percentile(sorted: &[f64], q: f64) -> f64 {
    if sorted.is_empty() {
        return f64::NAN;
    }
    let pos = q * (sorted.len() - 1) as f64;
    let lo = pos.floor() as usize;
    let hi = (lo + 1).min(sorted.len() - 1);
    let frac = pos - lo as f64;
    sorted[lo] * (1.0 - frac) + sorted[hi] * frac
}

fn sample_record(seq: u64) -> LogRecord {
    let now = unix_nano_now();
    LogRecord {
        time_unix_nano: now,
        observed_time_unix_nano: now,
        severity_number: Severity::Info,
        severity_text: "INFO".to_owned(),
        trace_id: [0; 16],
        span_id: [0; 8],
        body: format!("linger sweep sample log body padded for realism ..... seq={seq}"),
        attributes: vec![
            ("service.name", "linger-sweep".to_owned()).into(),
            ("http.status_code", AttributeValue::Int(200)).into(),
        ],
        event_name: "linger_sweep".to_owned(),
        ..LogRecord::default()
    }
}

fn parse_args() -> Args {
    let mut rate = 300.0;
    let mut duration = Duration::from_secs(10);
    let mut senders = 8;
    let mut sync = SyncMode::Normal;
    let mut lingers = parse_lingers("0,250us,500us,1ms,2ms,5ms");
    let mut values = std::env::args().skip(1);
    while let Some(flag) = values.next() {
        match flag.as_str() {
            "--rate" => rate = parse_f64(next(&mut values, &flag)),
            "--duration-secs" => {
                duration = Duration::from_secs_f64(parse_f64(next(&mut values, &flag)));
            }
            "--senders" => senders = parse_f64(next(&mut values, &flag)).max(1.0) as usize,
            "--sync" => {
                let value = next(&mut values, &flag);
                sync = SyncMode::parse(&value).expect("normal or full");
            }
            "--lingers" => lingers = parse_lingers(&next(&mut values, &flag)),
            other => panic!("unknown flag {other}"),
        }
    }
    assert!(rate > 0.0, "--rate must be positive");
    assert!(
        !lingers.is_empty(),
        "--lingers must list at least one value"
    );
    Args {
        rate,
        duration,
        senders,
        sync,
        lingers,
    }
}

fn next(values: &mut impl Iterator<Item = String>, flag: &str) -> String {
    values
        .next()
        .unwrap_or_else(|| panic!("{flag} needs a value"))
}

fn parse_f64(text: String) -> f64 {
    text.parse::<f64>()
        .unwrap_or_else(|_| panic!("expected a number, got {text}"))
}

fn parse_lingers(text: &str) -> Vec<Duration> {
    text.split(',')
        .map(|part| parse_linger(part.trim()).expect("linger like 2ms, 250us or 0"))
        .collect()
}

fn parse_linger(text: &str) -> Option<Duration> {
    let trimmed = text.trim();
    if trimmed.eq_ignore_ascii_case("off") {
        return Some(Duration::ZERO);
    }
    let split_at = trimmed
        .find(|c: char| !(c.is_ascii_digit() || c == '.'))
        .unwrap_or(trimmed.len());
    let (number, unit) = trimmed.split_at(split_at);
    let amount: f64 = number.parse().ok()?;
    let micros = amount
        * match unit.trim() {
            "" | "us" | "usec" | "micros" => 1.0,
            "ms" => 1_000.0,
            "s" => 1_000_000.0,
            _ => return None,
        };
    Some(Duration::from_micros(micros.round() as u64))
}
