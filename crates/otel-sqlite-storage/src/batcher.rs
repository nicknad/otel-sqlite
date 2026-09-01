//! Storage-owned insert batcher driver.
//!
//! This thread owns the [`InsertBatcher`] instances for every signal and sits
//! between ingress and the SQLite writer:
//!
//! ```text
//! ingress handlers ──(bounded input channel)──▶ batcher thread
//!                                                  │  recv_timeout(nearest deadline)
//!                                                  ▼
//!                                     bounded WriteCommand queue
//!                                                  │
//!                                                  ▼
//!                                            SQLite writer thread
//! ```
//!
//! The loop block-waits on exactly one of two events: another mapped chunk
//! arriving, or the age deadline of the oldest buffered record expiring. It
//! never polls. Full batches are submitted immediately when capacity is
//! reached; partial batches are submitted when their deadline expires.
//!
//! Submission uses a blocking send onto the bounded command queue so a slow
//! SQLite writer propagates backpressure through this thread into the input
//! channel (and finally to OTLP clients as `partial_success`), instead of ever
//! dropping completed batches.
//!
//! The per-origin accumulation state itself lives in `crate::origin_buffers`.

use std::sync::atomic::Ordering;
use std::time::Instant;

use crossbeam_channel::{Receiver, RecvTimeoutError, SendError, Sender};
use otel_sqlite_core::model::{LogRecord, MetricRecord};
use otel_sqlite_core::storage::{
    CommitLedger, IngestMessage, InsertBatcherConfig, LogWriteBatch, MetricWriteBatch, WriteCommand,
};

use crate::fault;
use crate::origin_buffers::{OriginBuffers, Submission};
use crate::stats::BatcherStats;

fn nearest_deadline(first: Option<Instant>, second: Option<Instant>) -> Option<Instant> {
    match (first, second) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (Some(a), None) => Some(a),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    }
}

enum Event {
    Message(IngestMessage),
    Timer,
    Disconnected,
}

/// Runs the insert batcher until the input channel closes (graceful drain and
/// final flush) or the command queue disappears (writer failure).
///
/// The function never polls: each iteration blocks on either an incoming
/// command or the nearest batch-age deadline via `crossbeam_channel::select`.
///
/// `ledger` is the durability-ticket book: chunks skipped as empty get their
/// ticket voided (nothing to persist), and a dead writer triggers
/// [`CommitLedger::close`] so waiting OTLP handlers fail fast instead of
/// hanging on acks that can never be backed by a commit.
pub(crate) fn run(
    input: Receiver<IngestMessage>,
    commands: Sender<WriteCommand>,
    ledger: &CommitLedger,
    config: InsertBatcherConfig,
    stats: &BatcherStats,
) {
    // Cleared on every exit path (including panics) so the watchdog can tell
    // a dead batcher thread from a merely idle one.
    let _running = stats.running_guard();
    stats.mark_progress();
    let mut logs = OriginBuffers::<LogRecord>::new(config);
    let mut metrics = OriginBuffers::<MetricRecord>::new(config);

    let fault_after = fault::armed_after(fault::BATCHER_AFTER_N);
    let mut events_processed = 0u64;

    let mut alive = true;
    while alive {
        events_processed += 1;
        if let Some(limit) = fault_after
            && events_processed >= limit
        {
            // Deliberate batcher death: stop consuming so the input channel
            // closes (new requests fail fast with UNAVAILABLE) while the
            // writer stays alive. The watchdog observes the dead thread,
            // halts ingestion, and the shutdown path eventually closes the
            // ledger so already-issued durable acks fail instead of hanging.
            tracing::error!(
                limit,
                "fault injection: insert batcher exiting after {limit} events"
            );
            break;
        }
        stats.observe_buffered(logs.buffered(), metrics.buffered());

        let event = match nearest_deadline(logs.deadline(), metrics.deadline()) {
            Some(deadline) => {
                match input.recv_timeout(deadline.saturating_duration_since(Instant::now())) {
                    Ok(message) => Event::Message(message),
                    Err(RecvTimeoutError::Timeout) => Event::Timer,
                    Err(RecvTimeoutError::Disconnected) => Event::Disconnected,
                }
            }
            None => match input.recv() {
                Ok(command) => Event::Message(command),
                Err(_) => Event::Disconnected,
            },
        };

        match event {
            Event::Message(IngestMessage::Logs(chunk)) => {
                if chunk.is_empty() {
                    ledger.void(chunk.commit_seq);
                    continue;
                }
                stats.chunks_ingested.fetch_add(1, Ordering::Relaxed);
                stats.mark_progress();
                // Chunk sizes are recorded at the ingress boundary
                // (`ingress_batch_size`); recording them here too would
                // double-count every chunk.

                let outcome = logs.push(chunk.origin, chunk.records, chunk.commit_seq);
                outcome.for_each(|submission| {
                    alive &= submit_log_batch(&commands, submission, stats);
                });
            }
            Event::Message(IngestMessage::Metrics(chunk)) => {
                if chunk.is_empty() {
                    ledger.void(chunk.commit_seq);
                    continue;
                }
                stats.chunks_ingested.fetch_add(1, Ordering::Relaxed);
                stats.mark_progress();

                let outcome = metrics.push(chunk.origin, chunk.records, chunk.commit_seq);
                outcome.for_each(|submission| {
                    alive &= submit_metric_batch(&commands, submission, stats);
                });
            }
            Event::Message(IngestMessage::Flush) => {
                // Barrier semantics: hand over the partial batches first so
                // everything ingested before this point is persisted before
                // the barrier reaches the writer.
                flush_buffers(&mut logs, &mut metrics, &commands, stats, &mut alive);
                if alive {
                    alive = submit(&commands, WriteCommand::Flush, stats);
                }
            }
            Event::Message(IngestMessage::Checkpoint(mode)) => {
                flush_buffers(&mut logs, &mut metrics, &commands, stats, &mut alive);
                if alive {
                    alive = submit(&commands, WriteCommand::Checkpoint(mode), stats);
                }
            }
            Event::Message(IngestMessage::Maintenance(operation)) => {
                flush_buffers(&mut logs, &mut metrics, &commands, stats, &mut alive);
                if alive {
                    alive = submit(&commands, WriteCommand::Maintenance(operation), stats);
                }
            }
            Event::Timer => {
                let now = Instant::now();
                let expired_logs = logs.flush_expired(now);
                let expired_metrics = metrics.flush_expired(now);
                let expired = expired_logs.len() + expired_metrics.len();
                if expired > 0 {
                    stats
                        .timer_flushes
                        .fetch_add(expired as u64, Ordering::Relaxed);
                }
                for submission in expired_logs {
                    alive &= submit_log_batch(&commands, submission, stats);
                }
                for submission in expired_metrics {
                    alive &= submit_metric_batch(&commands, submission, stats);
                }
            }
            Event::Disconnected => break,
        }
    }

    if alive {
        // Input closed: flush every partial batch so no buffered record is
        // lost, then close the command queue so the writer can drain and exit.
        flush_buffers(&mut logs, &mut metrics, &commands, stats, &mut alive);
    } else {
        tracing::error!(
            dropped_records = stats.dropped_records.load(Ordering::Relaxed),
            "command queue closed while the insert batcher still held records"
        );
        // The writer is gone: records held here were dropped, so no pending
        // ticket can ever be completed. Fail every durable-ack waiter now.
        ledger.close();
    }

    stats.observe_buffered(0, 0);
    drop(commands);
}

/// Emits every per-origin partial batch (if any) so control commands act as
/// barriers and shutdown loses nothing.
fn flush_buffers(
    logs: &mut OriginBuffers<LogRecord>,
    metrics: &mut OriginBuffers<MetricRecord>,
    commands: &Sender<WriteCommand>,
    stats: &BatcherStats,
    alive: &mut bool,
) {
    for submission in logs.flush_all() {
        *alive &= submit_log_batch(commands, submission, stats);
    }
    for submission in metrics.flush_all() {
        *alive &= submit_metric_batch(commands, submission, stats);
    }
}

fn submit(commands: &Sender<WriteCommand>, command: WriteCommand, stats: &BatcherStats) -> bool {
    match commands.send(command) {
        Ok(()) => true,
        Err(SendError(command)) => {
            // The writer is gone; there is nowhere to persist these records.
            stats
                .dropped_records
                .fetch_add(command.record_count() as u64, Ordering::Relaxed);
            ::metrics::counter!("insert_batcher_dropped_records_total")
                .increment(command.record_count() as u64);
            false
        }
    }
}

fn submit_log_batch(
    commands: &Sender<WriteCommand>,
    submission: Submission<LogRecord>,
    stats: &BatcherStats,
) -> bool {
    let command = WriteCommand::InsertLogs(LogWriteBatch {
        origin: submission.origin,
        records: submission.records,
        commit_seqs: submission.commit_seqs,
    });
    let submitted = submit(commands, command, stats);
    if submitted {
        stats.batches_emitted.fetch_add(1, Ordering::Relaxed);
        stats.mark_progress();
        ::metrics::counter!("insert_batcher_batches_emitted_total", "signal" => "logs")
            .increment(1);
    }
    submitted
}

fn submit_metric_batch(
    commands: &Sender<WriteCommand>,
    submission: Submission<MetricRecord>,
    stats: &BatcherStats,
) -> bool {
    let command = WriteCommand::InsertMetrics(MetricWriteBatch {
        origin: submission.origin,
        records: submission.records,
        commit_seqs: submission.commit_seqs,
    });
    let submitted = submit(commands, command, stats);
    if submitted {
        stats.batches_emitted.fetch_add(1, Ordering::Relaxed);
        stats.mark_progress();
        ::metrics::counter!("insert_batcher_batches_emitted_total", "signal" => "metrics")
            .increment(1);
    }
    submitted
}

#[cfg(test)]
mod tests {
    use super::*;
    use otel_sqlite_core::storage::{BatchOrigin, LogChunk};
    use std::sync::Arc;
    use std::time::Duration;

    fn config(max_records: usize, max_age_ms: u64) -> InsertBatcherConfig {
        InsertBatcherConfig::new(max_records, Duration::from_millis(max_age_ms))
    }

    fn chunk(origin: &str, count: usize) -> IngestMessage {
        chunk_with_seq(origin, count, 0)
    }

    fn chunk_with_seq(origin: &str, count: usize, commit_seq: u64) -> IngestMessage {
        IngestMessage::Logs(LogChunk {
            origin: BatchOrigin {
                resource: None,
                schema_url: origin.to_owned(),
            },
            records: (0..count).map(|_| LogRecord::default()).collect(),
            commit_seq,
        })
    }

    /// Spawns the driver and returns the input sender plus a collector that
    /// joins the thread and drains every submitted command in order.
    fn spawn_driver(
        config: InsertBatcherConfig,
        commands: crossbeam_channel::Sender<WriteCommand>,
    ) -> (
        crossbeam_channel::Sender<IngestMessage>,
        std::thread::JoinHandle<()>,
        Arc<BatcherStats>,
        Arc<CommitLedger>,
    ) {
        let (input, receiver) = crossbeam_channel::unbounded();
        let stats = Arc::new(BatcherStats::default());
        let ledger = Arc::new(CommitLedger::new());
        let thread_stats = Arc::clone(&stats);
        let thread_ledger = Arc::clone(&ledger);
        let handle = std::thread::spawn(move || {
            run(receiver, commands, &thread_ledger, config, &thread_stats);
        });
        (input, handle, stats, ledger)
    }

    fn drain(commands: crossbeam_channel::Receiver<WriteCommand>) -> Vec<WriteCommand> {
        commands.into_iter().collect()
    }

    fn log_sizes(commands: &[WriteCommand]) -> Vec<usize> {
        commands
            .iter()
            .filter_map(|command| match command {
                WriteCommand::InsertLogs(batch) => Some(batch.records.len()),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn combines_and_splits_chunks_into_sized_batches() {
        let (commands_tx, commands_rx) = crossbeam_channel::unbounded();
        let (input, handle, stats, _ledger) = spawn_driver(config(1000, 60_000), commands_tx);

        // Same-origin chunks accumulate across arrivals: 700 + 600 emit a
        // full batch of 1000 and leave 300 buffered; 900 more complete the
        // remainder into a second full batch, leaving 200 for shutdown.
        input.send(chunk("a", 700)).unwrap();
        input.send(chunk("a", 600)).unwrap();
        input.send(chunk("a", 900)).unwrap();
        drop(input);
        handle.join().unwrap();

        let commands = drain(commands_rx);
        assert_eq!(log_sizes(&commands), vec![1000, 1000, 200]);
        assert_eq!(stats.batches_emitted.load(Ordering::Relaxed), 3);
        assert_eq!(stats.dropped_records.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn flush_barrier_hands_over_partial_batches_in_order() {
        let (commands_tx, commands_rx) = crossbeam_channel::unbounded();
        let (input, handle, _, _ledger) = spawn_driver(config(1000, 60_000), commands_tx);

        input.send(chunk("a", 700)).unwrap();
        input.send(chunk("a", 600)).unwrap();
        // The barrier must hand over the 300-record partial before anything
        // ingested after it is persisted.
        input.send(IngestMessage::Flush).unwrap();
        input.send(chunk("a", 900)).unwrap();
        drop(input);
        handle.join().unwrap();

        let commands = drain(commands_rx);
        assert_eq!(log_sizes(&commands), vec![1000, 300, 900]);
    }

    #[test]
    fn oversized_input_is_split_at_capacity() {
        let (commands_tx, commands_rx) = crossbeam_channel::unbounded();
        let (input, handle, _, _ledger) = spawn_driver(config(1000, 60_000), commands_tx);

        input.send(chunk("a", 2500)).unwrap();
        drop(input);
        handle.join().unwrap();

        let commands = drain(commands_rx);
        assert_eq!(log_sizes(&commands), vec![1000, 1000, 500]);
    }

    #[test]
    fn partial_batch_flushed_when_max_age_expires() {
        let (commands_tx, commands_rx) = crossbeam_channel::bounded(4);
        let (input, receiver) = crossbeam_channel::unbounded();
        let stats = BatcherStats::default();
        let ledger = CommitLedger::new();
        let handle = std::thread::spawn(move || {
            run(receiver, commands_tx, &ledger, config(1000, 30), &stats);
        });

        let started = Instant::now();
        input.send(chunk("a", 37)).unwrap();

        let first = commands_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("timer flush emits the partial batch");
        let elapsed = started.elapsed();
        // Keep the input open until after the timer fired: shutdown would
        // otherwise flush the partial batch early.
        drop(input);

        handle.join().unwrap();
        assert!(
            commands_rx
                .recv_timeout(Duration::from_millis(200))
                .is_err()
        );

        match first {
            WriteCommand::InsertLogs(batch) => assert_eq!(batch.records.len(), 37),
            other => panic!("expected insert logs batch, got {other:?}"),
        }
        assert!(
            elapsed >= Duration::from_millis(25),
            "partial batch must wait out max_batch_age, emitted after {elapsed:?}"
        );
    }

    #[test]
    fn empty_chunks_never_create_batches() {
        let (commands_tx, commands_rx) = crossbeam_channel::unbounded();
        let (input, handle, _, ledger) = spawn_driver(config(8, 10), commands_tx);

        input
            .send(IngestMessage::Logs(LogChunk {
                origin: BatchOrigin::default(),
                records: Vec::new(),
                // An empty chunk carries no records, but its ticket must not
                // stall the commit watermark either.
                commit_seq: 5,
            }))
            .unwrap();
        input.send(IngestMessage::Flush).unwrap();
        drop(input);
        handle.join().unwrap();

        let commands = drain(commands_rx);
        assert_eq!(log_sizes(&commands), Vec::<usize>::new());
        assert_eq!(commands.len(), 1);
        assert!(matches!(commands[0], WriteCommand::Flush));
        assert_eq!(
            ledger.watermark().committed_through,
            0,
            "the batcher never completes tickets; the writer owns that"
        );
    }

    #[test]
    fn skipped_empty_chunk_voids_its_ticket_downstream() {
        let (commands_tx, _commands_rx) = crossbeam_channel::unbounded();
        let (input, handle, _, ledger) = spawn_driver(config(8, 10), commands_tx);

        // Tickets 1 and 2 belong to earlier traffic that already committed;
        // only then can ticket 3 release its position on the watermark.
        ledger.complete(1);
        ledger.complete(2);

        input
            .send(IngestMessage::Logs(LogChunk {
                origin: BatchOrigin::default(),
                records: Vec::new(),
                commit_seq: 3,
            }))
            .unwrap();
        drop(input);
        handle.join().unwrap();

        assert_eq!(
            ledger.watermark().committed_through,
            3,
            "the voided ticket releases its position on the watermark"
        );
        assert!(!ledger.watermark().closed);
    }

    #[test]
    fn origin_change_never_mixes_records_into_one_batch() {
        let (commands_tx, commands_rx) = crossbeam_channel::unbounded();
        let (input, handle, _, _ledger) = spawn_driver(config(1000, 60_000), commands_tx);

        input.send(chunk("service-a", 5)).unwrap();
        input.send(chunk("service-b", 3)).unwrap();
        drop(input);
        handle.join().unwrap();

        let commands = drain(commands_rx);
        assert_eq!(log_sizes(&commands), vec![5, 3]);
        for command in &commands {
            match command {
                WriteCommand::InsertLogs(batch) => match batch.origin.schema_url.as_str() {
                    "service-a" => assert_eq!(batch.records.len(), 5),
                    "service-b" => assert_eq!(batch.records.len(), 3),
                    other => panic!("unexpected origin {other}"),
                },
                other => panic!("expected insert logs batch, got {other:?}"),
            }
        }
    }

    #[test]
    fn write_batches_claim_every_ingest_ticket_of_their_origin() {
        let (commands_tx, commands_rx) = crossbeam_channel::unbounded();
        let (input, handle, _, _ledger) = spawn_driver(config(1000, 60_000), commands_tx);

        // Both chunks share one origin and fit one partial batch; the emitted
        // batch must claim every folded ticket — settling only the maximum
        // would strand ticket 3 on the commit watermark.
        input.send(chunk_with_seq("a", 4, 3)).unwrap();
        input.send(chunk_with_seq("a", 4, 9)).unwrap();
        input.send(chunk_with_seq("b", 2, 5)).unwrap();
        drop(input);
        handle.join().unwrap();

        let claimed: Vec<(String, Vec<u64>)> = drain(commands_rx)
            .into_iter()
            .filter_map(|command| match command {
                WriteCommand::InsertLogs(batch) => {
                    Some((batch.origin.schema_url.clone(), batch.commit_seqs))
                }
                _ => None,
            })
            .collect();
        assert_eq!(
            claimed,
            vec![("a".to_owned(), vec![3, 9]), ("b".to_owned(), vec![5])],
            "each origin's batch claims all of its folded ingest tickets"
        );
    }

    #[test]
    fn full_command_queue_applies_backpressure_instead_of_dropping() {
        let (commands_tx, commands_rx) = crossbeam_channel::bounded(1);
        let (input, receiver) = crossbeam_channel::bounded(8);
        let stats = Arc::new(BatcherStats::default());
        let ledger = CommitLedger::new();
        let thread_stats = Arc::clone(&stats);
        let handle = std::thread::spawn(move || {
            run(
                receiver,
                commands_tx,
                &ledger,
                config(10, 60_000),
                &thread_stats,
            );
        });

        // Chunks are exactly one storage batch each.
        let mut accepted = 0usize;
        let send_chunk = |input: &crossbeam_channel::Sender<IngestMessage>| {
            input.try_send(chunk("a", 10)).is_ok()
        };

        assert!(send_chunk(&input), "first chunk is accepted");
        accepted += 1;
        std::thread::sleep(Duration::from_millis(50));

        // The batcher ends up blocked inside its blocking send onto the full
        // command queue: the ingest channel must saturate (backpressure)
        // rather than dropping or overwriting any completed batch.
        let mut saturated = false;
        for _ in 0..32 {
            if send_chunk(&input) {
                accepted += 1;
            } else {
                saturated = true;
            }
        }
        assert!(saturated, "ingest channel must saturate under load");
        assert!(
            input.try_send(chunk("a", 10)).is_err(),
            "backpressure must persist while the writer does not drain"
        );

        // Draining the writer side unblocks the chain; everything accepted is
        // submitted exactly once and nothing is dropped.
        drop(input);
        let mut sizes = Vec::new();
        while let Ok(command) = commands_rx.recv_timeout(Duration::from_secs(2)) {
            if let WriteCommand::InsertLogs(batch) = command {
                sizes.push(batch.records.len());
            }
        }
        handle.join().unwrap();

        assert_eq!(stats.dropped_records.load(Ordering::Relaxed), 0);
        assert_eq!(sizes.iter().sum::<usize>(), accepted * 10);
        assert!(sizes.iter().all(|size| *size == 10));
        assert_eq!(sizes.len(), accepted);
    }

    #[test]
    fn closed_command_queue_stops_the_batcher_without_hanging() {
        let (commands_tx, commands_rx) = crossbeam_channel::bounded(2);
        let (input, receiver) = crossbeam_channel::unbounded();
        let stats = Arc::new(BatcherStats::default());
        let ledger = Arc::new(CommitLedger::new());
        let thread_stats = Arc::clone(&stats);
        let thread_ledger = Arc::clone(&ledger);
        let handle = std::thread::spawn(move || {
            run(
                receiver,
                commands_tx,
                &thread_ledger,
                config(4, 60_000),
                &thread_stats,
            );
        });
        drop(commands_rx); // simulate writer death

        // The batcher fast-fails once submissions fail, which disconnects the
        // input channel; stop feeding as soon as that is observed.
        for _ in 0..64 {
            if input.try_send(chunk("a", 8)).is_err() {
                break;
            }
        }
        let started = Instant::now();
        drop(input);
        handle
            .join()
            .expect("batcher exits when the command queue closes");

        assert!(
            started.elapsed() < Duration::from_secs(5),
            "batcher must stop promptly once submissions fail"
        );
        assert!(
            stats.dropped_records.load(Ordering::Relaxed) > 0,
            "records held when the queue closed are accounted as dropped"
        );
        assert!(
            ledger.watermark().closed,
            "writer death must close the ledger so durable-ack waiters fail fast"
        );
    }

    #[test]
    fn shutdown_flush_drains_every_buffered_record() {
        let (commands_tx, commands_rx) = crossbeam_channel::unbounded();
        let (input, handle, _, _ledger) = spawn_driver(config(100, 600_000), commands_tx);

        input.send(chunk("a", 37)).unwrap();
        input.send(chunk("a", 41)).unwrap();
        drop(input); // no Flush, no timer expiry: shutdown must flush 78 records
        handle.join().unwrap();

        let commands = drain(commands_rx);
        assert_eq!(log_sizes(&commands), vec![78]);
        assert_eq!(log_sizes(&commands).iter().sum::<usize>(), 78);
    }

    #[test]
    fn fifo_order_is_preserved_across_all_emissions() {
        let (commands_tx, commands_rx) = crossbeam_channel::unbounded();
        let (input, handle, _, _ledger) = spawn_driver(config(4, 60_000), commands_tx);

        // Three consecutive chunks of 5/7/3 records with contiguous sequence
        // numbers; capacity is 4 so every chunk splits and combines.
        let mut next_seq = 0i64;
        for count in [5usize, 7, 3] {
            input
                .send(IngestMessage::Logs(LogChunk {
                    origin: BatchOrigin::default(),
                    records: (0..count)
                        .map(|_| {
                            let record = LogRecord {
                                time_unix_nano: next_seq,
                                ..LogRecord::default()
                            };
                            next_seq += 1;
                            record
                        })
                        .collect(),
                    commit_seq: 0,
                }))
                .unwrap();
        }
        drop(input);
        handle.join().unwrap();

        let sequence: Vec<i64> = drain(commands_rx)
            .into_iter()
            .flat_map(|command| match command {
                WriteCommand::InsertLogs(batch) => batch
                    .records
                    .into_records()
                    .into_iter()
                    .map(|record| record.time_unix_nano)
                    .collect::<Vec<_>>(),
                _ => panic!("expected only insert batches"),
            })
            .collect();

        assert_eq!(sequence.len(), 15);
        assert_eq!(sequence, (0..15).collect::<Vec<_>>());
    }
}
