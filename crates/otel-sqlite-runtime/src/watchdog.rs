//! The watchdog: a supervisor that observes pipeline evidence.
//!
//! The watchdog answers *"is the sidecar functioning?"* while the OS /
//! container runtime answers *"is the sidecar alive?"*. It never manipulates
//! components: no commands are enqueued, no SQLite connection is opened, and
//! nothing it does can deadlock against a wedged writer or batcher. Its only
//! output is evidence-derived verdicts:
//!
//! ```text
//! writer/batcher ──(atomic progress + liveness)──▶ watchdog
//!                                                     │
//!                                     ┌───────────────┼───────────────┐
//!                                     ▼               ▼               ▼
//!                                  metrics          logging      halt signal
//! ```
//!
//! # Evidence, not questions
//!
//! Components publish cheap atomic facts (`last_progress_ms`, `running`,
//! queue depth); the watchdog polls them on a timer and derives health:
//!
//! * **Thread death** — a component's `running` flag cleared by its drop
//!   guard (covers early returns *and* panics) → [`HealthState::Unhealthy`].
//! * **Stall with pending work** — work queued or buffered while neither the
//!   writer nor the batcher made progress for `unhealthy_after` →
//!   [`HealthState::Unhealthy`]. This is `WORK BUT NO PROGRESS`.
//! * **Idle pipeline** — zero pending work makes stale progress ages
//!   legitimate (`NO WORK`) → [`HealthState::Healthy`].
//! * **Slow drain / saturation** — progress continues but ages exceed
//!   `degraded_after`, or the command queue crosses
//!   [`WatchdogConfig::queue_pressure_ratio`] → [`HealthState::Degraded`]
//!   ("SQLite is slow").
//!
//! # Action on unhealthy
//!
//! With [`WatchdogConfig::halt_on_unhealthy`] (default) the watchdog sends a
//! one-shot halt signal that stops OTLP ingestion; `main` then drains and
//! joins every thread so the process exits cleanly and an external supervisor
//! can restart it. Restarting individual threads in-process is explicitly out
//! of scope: the supervisor owns process restarts, the watchdog only reports
//! and requests.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::time::Duration;

use crossbeam_channel::{Receiver, Sender, TryRecvError, after, select};
use thiserror::Error;
use tokio::sync::watch;

use crate::config::WatchdogConfig;

/// Derived health of the observed pipeline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HealthState {
    Healthy,
    Degraded,
    Unhealthy,
}

impl HealthState {
    /// Numeric encoding for the `sidecar_health_state` gauge
    /// (0 = healthy, 1 = degraded, 2 = unhealthy).
    pub const fn as_metric(self) -> f64 {
        match self {
            Self::Healthy => 0.0,
            Self::Degraded => 1.0,
            Self::Unhealthy => 2.0,
        }
    }
}

/// One observation of pipeline evidence, produced by a
/// [`HealthSampleSource`].
#[derive(Debug, Clone, Copy)]
pub struct PipelineSample {
    pub writer_running: bool,
    pub batcher_running: bool,
    /// Milliseconds since the writer last executed a command successfully.
    pub writer_idle_ms: u64,
    /// Milliseconds since the batcher last received input or submitted a batch.
    pub batcher_idle_ms: u64,
    /// Depth of the bounded write-command queue.
    pub queue_depth: usize,
    /// Capacity of that queue; `0` disables the saturation rule.
    pub queue_capacity: usize,
    /// Records buffered inside the insert batcher (pending, unsent).
    pub pending_records: usize,
    /// Issued durability tickets that have neither completed nor voided.
    /// In-flight records account for some of these; a ticket that stays
    /// outstanding while nothing is queued or buffered is a leak that will
    /// hold every durable ack behind it forever.
    pub outstanding_tickets: u64,
    /// Milliseconds since the contiguous commit watermark last advanced.
    pub watermark_idle_ms: u64,
    /// Whether the writer is currently executing a maintenance-class command
    /// (checkpoint, prune, vacuum, FTS rebuild, quota eviction). Insert
    /// commits legitimately pause during that window.
    pub maintenance_in_progress: bool,
    /// Milliseconds since the current maintenance command started; `0` when
    /// none is in progress.
    pub maintenance_elapsed_ms: u64,
}

impl PipelineSample {
    /// Work accepted by the pipeline but not yet persisted: queued commands
    /// plus records still buffered in the insert batcher.
    pub fn pending_work(&self) -> usize {
        self.queue_depth.saturating_add(self.pending_records)
    }
}

/// Source of health evidence. Implemented in production by an adapter over
/// `otel_sqlite_storage::StorageHealth`; tests substitute canned samples.
pub trait HealthSampleSource: Send + Sync + 'static {
    fn sample(&self) -> PipelineSample;
}

/// Outcome of evaluating one sample against the configured thresholds.
struct Evaluation {
    state: HealthState,
    reason: String,
}

/// Pure evaluator: maps evidence to a verdict. No I/O, no clocks beyond the
/// sample itself — trivially unit-testable.
fn evaluate(sample: &PipelineSample, config: &WatchdogConfig) -> Evaluation {
    if !sample.writer_running {
        return Evaluation {
            state: HealthState::Unhealthy,
            reason: "sqlite writer thread is not running".to_owned(),
        };
    }
    if !sample.batcher_running {
        return Evaluation {
            state: HealthState::Unhealthy,
            reason: "insert batcher thread is not running".to_owned(),
        };
    }

    // Maintenance window. The writer has stopped committing by design, so
    // the ordinary stall rules would produce a false "records lost" verdict
    // during a legitimate VACUUM/rebuild under live traffic. Suspend them,
    // but keep one bound so a genuinely hung maintenance call is still
    // detected and reported.
    if sample.maintenance_in_progress {
        let elapsed = Duration::from_millis(sample.maintenance_elapsed_ms);
        if elapsed >= config.maintenance_stall_after {
            return Evaluation {
                state: HealthState::Unhealthy,
                reason: format!(
                    "maintenance has been running for {elapsed:.1?} without completing \
                     (limit {:.1?})",
                    config.maintenance_stall_after,
                ),
            };
        }
        let pending = sample.pending_work();
        return Evaluation {
            state: HealthState::Healthy,
            reason: format!("maintenance in progress ({elapsed:.1?}), {pending} record(s) pending"),
        };
    }

    // Durability-leak detection. The commit watermark advances contiguously,
    // so one lost ticket (records dropped without `void`) stalls *every*
    // durable ack forever — a failure mode no amount of waiting cures.
    //
    // Detected shape: tickets outstanding while the pipeline holds no work
    // at all. Their records are visible nowhere, so they can never settle;
    // after `ack_stall_after` this is reported instead of politely masked by
    // the idle "NO WORK is healthy" verdict below.
    //
    // Tickets outstanding *behind* visible work stay with the ordinary
    // progress rules: from evidence alone they are indistinguishable from
    // legitimate in-flight records, and a slow drain must degrade gracefully
    // rather than halt.
    let pending = sample.pending_work();
    if sample.outstanding_tickets > 0
        && pending == 0
        && Duration::from_millis(sample.watermark_idle_ms) >= config.ack_stall_after
    {
        return Evaluation {
            state: HealthState::Unhealthy,
            reason: format!(
                "{} durability ticket(s) unsettled on an otherwise empty pipeline; the \
                 commit watermark has not advanced for {:.1?}, so accepted records were \
                 lost and can never be acknowledged",
                sample.outstanding_tickets,
                Duration::from_millis(sample.watermark_idle_ms),
            ),
        };
    }

    if pending == 0 {
        // NO WORK: stale progress ages are expected and harmless.
        return Evaluation {
            state: HealthState::Healthy,
            reason: "pipeline idle".to_owned(),
        };
    }

    // WORK BUT NO PROGRESS: judge each stage independently. Taking the
    // fresher of the two would let a still-accepting batcher mask a wedged
    // writer until its queue fills.
    let writer_age = Duration::from_millis(sample.writer_idle_ms);
    let batcher_age = Duration::from_millis(sample.batcher_idle_ms);
    let age = writer_age.max(batcher_age);
    if writer_age >= config.unhealthy_after || batcher_age >= config.unhealthy_after {
        return Evaluation {
            state: HealthState::Unhealthy,
            reason: format!(
                "{pending} record(s) pending but no pipeline progress for {age:.1?} \
                 (writer {writer_age:.1?}, batcher {batcher_age:.1?})"
            ),
        };
    }
    if writer_age >= config.degraded_after || batcher_age >= config.degraded_after {
        return Evaluation {
            state: HealthState::Degraded,
            reason: format!(
                "{pending} record(s) pending, draining slowly ({age:.1?} since last progress)"
            ),
        };
    }

    let threshold = queue_pressure_threshold(sample.queue_capacity, config.queue_pressure_ratio);
    if sample.queue_depth >= threshold {
        return Evaluation {
            state: HealthState::Degraded,
            reason: format!(
                "command queue at {}/{} (saturation pressure)",
                sample.queue_depth, sample.queue_capacity
            ),
        };
    }

    Evaluation {
        state: HealthState::Healthy,
        reason: format!("pipeline progressing, {pending} record(s) pending"),
    }
}

fn queue_pressure_threshold(capacity: usize, ratio: f32) -> usize {
    if capacity == 0 {
        return usize::MAX;
    }
    let threshold = (capacity as f32 * ratio.clamp(0.0, 1.0)) as usize;
    threshold.max(1)
}

#[derive(Debug, Error)]
pub enum WatchdogError {
    #[error("watchdog thread panicked")]
    Panicked,
}

/// Observer-only supervisor over the storage pipeline.
///
/// Constructed with a [`HealthSampleSource`] (evidence reader), a
/// [`WatchdogConfig`], and the sending half of a halt signal. On an
/// unhealthy verdict it sends the halt once and exits its loop; graceful
/// drain, joins, and restarts remain the caller's job.
pub struct Watchdog {
    source: Box<dyn HealthSampleSource>,
    config: WatchdogConfig,
    halt: watch::Sender<bool>,
}

impl Watchdog {
    pub fn new(
        source: impl HealthSampleSource,
        config: WatchdogConfig,
        halt: watch::Sender<bool>,
    ) -> Self {
        Self {
            source: Box::new(source),
            config,
            halt,
        }
    }

    /// Starts the watchdog on its own thread and returns a handle controlling
    /// its lifecycle.
    pub fn spawn(self) -> WatchdogHandle {
        let (shutdown_tx, shutdown_rx) = crossbeam_channel::bounded::<()>(1);
        let halted = Arc::new(AtomicBool::new(false));
        let thread_halted = Arc::clone(&halted);
        let join_handle = std::thread::Builder::new()
            .name("otel-sqlite-watchdog".to_owned())
            .spawn(move || {
                run(
                    self.source,
                    self.config,
                    self.halt,
                    thread_halted,
                    shutdown_rx,
                );
            })
            .expect("failed to spawn watchdog thread");

        WatchdogHandle {
            shutdown: Some(shutdown_tx),
            join: Some(join_handle),
            halted,
        }
    }
}

/// Control handle for a spawned [`Watchdog`].
#[derive(Debug)]
pub struct WatchdogHandle {
    shutdown: Option<Sender<()>>,
    join: Option<JoinHandle<()>>,
    halted: Arc<AtomicBool>,
}

impl WatchdogHandle {
    /// Whether the watchdog sent the halt signal during this run.
    pub fn requested_halt(&self) -> bool {
        self.halted.load(Ordering::Relaxed)
    }

    /// Signals the watchdog to stop without waiting for it. Idempotent: a
    /// second call (or one after the thread has exited) is a no-op.
    pub fn shutdown(&self) {
        if let Some(sender) = &self.shutdown {
            let _ = sender.try_send(());
        }
    }

    /// Signals shutdown and joins the thread. Returns an error if the
    /// watchdog panicked; observation already stopped either way.
    pub fn stop(self) -> Result<(), WatchdogError> {
        self.shutdown();
        self.join()
    }

    /// Joins the watchdog thread. Dropping every shutdown sender (including
    /// this handle's) also releases a watchdog waiting for its next tick.
    pub fn join(mut self) -> Result<(), WatchdogError> {
        self.shutdown = None;
        if let Some(handle) = self.join.take() {
            handle.join().map_err(|_| WatchdogError::Panicked)?;
        }
        Ok(())
    }
}

impl Drop for WatchdogHandle {
    fn drop(&mut self) {
        // Best-effort: releasing the sender wakes a block-waiting watchdog
        // even when nobody called `stop()`.
        self.shutdown = None;
    }
}

fn run(
    source: Box<dyn HealthSampleSource>,
    config: WatchdogConfig,
    halt: watch::Sender<bool>,
    halted: Arc<AtomicBool>,
    shutdown: Receiver<()>,
) {
    tracing::info!(tick = ?config.tick_interval, "watchdog started");
    ::metrics::gauge!("sidecar_watchdog_active").set(1.0);

    let mut previous = HealthState::Healthy;
    loop {
        // Drain a shutdown requested while the previous cycle was running so
        // we never evaluate again once teardown began.
        if shutdown_signalled(&shutdown) {
            break;
        }

        let sample = source.sample();
        let evaluation = evaluate(&sample, &config);
        emit_metrics(&sample, evaluation.state);

        if evaluation.state != previous {
            log_transition(previous, evaluation.state, &evaluation.reason);
            previous = evaluation.state;
        } else if evaluation.state == HealthState::Unhealthy {
            // Keep surfacing the cause while unhealthy; transitions alone can
            // scroll away when the process is about to be restarted.
            tracing::error!(reason = %evaluation.reason, "watchdog: pipeline unhealthy");
        }

        if evaluation.state == HealthState::Unhealthy
            && config.halt_on_unhealthy
            && !halted.swap(true, Ordering::Relaxed)
        {
            // Observation stops here, but the drain this triggers is exactly
            // what a post-mortem needs to see: record the complete final
            // sample alongside the verdict so the evidence outlives the loop.
            tracing::error!(
                reason = %evaluation.reason,
                writer_running = sample.writer_running,
                batcher_running = sample.batcher_running,
                writer_idle_ms = sample.writer_idle_ms,
                batcher_idle_ms = sample.batcher_idle_ms,
                queue_depth = sample.queue_depth,
                queue_capacity = sample.queue_capacity,
                pending_records = sample.pending_records,
                outstanding_tickets = sample.outstanding_tickets,
                watermark_idle_ms = sample.watermark_idle_ms,
                maintenance_in_progress = sample.maintenance_in_progress,
                maintenance_elapsed_ms = sample.maintenance_elapsed_ms,
                "watchdog initiating graceful shutdown"
            );
            let _ = halt.send(true);
            break;
        }

        let timer = after(config.tick_interval);
        select! {
            recv(shutdown) -> _ => break,
            recv(timer) -> _ => {},
        }
    }

    ::metrics::gauge!("sidecar_watchdog_active").set(0.0);
    if !halted.load(Ordering::Relaxed) {
        // After an unhealthy halt the last verdict must stay visible until the
        // process exits; only a clean observer shutdown resets it.
        ::metrics::gauge!("sidecar_health_state").set(HealthState::Healthy.as_metric());
    }
    tracing::info!("watchdog stopped");
}

fn emit_metrics(sample: &PipelineSample, state: HealthState) {
    ::metrics::gauge!("sidecar_health_state").set(state.as_metric());
    ::metrics::gauge!("sidecar_pipeline_pending_records").set(sample.pending_work() as f64);
    ::metrics::gauge!("sidecar_component_idle_ms", "component" => "writer")
        .set(sample.writer_idle_ms as f64);
    ::metrics::gauge!("sidecar_component_idle_ms", "component" => "batcher")
        .set(sample.batcher_idle_ms as f64);
}

fn log_transition(from: HealthState, to: HealthState, reason: &str) {
    let detail = format!("health {from:?} -> {to:?}");
    match to {
        HealthState::Unhealthy => tracing::error!(reason, "{detail}"),
        HealthState::Degraded => tracing::warn!(reason, "{detail}"),
        HealthState::Healthy => tracing::info!(reason, "{detail}"),
    }
}

fn shutdown_signalled(shutdown: &Receiver<()>) -> bool {
    !matches!(shutdown.try_recv(), Err(TryRecvError::Empty))
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use otel_sqlite_test_support::wait_until;

    use super::*;
    use crate::config::WatchdogConfig;

    fn config() -> WatchdogConfig {
        WatchdogConfig {
            tick_interval: Duration::from_millis(20),
            degraded_after: Duration::from_millis(50),
            unhealthy_after: Duration::from_millis(100),
            ack_stall_after: Duration::from_millis(60),
            maintenance_stall_after: Duration::from_millis(300),
            queue_pressure_ratio: 0.75,
            halt_on_unhealthy: true,
        }
    }

    fn running_sample() -> PipelineSample {
        PipelineSample {
            writer_running: true,
            batcher_running: true,
            writer_idle_ms: 10,
            batcher_idle_ms: 10,
            queue_depth: 2,
            queue_capacity: 100,
            pending_records: 5,
            outstanding_tickets: 0,
            watermark_idle_ms: 10,
            maintenance_in_progress: false,
            maintenance_elapsed_ms: 0,
        }
    }

    #[test]
    fn idle_pipeline_is_healthy_regardless_of_progress_age() {
        // NO WORK must not look like WORK BUT NO PROGRESS.
        let mut sample = running_sample();
        sample.writer_idle_ms = u64::MAX / 2;
        sample.batcher_idle_ms = u64::MAX / 2;
        sample.queue_depth = 0;
        sample.pending_records = 0;

        let evaluation = evaluate(&sample, &config());
        assert_eq!(evaluation.state, HealthState::Healthy);
    }

    #[test]
    fn fresh_progress_with_pending_work_is_healthy() {
        let evaluation = evaluate(&running_sample(), &config());
        assert_eq!(evaluation.state, HealthState::Healthy);
    }

    #[test]
    fn stalled_progress_with_pending_work_is_unhealthy() {
        let mut sample = running_sample();
        sample.writer_idle_ms = 150;
        sample.batcher_idle_ms = 150;
        assert_eq!(evaluate(&sample, &config()).state, HealthState::Unhealthy);
        // Fresh progress on either stage alone is not enough to clear the
        // verdict; see `fresh_batcher_progress_cannot_mask_a_wedged_writer`.
    }

    #[test]
    fn slow_drain_is_degraded() {
        let mut sample = running_sample();
        sample.writer_idle_ms = 60;
        sample.batcher_idle_ms = 60;
        assert_eq!(evaluate(&sample, &config()).state, HealthState::Degraded);
    }

    /// A long VACUUM/rebuild under live traffic must not be mistaken for a
    /// lost-record stall; only exceeding the maintenance bound is unhealthy.
    #[test]
    fn maintenance_suspends_stall_verdicts_until_its_own_bound() {
        let mut sample = running_sample();
        sample.maintenance_in_progress = true;
        sample.maintenance_elapsed_ms = 50;
        // Even a frozen watermark with outstanding tickets is expected while
        // maintenance holds the writer.
        sample.writer_idle_ms = 50_000;
        sample.batcher_idle_ms = 50_000;
        sample.outstanding_tickets = 2;
        sample.watermark_idle_ms = 50_000;
        assert_eq!(evaluate(&sample, &config()).state, HealthState::Healthy);

        sample.maintenance_elapsed_ms = 500; // >= maintenance_stall_after (300)
        assert_eq!(evaluate(&sample, &config()).state, HealthState::Unhealthy);
    }

    /// The fresher of the two stages must not mask a wedged counterpart:
    /// with a stalled writer, fresh batcher activity only means the queue is
    /// still absorbing records.
    #[test]
    fn fresh_batcher_progress_cannot_mask_a_wedged_writer() {
        let mut sample = running_sample();
        sample.writer_idle_ms = 150; // >= unhealthy_after (100)
        sample.batcher_idle_ms = 5;
        assert_eq!(evaluate(&sample, &config()).state, HealthState::Unhealthy);
    }

    #[test]
    fn saturated_queue_is_degraded_even_while_progressing() {
        let mut sample = running_sample();
        sample.queue_depth = 90; // >= 75% of capacity 100
        sample.pending_records = 0;
        assert_eq!(evaluate(&sample, &config()).state, HealthState::Degraded);

        sample.queue_depth = 40;
        assert_eq!(evaluate(&sample, &config()).state, HealthState::Healthy);
    }

    #[test]
    fn dead_threads_are_unhealthy_before_any_staleness_rule() {
        let mut sample = running_sample();
        sample.writer_running = false;
        assert_eq!(evaluate(&sample, &config()).state, HealthState::Unhealthy);

        sample = running_sample();
        sample.batcher_running = false;
        assert_eq!(evaluate(&sample, &config()).state, HealthState::Unhealthy);
    }

    /// A leaked ticket with an otherwise empty pipeline: the records are
    /// visible nowhere, so they can never settle. Detected at
    /// `ack_stall_after`, and — crucially — *not* masked by the idle
    /// "NO WORK is healthy" shortcut.
    #[test]
    fn outstanding_tickets_on_an_empty_pipeline_stall_into_unhealthy() {
        let mut sample = running_sample();
        sample.queue_depth = 0;
        sample.pending_records = 0;
        sample.outstanding_tickets = 1;
        sample.watermark_idle_ms = 10;
        assert_eq!(
            evaluate(&sample, &config()).state,
            HealthState::Healthy,
            "a fresh watermark gap is normal mid-flight state"
        );

        sample.watermark_idle_ms = 60_000;
        assert_eq!(evaluate(&sample, &config()).state, HealthState::Unhealthy);
    }

    /// Outstanding tickets while work is still visibly in flight are left to
    /// the ordinary progress rules: from evidence alone they are
    /// indistinguishable from legitimate in-flight records. A ticket leaked
    /// amid ongoing traffic becomes detectable once traffic stops.
    #[test]
    fn outstanding_tickets_with_pending_work_follow_progress_rules() {
        let mut sample = running_sample();
        sample.outstanding_tickets = 3;
        sample.watermark_idle_ms = 60_000;
        // Fresh stage progress: not a stall by the ordinary rules.
        assert_eq!(evaluate(&sample, &config()).state, HealthState::Healthy);

        // A real pipeline stall still fires through the ordinary rule.
        sample.writer_idle_ms = 150;
        sample.batcher_idle_ms = 150;
        assert_eq!(evaluate(&sample, &config()).state, HealthState::Unhealthy);
    }

    /// Canned-sample source for driving the spawned watchdog in tests.
    struct FixedSource(PipelineSample);

    impl HealthSampleSource for FixedSource {
        fn sample(&self) -> PipelineSample {
            self.0
        }
    }

    fn stalled_sample() -> PipelineSample {
        let mut sample = running_sample();
        sample.writer_idle_ms = 5_000;
        sample.batcher_idle_ms = 5_000;
        sample
    }

    #[test]
    fn unhealthy_verdict_sends_the_halt_signal_once() {
        let (halt_tx, mut halt_rx) = watch::channel(false);
        let handle = Watchdog::new(FixedSource(stalled_sample()), config(), halt_tx).spawn();

        assert!(
            wait_until(Duration::from_secs(5), || *halt_rx.borrow_and_update()),
            "unhealthy verdict must trigger the halt signal"
        );
        assert!(handle.requested_halt());

        // The watchdog exits by itself after halting; stopping stays clean.
        handle.stop().expect("clean stop after halt");
    }

    #[test]
    fn watchdog_never_halts_when_healthy_or_halt_is_disabled() {
        let assert_no_halt = |source: PipelineSample, halt_on_unhealthy: bool| {
            let (halt_tx, mut halt_rx) = watch::channel(false);
            let mut cfg = config();
            cfg.halt_on_unhealthy = halt_on_unhealthy;
            let handle = Watchdog::new(FixedSource(source), cfg, halt_tx).spawn();

            std::thread::sleep(Duration::from_millis(120));
            assert!(!handle.requested_halt());
            assert!(
                !*halt_rx.borrow_and_update(),
                "no halt expected in this case"
            );
            handle.stop().expect("clean stop");
        };

        // A healthy pipeline must never trigger the halt, even with halting enabled.
        assert_no_halt(running_sample(), true);
        // A stalled pipeline must only observe when halting is disabled.
        assert_no_halt(stalled_sample(), false);
    }

    #[test]
    fn shutdown_stops_the_watchdog_promptly() {
        let (halt_tx, _halt_rx) = watch::channel(false);
        let mut cfg = config();
        cfg.halt_on_unhealthy = false; // keep the loop running for the test
        let handle = Watchdog::new(FixedSource(stalled_sample()), cfg, halt_tx).spawn();
        std::thread::sleep(Duration::from_millis(50));

        let started = Instant::now();
        handle.stop().expect("clean stop");
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "stop must not wait out the next tick deadline"
        );
    }
}
