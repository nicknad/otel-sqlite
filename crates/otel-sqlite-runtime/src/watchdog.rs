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
use otel_sqlite_core::PipelineSample;
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
    //
    // Saturation and durability stalls are still real during maintenance:
    // they are surfaced as Degraded (informational, never a halt) so an
    // operator sees them long before `maintenance_stall_after`.
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
        let watermark_age = Duration::from_millis(sample.watermark_idle_ms);
        if sample.outstanding_tickets > 0 && watermark_age >= config.ack_stall_after {
            return Evaluation {
                state: HealthState::Degraded,
                reason: format!(
                    "maintenance in progress ({elapsed:.1?}), {} durability ticket(s) \
                     unsettled and the commit watermark has not advanced for {watermark_age:.1?}",
                    sample.outstanding_tickets,
                ),
            };
        }
        let threshold =
            queue_pressure_threshold(sample.queue_capacity, config.queue_pressure_ratio);
        if sample.queue_depth >= threshold {
            return Evaluation {
                state: HealthState::Degraded,
                reason: format!(
                    "maintenance in progress ({elapsed:.1?}), command queue at {}/{} \
                     (saturation pressure)",
                    sample.queue_depth, sample.queue_capacity
                ),
            };
        }
        let pending = sample.pending_work();
        return Evaluation {
            state: HealthState::Healthy,
            reason: format!(
                "maintenance in progress ({elapsed:.1?}), {pending} work item(s) pending"
            ),
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
                "{pending} work item(s) pending but no pipeline progress for {age:.1?} \
                 (writer {writer_age:.1?}, batcher {batcher_age:.1?})"
            ),
        };
    }
    if writer_age >= config.degraded_after || batcher_age >= config.degraded_after {
        return Evaluation {
            state: HealthState::Degraded,
            reason: format!(
                "{pending} work item(s) pending, draining slowly ({age:.1?} since last progress)"
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
        reason: format!("pipeline progressing, {pending} work item(s) pending"),
    }
}

fn queue_pressure_threshold(capacity: usize, ratio: f32) -> usize {
    if capacity == 0 {
        return usize::MAX;
    }
    // A non-finite ratio cannot express a threshold; treat it as "only a
    // completely full queue is saturated". The binary rejects such values, but
    // this is a public library type.
    let ratio = if ratio.is_finite() {
        ratio.clamp(0.0, 1.0)
    } else {
        1.0
    };
    let threshold = (capacity as f32 * ratio) as usize;
    threshold.max(1)
}

/// Floor for the sampling interval. A zero or sub-millisecond interval would
/// make the watchdog loop continuously, taking the commit ledger's locks and
/// emitting metrics on every iteration. The binary rejects zero, but
/// [`WatchdogConfig`] is a public type and callers may still pass tiny values.
const MIN_TICK_INTERVAL: Duration = Duration::from_millis(10);

/// Clamps the configured interval into a range that cannot spin.
fn normalized_tick_interval(interval: Duration) -> Duration {
    interval.max(MIN_TICK_INTERVAL)
}

#[derive(Debug, Error)]
pub enum WatchdogError {
    #[error("watchdog thread panicked")]
    Panicked,
    #[error(transparent)]
    InvalidConfig(#[from] crate::config::WatchdogConfigError),
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
    /// Validates the configuration and creates a watchdog.
    ///
    /// Returns [`WatchdogError::InvalidConfig`] for threshold orderings or
    /// ranges that cannot produce meaningful verdicts, instead of spawning an
    /// observer that silently never degrades or halts.
    pub fn new(
        source: impl HealthSampleSource,
        mut config: WatchdogConfig,
        halt: watch::Sender<bool>,
    ) -> Result<Self, WatchdogError> {
        config.validate()?;
        config.tick_interval = normalized_tick_interval(config.tick_interval);
        Ok(Self {
            source: Box::new(source),
            config,
            halt,
        })
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

/// Publishes the watchdog's liveness for as long as the thread runs, including
/// unwind paths (a panic must not leave the gauge stuck at 1).
struct ActiveGauge;

impl ActiveGauge {
    fn new() -> Self {
        ::metrics::gauge!("sidecar_watchdog_active").set(1.0);
        Self
    }
}

impl Drop for ActiveGauge {
    fn drop(&mut self) {
        ::metrics::gauge!("sidecar_watchdog_active").set(0.0);
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
    let _active = ActiveGauge::new();

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

        if evaluation.state == HealthState::Unhealthy && config.halt_on_unhealthy {
            // Teardown may have started while this sample was evaluated: a
            // component that stopped because we are shutting down is not a
            // fault, and a halt nobody receives is not a halt.
            if shutdown_signalled(&shutdown) {
                break;
            }
            if halt.send(true).is_err() {
                tracing::error!(
                    reason = %evaluation.reason,
                    "watchdog: pipeline unhealthy but the halt receiver is gone; stopping observation"
                );
                break;
            }
            halted.store(true, Ordering::Relaxed);
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
            break;
        }

        let timer = after(config.tick_interval);
        select! {
            recv(shutdown) -> _ => break,
            recv(timer) -> _ => {},
        }
    }

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
    ::metrics::gauge!("sidecar_queue_depth").set(sample.queue_depth as f64);
    ::metrics::gauge!("sidecar_queue_capacity").set(sample.queue_capacity as f64);
    ::metrics::gauge!("sidecar_outstanding_tickets").set(sample.outstanding_tickets as f64);
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
    /// Durability stalls and saturation are still surfaced as Degraded.
    #[test]
    fn maintenance_suspends_stall_verdicts_until_its_own_bound() {
        let mut sample = running_sample();
        sample.maintenance_in_progress = true;
        sample.maintenance_elapsed_ms = 50;
        // Even a frozen watermark with outstanding tickets is expected while
        // maintenance holds the writer, but it must not be invisible: report
        // it as Degraded rather than halting.
        sample.writer_idle_ms = 50_000;
        sample.batcher_idle_ms = 50_000;
        sample.outstanding_tickets = 2;
        sample.watermark_idle_ms = 50_000;
        assert_eq!(evaluate(&sample, &config()).state, HealthState::Degraded);

        sample.maintenance_elapsed_ms = 500; // >= maintenance_stall_after (300)
        assert_eq!(evaluate(&sample, &config()).state, HealthState::Unhealthy);
    }

    /// Saturation behind a long maintenance call is a real capacity signal;
    /// it must not be hidden by the maintenance window.
    #[test]
    fn saturated_queue_during_maintenance_is_degraded() {
        let mut sample = running_sample();
        sample.maintenance_in_progress = true;
        sample.maintenance_elapsed_ms = 50;
        sample.outstanding_tickets = 0;
        sample.watermark_idle_ms = 0;
        sample.queue_depth = 90; // >= 75% of capacity 100
        sample.pending_records = 0;
        assert_eq!(evaluate(&sample, &config()).state, HealthState::Degraded);

        // Nothing pending behind it: maintenance alone stays Healthy.
        sample.queue_depth = 0;
        sample.pending_records = 0;
        assert_eq!(evaluate(&sample, &config()).state, HealthState::Healthy);
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
        let handle = Watchdog::new(FixedSource(stalled_sample()), config(), halt_tx)
            .expect("valid config")
            .spawn();

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
            let handle = Watchdog::new(FixedSource(source), cfg, halt_tx)
                .expect("valid config")
                .spawn();

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
        let handle = Watchdog::new(FixedSource(stalled_sample()), cfg, halt_tx)
            .expect("valid config")
            .spawn();
        std::thread::sleep(Duration::from_millis(50));

        let started = Instant::now();
        handle.stop().expect("clean stop");
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "stop must not wait out the next tick deadline"
        );
    }

    #[test]
    fn non_finite_queue_pressure_ratio_degrades_only_on_a_full_queue() {
        assert_eq!(queue_pressure_threshold(100, f32::NAN), 100);
        assert_eq!(queue_pressure_threshold(100, f32::INFINITY), 100);
        assert_eq!(queue_pressure_threshold(100, 0.75), 75);
        assert_eq!(queue_pressure_threshold(0, 0.75), usize::MAX);
    }

    /// A halt sent to a channel nobody listens on is not a halt: the handle
    /// must not claim one, or `main` reports an unrecoverable fault that never
    /// triggered a shutdown.
    #[test]
    fn a_halt_nobody_receives_is_not_reported_as_requested() {
        let (halt_tx, halt_rx) = watch::channel(false);
        drop(halt_rx);
        let handle = Watchdog::new(FixedSource(stalled_sample()), config(), halt_tx)
            .expect("valid config")
            .spawn();

        // The loop samples immediately; give it time to attempt the send.
        std::thread::sleep(Duration::from_millis(100));
        assert!(!handle.requested_halt());
        handle.stop().expect("clean stop");
    }

    /// A zero (or sub-millisecond) interval would turn the tick loop into a
    /// busy loop that also hammers the commit ledger and metrics registry.
    #[test]
    fn tick_interval_is_floored_to_avoid_a_busy_loop() {
        assert_eq!(normalized_tick_interval(Duration::ZERO), MIN_TICK_INTERVAL);
        assert_eq!(
            normalized_tick_interval(Duration::from_nanos(1)),
            MIN_TICK_INTERVAL
        );
        assert_eq!(
            normalized_tick_interval(Duration::from_secs(5)),
            Duration::from_secs(5)
        );
    }

    /// Library callers bypass the binary's config-file validation, so an
    /// unusable threshold ordering must fail at construction instead of
    /// spawning an observer whose degraded state can never be reached.
    #[test]
    fn invalid_config_is_rejected_at_construction() {
        let (halt_tx, _halt_rx) = watch::channel(false);
        let config = WatchdogConfig {
            degraded_after: Duration::from_secs(120),
            ..config()
        };
        assert!(matches!(
            Watchdog::new(FixedSource(running_sample()), config, halt_tx),
            Err(WatchdogError::InvalidConfig(_))
        ));
    }
}
