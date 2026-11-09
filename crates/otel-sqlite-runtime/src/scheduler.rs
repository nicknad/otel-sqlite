//! Pure maintenance scheduling logic.
//!
//! The scheduler holds one task per periodic operation and owns all schedule
//! state (`next_due`). It knows nothing about threads or SQLite: a tick takes
//! an injected timestamp and the command-queue sender, which makes every rule
//! below testable without sleeping or creating a database.
//!
//! Invariants implemented here:
//!
//! - **No duplicates**: an operation is only re-enqueued once its interval has
//!   elapsed *after a successful enqueue*, so at most one outstanding instance
//!   normally exists. The state lives in the scheduler — never peeked from the
//!   queue.
//! - **Best-effort under queue pressure**: a full queue defers the operation
//!   by `retry_delay` (it stays due); it does not abort the worker.
//! - **Terminal disconnect**: a closed queue (writer gone) is reported so the
//!   worker can exit instead of spinning against a dead pipeline.

use std::time::{Duration, Instant};

use crossbeam_channel::{Sender, TrySendError};
use otel_sqlite_core::saturating_deadline;
use otel_sqlite_core::storage::{MaintenanceOperation, WriteCommand};

use crate::config::MaintenanceConfig;

/// Floor for the full-queue retry delay. A zero delay would make the worker
/// spin against a saturated queue instead of yielding; the binary rejects
/// zero, but this is a public library type.
const MIN_RETRY_DELAY: Duration = Duration::from_millis(10);

/// Queue depth above which heavy maintenance (VACUUM, index rebuild, a large
/// prune) is deferred. Running it while ingestion waits stalls the single
/// writer for seconds; the scheduler re-arms it at `retry_delay`. This is the
/// only admission policy for maintenance — the writer never silently skips a
/// command it already accepted.
const HEAVY_MAINTENANCE_BACKLOG_LIMIT: usize = 1_000;

/// Outcome of one scheduler tick.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TickOutcome {
    /// The queue is still open; schedule the next tick normally.
    Continue,
    /// The writer is gone: there is nowhere to schedule into anymore.
    Disconnected,
}

/// One periodic maintenance operation and its schedule state.
struct Task {
    operation: MaintenanceOperation,
    /// `None` disables the operation entirely.
    interval: Option<Duration>,
    next_due: Option<Instant>,
    /// True while the last enqueue was deferred by queue pressure. Used to
    /// emit the warning on the transition instead of on every retry.
    deferred: bool,
}

impl Task {
    fn new(operation: MaintenanceOperation, interval: Option<Duration>, now: Instant) -> Self {
        // A zero interval would keep the task permanently due and spin the
        // worker. The binary rejects zero, but library callers may not.
        let interval = interval.filter(|interval| !interval.is_zero());
        Self {
            operation,
            interval,
            // First fire happens one full interval after startup: no burst of
            // maintenance work colliding with ingestion ramp-up.
            next_due: interval.map(|interval| saturating_deadline(now, interval)),
            deferred: false,
        }
    }

    fn due(&self, now: Instant) -> bool {
        self.next_due.is_some_and(|next_due| next_due <= now)
    }

    /// Successful enqueue: the next instance of this operation may only be
    /// enqueued after a full interval. This is what keeps at most one
    /// outstanding instance per operation.
    fn reschedule(&mut self, now: Instant) {
        self.deferred = false;
        if let Some(interval) = self.interval {
            self.next_due = Some(saturating_deadline(now, interval));
        }
    }

    /// Failed enqueue: keep the operation due, but retry only after the
    /// configured delay instead of hammering a full queue.
    fn defer(&mut self, now: Instant, retry_delay: Duration) {
        self.deferred = true;
        self.next_due = Some(saturating_deadline(now, retry_delay));
    }
}

pub(crate) struct Scheduler {
    tasks: Vec<Task>,
    retry_delay: Duration,
}

impl Scheduler {
    pub(crate) fn new(mut config: MaintenanceConfig, now: Instant) -> Self {
        // See `MIN_RETRY_DELAY`: a zero delay would spin `tick` against a full
        // queue. Clamp here so both the worker and the tests share one rule.
        config.retry_delay = config.retry_delay.max(MIN_RETRY_DELAY);

        let mut tasks = Vec::with_capacity(5);
        if let Some(purge) = config.purge {
            tasks.push(Task::new(
                MaintenanceOperation::Prune(purge.retention),
                Some(purge.interval),
                now,
            ));
        }
        tasks.push(Task::new(
            MaintenanceOperation::Checkpoint(config.checkpoint_mode),
            config.checkpoint_interval,
            now,
        ));
        tasks.push(Task::new(
            MaintenanceOperation::Analyze,
            config.optimize_interval,
            now,
        ));
        tasks.push(Task::new(
            MaintenanceOperation::Vacuum,
            config.vacuum_interval,
            now,
        ));
        tasks.push(Task::new(
            MaintenanceOperation::RebuildFts,
            config.rebuild_fts_interval,
            now,
        ));
        Self {
            tasks,
            retry_delay: config.retry_delay,
        }
    }

    /// How many periodic operations are enabled. The worker logs this at
    /// startup; deriving it from the task list keeps the worker from
    /// duplicating schedule knowledge.
    pub(crate) fn enabled_task_count(&self) -> usize {
        self.tasks
            .iter()
            .filter(|task| task.interval.is_some())
            .count()
    }

    /// Enqueues every operation that is due.
    pub(crate) fn tick(&mut self, now: Instant, sink: &Sender<WriteCommand>) -> TickOutcome {
        for task in &mut self.tasks {
            if !task.due(now) {
                continue;
            }
            let operation = task.operation;
            let command = WriteCommand::Maintenance(operation);

            if should_defer_heavy(operation, sink.len()) {
                let first_deferral = !task.deferred;
                task.defer(now, self.retry_delay);
                ::metrics::counter!(
                    "maintenance_enqueue_failures_total",
                    "operation" => operation.as_str(),
                    "reason" => "backlog"
                )
                .increment(1);
                if first_deferral {
                    tracing::warn!(
                        operation = operation.as_str(),
                        queue_depth = sink.len(),
                        retry_in_ms = self.retry_delay.as_millis() as u64,
                        "maintenance deferred; ingestion backlog present"
                    );
                } else {
                    tracing::debug!(
                        operation = operation.as_str(),
                        "maintenance still deferred; ingestion backlog present"
                    );
                }
                continue;
            }

            match sink.try_send(command) {
                Ok(()) => {
                    if task.deferred {
                        tracing::info!(
                            operation = operation.as_str(),
                            "maintenance enqueue recovered after queue pressure"
                        );
                    }
                    task.reschedule(now);
                    ::metrics::counter!(
                        "maintenance_commands_total",
                        "operation" => operation.as_str()
                    )
                    .increment(1);
                    tracing::info!(
                        operation = operation.as_str(),
                        "maintenance command scheduled"
                    );
                }
                Err(TrySendError::Full(command)) => {
                    let first_deferral = !task.deferred;
                    task.defer(now, self.retry_delay);
                    ::metrics::counter!(
                        "maintenance_enqueue_failures_total",
                        "operation" => operation.as_str(),
                        "reason" => "queue_full"
                    )
                    .increment(1);
                    // Warn once per deferral episode: a long saturation would
                    // otherwise emit a warning every `retry_delay`.
                    if first_deferral {
                        tracing::warn!(
                            operation = operation.as_str(),
                            retry_in_ms = self.retry_delay.as_millis() as u64,
                            "maintenance enqueue deferred; command queue full ({})",
                            command.kind()
                        );
                    } else {
                        tracing::debug!(
                            operation = operation.as_str(),
                            "maintenance enqueue still deferred; command queue full ({})",
                            command.kind()
                        );
                    }
                }
                Err(TrySendError::Disconnected(command)) => {
                    ::metrics::counter!(
                        "maintenance_enqueue_failures_total",
                        "operation" => operation.as_str(),
                        "reason" => "queue_closed"
                    )
                    .increment(1);
                    tracing::error!(
                        operation = operation.as_str(),
                        "command queue closed while enqueueing {}",
                        command.kind()
                    );
                    return TickOutcome::Disconnected;
                }
            }
        }

        TickOutcome::Continue
    }

    /// Earliest instant at which some enabled operation becomes due again.
    /// Returns `None` when no periodic operation is enabled (the worker then
    /// block-waits on shutdown only).
    pub(crate) fn next_deadline(&self) -> Option<Instant> {
        self.tasks.iter().filter_map(|task| task.next_due).min()
    }

    #[cfg(test)]
    fn is_due(&self, operation: MaintenanceOperation, now: Instant) -> bool {
        self.tasks
            .iter()
            .any(|task| task.operation == operation && task.due(now))
    }
}

/// Whether this operation should wait for the ingestion backlog to drain.
const fn should_defer_heavy(operation: MaintenanceOperation, queue_depth: usize) -> bool {
    operation.is_heavy() && queue_depth > HEAVY_MAINTENANCE_BACKLOG_LIMIT
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crossbeam_channel::{Receiver, bounded};
    use otel_sqlite_core::storage::{CheckpointMode, MaintenanceOperation, WriteCommand};

    use super::*;
    use crate::config::PurgeSchedule;

    #[test]
    fn heavy_operations_wait_for_backlog_but_cheap_ones_do_not() {
        assert!(!should_defer_heavy(
            MaintenanceOperation::Analyze,
            usize::MAX
        ));
        assert!(!should_defer_heavy(
            MaintenanceOperation::Vacuum,
            HEAVY_MAINTENANCE_BACKLOG_LIMIT
        ));
        assert!(should_defer_heavy(
            MaintenanceOperation::Vacuum,
            HEAVY_MAINTENANCE_BACKLOG_LIMIT + 1
        ));
        assert!(should_defer_heavy(
            MaintenanceOperation::Prune(Duration::from_secs(3_600)),
            HEAVY_MAINTENANCE_BACKLOG_LIMIT + 1
        ));
    }

    fn drain(receiver: &Receiver<WriteCommand>) -> Vec<WriteCommand> {
        receiver.try_iter().collect()
    }

    fn assert_drained(receiver: &Receiver<WriteCommand>) {
        assert_eq!(drain(receiver), Vec::new(), "queue must stay empty");
    }

    fn kinds(commands: &[WriteCommand]) -> Vec<&'static str> {
        commands.iter().map(WriteCommand::kind).collect()
    }

    fn config(purge: Option<Duration>) -> MaintenanceConfig {
        MaintenanceConfig {
            purge: purge.map(|interval| PurgeSchedule {
                retention: Duration::from_secs(3_600),
                interval,
            }),
            ..MaintenanceConfig::default()
        }
    }

    #[test]
    fn purge_becomes_due_after_its_interval() {
        let base = Instant::now();
        let mut scheduler = Scheduler::new(config(Some(Duration::from_secs(1))), base);
        let (sink, enqueued) = bounded::<WriteCommand>(16);
        let prune = MaintenanceOperation::Prune(Duration::from_secs(3_600));

        assert!(!scheduler.is_due(prune, base));
        assert!(!scheduler.is_due(prune, base + Duration::from_millis(999)));
        assert!(scheduler.is_due(prune, base + Duration::from_secs(1)));

        assert_eq!(
            scheduler.tick(base + Duration::from_secs(1), &sink),
            TickOutcome::Continue
        );
        assert_eq!(
            drain(&enqueued),
            vec![WriteCommand::Maintenance(prune)],
            "the configured retention window must ride on the prune command"
        );
    }

    #[test]
    fn first_fire_waits_a_full_interval_after_startup() {
        let base = Instant::now();
        let mut scheduler = Scheduler::new(config(Some(Duration::from_secs(60))), base);
        let (sink, enqueued) = bounded::<WriteCommand>(16);

        scheduler.tick(base, &sink);
        assert_drained(&enqueued);
    }

    #[test]
    fn operations_follow_independent_schedules() {
        let cfg = MaintenanceConfig {
            purge: Some(PurgeSchedule {
                retention: Duration::from_secs(60),
                interval: Duration::from_secs(1),
            }),
            checkpoint_interval: Some(Duration::from_secs(3)),
            optimize_interval: None,
            vacuum_interval: Some(Duration::from_secs(5)),
            ..MaintenanceConfig::default()
        };
        let base = Instant::now();
        let mut scheduler = Scheduler::new(cfg, base);
        let (sink, enqueued) = bounded::<WriteCommand>(16);

        scheduler.tick(base + Duration::from_secs(1), &sink);
        assert_eq!(enqueued.len(), 1);

        scheduler.tick(base + Duration::from_secs(3), &sink);
        assert_eq!(enqueued.len(), 3, "purge again plus checkpoint");

        scheduler.tick(base + Duration::from_secs(5), &sink);
        let commands = drain(&enqueued);
        assert_eq!(
            kinds(&commands),
            vec![
                "purge",      // @1s
                "purge",      // @3s
                "checkpoint", // @3s
                "purge",      // @5s
                "vacuum",     // @5s
            ]
        );
    }

    #[test]
    fn no_duplicate_command_within_one_interval() {
        let base = Instant::now();
        let mut scheduler = Scheduler::new(config(Some(Duration::from_secs(2))), base);
        let (sink, enqueued) = bounded::<WriteCommand>(16);

        scheduler.tick(base + Duration::from_secs(2), &sink);
        for ms in [2_100u64, 2_500, 2_900, 3_500, 3_900] {
            scheduler.tick(base + Duration::from_millis(ms), &sink);
            assert_eq!(enqueued.len(), 1, "no duplicate before interval elapses");
        }

        scheduler.tick(base + Duration::from_secs(4), &sink);
        assert_eq!(drain(&enqueued).len(), 2);
    }

    #[test]
    fn full_queue_defers_operation_without_losing_it() {
        let base = Instant::now();
        let mut scheduler = Scheduler::new(
            MaintenanceConfig {
                retry_delay: Duration::from_millis(500),
                ..config(Some(Duration::from_secs(10)))
            },
            base,
        );
        // A zero-capacity (rendezvous) channel has no slot until a receiver
        // waits on it, so `try_send` reports Full exactly like a saturated
        // bounded queue; the idle receiver just never accepts anything.
        let (saturated, idle) = bounded::<WriteCommand>(0);

        scheduler.tick(base + Duration::from_secs(10), &saturated);
        assert_drained(&idle);

        // Still within the retry window: nothing is re-attempted yet, and no
        // duplicate appears.
        scheduler.tick(base + Duration::new(10, 400_000_000), &saturated);
        assert_drained(&idle);

        // Pressure gone: the pending operation goes through exactly once.
        let (sink, enqueued) = bounded::<WriteCommand>(16);
        scheduler.tick(base + Duration::from_millis(10_500), &sink);
        let mut commands = drain(&enqueued);
        assert_eq!(commands.len(), 1);

        // Normal cadence resumes from the successful enqueue.
        scheduler.tick(base + Duration::from_secs(19), &sink);
        assert_drained(&enqueued);
        scheduler.tick(base + Duration::from_millis(20_500), &sink);
        commands.extend(drain(&enqueued));
        assert_eq!(commands.len(), 2);
    }

    #[test]
    fn disconnected_queue_is_terminal_for_the_tick() {
        let base = Instant::now();
        let mut scheduler = Scheduler::new(config(Some(Duration::from_secs(1))), base);
        let (sink, receiver) = bounded::<WriteCommand>(16);
        drop(receiver);

        assert_eq!(
            scheduler.tick(base + Duration::from_secs(1), &sink),
            TickOutcome::Disconnected
        );
    }

    #[test]
    fn disabled_operations_never_schedule() {
        let cfg = MaintenanceConfig {
            purge: None,
            checkpoint_interval: None,
            optimize_interval: None,
            vacuum_interval: None,
            rebuild_fts_interval: None,
            ..MaintenanceConfig::default()
        };
        let base = Instant::now();
        let mut scheduler = Scheduler::new(cfg, base);
        let (sink, enqueued) = bounded::<WriteCommand>(16);

        scheduler.tick(base + Duration::from_secs(1_000), &sink);
        assert_eq!(scheduler.next_deadline(), None);
        assert_eq!(scheduler.enabled_task_count(), 0);
        assert_drained(&enqueued);
    }

    #[test]
    fn next_deadline_is_the_nearest_enabled_schedule() {
        let cfg = MaintenanceConfig {
            purge: Some(PurgeSchedule {
                retention: Duration::from_secs(60),
                interval: Duration::from_secs(10),
            }),
            checkpoint_interval: Some(Duration::from_secs(300)),
            optimize_interval: None,
            vacuum_interval: Some(Duration::from_secs(86_400)),
            ..MaintenanceConfig::default()
        };
        let base = Instant::now();
        let scheduler = Scheduler::new(cfg, base);

        assert_eq!(
            scheduler.next_deadline(),
            Some(base + Duration::from_secs(10))
        );
    }

    #[test]
    fn built_commands_carry_configured_parameters() {
        let cfg = MaintenanceConfig {
            purge: Some(PurgeSchedule {
                retention: Duration::from_secs(90),
                interval: Duration::from_secs(1),
            }),
            checkpoint_mode: CheckpointMode::Restart,
            checkpoint_interval: Some(Duration::from_secs(1)),
            optimize_interval: Some(Duration::from_secs(1)),
            vacuum_interval: Some(Duration::from_secs(1)),
            rebuild_fts_interval: Some(Duration::from_secs(1)),
            retry_delay: Duration::from_secs(1),
        };
        let base = Instant::now();
        let mut scheduler = Scheduler::new(cfg, base);
        let (sink, enqueued) = bounded::<WriteCommand>(16);

        scheduler.tick(base + Duration::from_secs(1), &sink);
        let commands = drain(&enqueued);

        for operation in [
            MaintenanceOperation::Prune(Duration::from_secs(90)),
            MaintenanceOperation::Checkpoint(CheckpointMode::Restart),
            MaintenanceOperation::Analyze,
            MaintenanceOperation::Vacuum,
            MaintenanceOperation::RebuildFts,
        ] {
            assert!(
                commands.contains(&WriteCommand::Maintenance(operation)),
                "missing {operation:?} in {commands:?}"
            );
        }
    }

    #[test]
    fn rebuild_fts_follows_its_own_schedule() {
        let cfg = MaintenanceConfig {
            purge: None,
            checkpoint_interval: None,
            optimize_interval: None,
            vacuum_interval: None,
            rebuild_fts_interval: Some(Duration::from_secs(30)),
            ..MaintenanceConfig::default()
        };
        let base = Instant::now();
        let mut scheduler = Scheduler::new(cfg, base);
        let (sink, enqueued) = bounded::<WriteCommand>(16);

        scheduler.tick(base, &sink);
        assert_drained(&enqueued);

        scheduler.tick(base + Duration::from_secs(30), &sink);
        assert_eq!(
            drain(&enqueued),
            vec![WriteCommand::Maintenance(MaintenanceOperation::RebuildFts)]
        );
        assert_eq!(scheduler.enabled_task_count(), 1);
    }

    /// The binary rejects zero durations, but `MaintenanceConfig` is public:
    /// a zero interval must disable the task, never make it permanently due.
    #[test]
    fn zero_intervals_are_disabled_instead_of_spinning() {
        let cfg = MaintenanceConfig {
            purge: Some(PurgeSchedule {
                retention: Duration::from_secs(60),
                interval: Duration::ZERO,
            }),
            checkpoint_interval: Some(Duration::ZERO),
            optimize_interval: Some(Duration::ZERO),
            vacuum_interval: Some(Duration::ZERO),
            rebuild_fts_interval: Some(Duration::ZERO),
            retry_delay: Duration::ZERO,
            ..MaintenanceConfig::default()
        };
        let base = Instant::now();
        let mut scheduler = Scheduler::new(cfg, base);
        let (sink, enqueued) = bounded::<WriteCommand>(16);

        assert_eq!(scheduler.enabled_task_count(), 0);
        assert_eq!(scheduler.next_deadline(), None);
        scheduler.tick(base, &sink);
        assert_drained(&enqueued);
    }

    #[test]
    fn zero_retry_delay_is_lifted_to_a_floor() {
        let base = Instant::now();
        let mut scheduler = Scheduler::new(
            MaintenanceConfig {
                purge: Some(PurgeSchedule {
                    retention: Duration::from_secs(60),
                    interval: Duration::from_secs(1),
                }),
                checkpoint_interval: None,
                optimize_interval: None,
                vacuum_interval: None,
                rebuild_fts_interval: None,
                retry_delay: Duration::ZERO,
                ..MaintenanceConfig::default()
            },
            base,
        );
        let (saturated, idle) = bounded::<WriteCommand>(0);

        scheduler.tick(base + Duration::from_secs(1), &saturated);
        assert_drained(&idle);
        assert_eq!(
            scheduler.next_deadline(),
            Some(base + Duration::from_secs(1) + MIN_RETRY_DELAY)
        );
    }

    #[test]
    fn repeated_deferrals_keep_the_operation_due_until_the_queue_drains() {
        let base = Instant::now();
        let mut scheduler = Scheduler::new(
            MaintenanceConfig {
                purge: Some(PurgeSchedule {
                    retention: Duration::from_secs(60),
                    interval: Duration::from_secs(1),
                }),
                checkpoint_interval: None,
                optimize_interval: None,
                vacuum_interval: None,
                rebuild_fts_interval: None,
                retry_delay: Duration::from_millis(100),
                ..MaintenanceConfig::default()
            },
            base,
        );
        let (saturated, idle) = bounded::<WriteCommand>(0);
        for tick in 1..=5u64 {
            scheduler.tick(base + Duration::from_secs(tick), &saturated);
            assert_drained(&idle);
        }

        let (sink, enqueued) = bounded::<WriteCommand>(16);
        scheduler.tick(base + Duration::from_secs(6), &sink);
        assert_eq!(drain(&enqueued).len(), 1);
    }
}
