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
use otel_sqlite_core::storage::{MaintenanceOperation, WriteCommand};

use crate::config::MaintenanceConfig;

/// Periodic maintenance operations supported by the worker.
///
/// Each maps onto an existing writer command variant; none of them carry SQL.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Operation {
    Purge,
    Checkpoint,
    Optimize,
    Vacuum,
    RebuildFts,
}

impl Operation {
    pub(crate) const fn label(self) -> &'static str {
        match self {
            Self::Purge => "purge",
            Self::Checkpoint => "checkpoint",
            Self::Optimize => "optimize",
            Self::Vacuum => "vacuum",
            Self::RebuildFts => "rebuild_fts",
        }
    }

    /// Builds the writer command for this operation.
    ///
    /// All parameters (retention windows, checkpoint mode) come from
    /// configuration; the command itself is interpreted exclusively by the
    /// SQLite writer.
    fn build(self, config: &MaintenanceConfig) -> WriteCommand {
        match self {
            Self::Purge => WriteCommand::Maintenance(MaintenanceOperation::Prune(config.retention)),
            Self::Checkpoint => WriteCommand::Checkpoint(config.checkpoint_mode),
            Self::Optimize => WriteCommand::Maintenance(MaintenanceOperation::Analyze),
            Self::Vacuum => WriteCommand::Maintenance(MaintenanceOperation::Vacuum),
            Self::RebuildFts => WriteCommand::Maintenance(MaintenanceOperation::RebuildFts),
        }
    }
}

/// Outcome of one scheduler tick.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct TickReport {
    /// Commands successfully enqueued during this tick.
    pub scheduled: usize,
    /// Operations that were due but deferred because the queue was full.
    pub deferred: usize,
    /// True when the queue was disconnected (writer gone): terminal.
    pub disconnected: bool,
}

/// `now + duration`, saturated to the representable range instead of
/// panicking on extreme configuration values.
fn schedule_instant(now: Instant, duration: Duration) -> Instant {
    const TEN_YEARS: Duration = Duration::from_secs(60 * 60 * 24 * 365 * 10);
    now.checked_add(duration)
        .or_else(|| now.checked_add(TEN_YEARS))
        .unwrap_or(now)
}

struct Task {
    operation: Operation,
    /// `None` disables the operation entirely.
    interval: Option<Duration>,
    next_due: Option<Instant>,
}

impl Task {
    fn new(operation: Operation, interval: Option<Duration>, now: Instant) -> Self {
        Self {
            operation,
            interval,
            // First fire happens one full interval after startup: no burst of
            // maintenance work colliding with ingestion ramp-up.
            next_due: interval.map(|interval| schedule_instant(now, interval)),
        }
    }

    fn due(&self, now: Instant) -> bool {
        self.next_due.is_some_and(|next_due| next_due <= now)
    }

    /// Successful enqueue: the next instance of this operation may only be
    /// enqueued after a full interval. This is what keeps at most one
    /// outstanding instance per operation.
    fn reschedule(&mut self, now: Instant) {
        if let Some(interval) = self.interval {
            self.next_due = Some(schedule_instant(now, interval));
        }
    }

    /// Failed enqueue: keep the operation due, but retry only after the
    /// configured delay instead of hammering a full queue.
    fn defer(&mut self, now: Instant, retry_delay: Duration) {
        self.next_due = Some(schedule_instant(now, retry_delay));
    }
}

pub(crate) struct Scheduler {
    tasks: Vec<Task>,
    config: MaintenanceConfig,
}

impl Scheduler {
    pub(crate) fn new(config: MaintenanceConfig, now: Instant) -> Self {
        let tasks = vec![
            Task::new(Operation::Purge, purge_interval(&config), now),
            Task::new(Operation::Checkpoint, config.checkpoint_interval, now),
            Task::new(Operation::Optimize, config.optimize_interval, now),
            Task::new(Operation::Vacuum, config.vacuum_interval, now),
            Task::new(Operation::RebuildFts, config.rebuild_fts_interval, now),
        ];
        Self { tasks, config }
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

    /// Enqueues every operation that is due, then reports what happened.
    pub(crate) fn tick(&mut self, now: Instant, sink: &Sender<WriteCommand>) -> TickReport {
        let mut report = TickReport::default();

        for task in &mut self.tasks {
            if !task.due(now) {
                continue;
            }
            let operation = task.operation;
            let command = operation.build(&self.config);

            match sink.try_send(command) {
                Ok(()) => {
                    task.reschedule(now);
                    report.scheduled += 1;
                    ::metrics::counter!(
                        "maintenance_commands_total",
                        "operation" => operation.label()
                    )
                    .increment(1);
                    tracing::info!(
                        operation = operation.label(),
                        "maintenance command scheduled"
                    );
                }
                Err(TrySendError::Full(command)) => {
                    task.defer(now, self.config.retry_delay);
                    report.deferred += 1;
                    ::metrics::counter!(
                        "maintenance_enqueue_failures_total",
                        "operation" => operation.label(),
                        "reason" => "queue_full"
                    )
                    .increment(1);
                    tracing::warn!(
                        operation = operation.label(),
                        retry_in_ms = self.config.retry_delay.as_millis() as u64,
                        "maintenance enqueue deferred; command queue full ({})",
                        command.kind()
                    );
                }
                Err(TrySendError::Disconnected(command)) => {
                    report.disconnected = true;
                    ::metrics::counter!(
                        "maintenance_enqueue_failures_total",
                        "operation" => operation.label(),
                        "reason" => "queue_closed"
                    )
                    .increment(1);
                    tracing::error!(
                        operation = operation.label(),
                        "command queue closed while enqueueing {}",
                        command.kind()
                    );
                    return report;
                }
            }
        }

        report
    }

    /// Earliest instant at which some enabled operation becomes due again.
    /// Returns `None` when no periodic operation is enabled (the worker then
    /// block-waits on shutdown only).
    pub(crate) fn next_deadline(&self) -> Option<Instant> {
        self.tasks.iter().filter_map(|task| task.next_due).min()
    }

    #[cfg(test)]
    fn is_due(&self, operation: Operation, now: Instant) -> bool {
        self.tasks
            .iter()
            .any(|task| task.operation == operation && task.due(now))
    }
}

/// Purge cadence: enabled when a retention window is set. The enqueued prune
/// command carries that window; `None` never schedules.
fn purge_interval(config: &MaintenanceConfig) -> Option<Duration> {
    config.retention?;
    config.purge_interval
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crossbeam_channel::{Receiver, bounded};
    use otel_sqlite_core::storage::{CheckpointMode, MaintenanceOperation, WriteCommand};

    use super::*;

    /// Drains every command the scheduler enqueued so far.
    fn drain(receiver: &Receiver<WriteCommand>) -> Vec<WriteCommand> {
        receiver.try_iter().collect()
    }

    fn kinds(commands: &[WriteCommand]) -> Vec<&'static str> {
        commands.iter().map(WriteCommand::kind).collect()
    }

    fn config(purge: Option<Duration>) -> MaintenanceConfig {
        MaintenanceConfig {
            retention: Some(Duration::from_secs(3_600)),
            purge_interval: purge,
            ..MaintenanceConfig::default()
        }
    }

    #[test]
    fn purge_becomes_due_after_its_interval() {
        let base = Instant::now();
        let mut scheduler = Scheduler::new(config(Some(Duration::from_secs(1))), base);
        let (sink, enqueued) = bounded::<WriteCommand>(16);

        assert!(!scheduler.is_due(Operation::Purge, base));
        assert!(!scheduler.is_due(Operation::Purge, base + Duration::from_millis(999)));
        assert!(scheduler.is_due(Operation::Purge, base + Duration::from_secs(1)));

        let report = scheduler.tick(base + Duration::from_secs(1), &sink);
        assert_eq!(report.scheduled, 1);
        let commands = drain(&enqueued);
        assert_eq!(kinds(&commands), vec!["maintenance"]);
        let WriteCommand::Maintenance(MaintenanceOperation::Prune(window)) = &commands[0] else {
            panic!("expected a prune command");
        };
        assert_eq!(
            window,
            &Some(Duration::from_secs(3_600)),
            "the configured retention window must ride on the prune command"
        );
    }

    #[test]
    fn first_fire_waits_a_full_interval_after_startup() {
        let base = Instant::now();
        let mut scheduler = Scheduler::new(config(Some(Duration::from_secs(60))), base);
        let (sink, enqueued) = bounded::<WriteCommand>(16);

        let report = scheduler.tick(base, &sink);
        assert_eq!(report, TickReport::default());
        assert!(drain(&enqueued).is_empty());
    }

    #[test]
    fn operations_follow_independent_schedules() {
        let cfg = MaintenanceConfig {
            retention: Some(Duration::from_secs(60)),
            purge_interval: Some(Duration::from_secs(1)),
            checkpoint_interval: Some(Duration::from_secs(3)),
            optimize_interval: None,
            vacuum_interval: Some(Duration::from_secs(5)),
            ..MaintenanceConfig::default()
        };
        let base = Instant::now();
        let mut scheduler = Scheduler::new(cfg, base);
        let (sink, enqueued) = bounded::<WriteCommand>(16);

        let report = scheduler.tick(base + Duration::from_secs(1), &sink);
        assert_eq!(report.scheduled, 1);

        let report = scheduler.tick(base + Duration::from_secs(3), &sink);
        assert_eq!(report.scheduled, 2, "purge (due again) plus checkpoint");

        let report = scheduler.tick(base + Duration::from_secs(5), &sink);
        assert_eq!(report.scheduled, 2, "purge (due again) plus vacuum");

        let commands = drain(&enqueued);
        assert_eq!(
            kinds(&commands),
            vec![
                "maintenance", // purge @1s
                "maintenance", // purge @3s
                "checkpoint",  // @3s
                "maintenance", // purge @5s
                "maintenance", // vacuum @5s
            ]
        );
        assert_eq!(
            kinds(&commands)
                .iter()
                .filter(|k| **k == "checkpoint")
                .count(),
            1
        );
    }

    #[test]
    fn no_duplicate_command_within_one_interval() {
        let base = Instant::now();
        let mut scheduler = Scheduler::new(config(Some(Duration::from_secs(2))), base);
        let (sink, enqueued) = bounded::<WriteCommand>(16);

        scheduler.tick(base + Duration::from_secs(2), &sink);
        for ms in [2_100u64, 2_500, 2_900, 3_500, 3_900] {
            let report = scheduler.tick(base + Duration::from_millis(ms), &sink);
            assert_eq!(report.scheduled, 0, "no duplicate before interval elapses");
        }

        let report = scheduler.tick(base + Duration::from_secs(4), &sink);
        assert_eq!(report.scheduled, 1);
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

        let report = scheduler.tick(base + Duration::from_secs(10), &saturated);
        assert_eq!(report.deferred, 1);
        assert_eq!(report.scheduled, 0);
        assert!(drain(&idle).is_empty());

        // Still within the retry window: nothing is re-attempted yet, and no
        // duplicate appears.
        let report = scheduler.tick(base + Duration::new(10, 400_000_000), &saturated);
        assert_eq!(report.scheduled, 0);
        assert!(drain(&idle).is_empty());

        // Pressure gone: the pending operation goes through exactly once.
        let (sink, enqueued) = bounded::<WriteCommand>(16);
        let report = scheduler.tick(base + Duration::from_millis(10_500), &sink);
        assert_eq!(report.scheduled, 1);
        let mut commands = drain(&enqueued);
        assert_eq!(commands.len(), 1);

        // Normal cadence resumes from the successful enqueue.
        let report = scheduler.tick(base + Duration::from_secs(19), &sink);
        assert_eq!(report.scheduled, 0);
        let report = scheduler.tick(base + Duration::from_millis(20_500), &sink);
        assert_eq!(report.scheduled, 1);
        commands.extend(drain(&enqueued));
        assert_eq!(commands.len(), 2);
    }

    #[test]
    fn disconnected_queue_is_terminal_for_the_tick() {
        let base = Instant::now();
        let mut scheduler = Scheduler::new(config(Some(Duration::from_secs(1))), base);
        let (sink, receiver) = bounded::<WriteCommand>(16);
        drop(receiver); // the writer is gone

        let report = scheduler.tick(base + Duration::from_secs(1), &sink);
        assert!(report.disconnected);
        assert_eq!(report.scheduled, 0);
    }

    #[test]
    fn disabled_operations_never_schedule() {
        let cfg = MaintenanceConfig {
            retention: None,
            purge_interval: Some(Duration::from_secs(1)), // ignored without retention
            checkpoint_interval: None,
            optimize_interval: None,
            vacuum_interval: None,
            ..MaintenanceConfig::default()
        };
        let base = Instant::now();
        let mut scheduler = Scheduler::new(cfg, base);
        let (sink, enqueued) = bounded::<WriteCommand>(16);

        let report = scheduler.tick(base + Duration::from_secs(1_000), &sink);
        assert_eq!(report, TickReport::default());
        assert_eq!(scheduler.next_deadline(), None);
        assert!(drain(&enqueued).is_empty());
    }

    #[test]
    fn next_deadline_is_the_nearest_enabled_schedule() {
        let cfg = MaintenanceConfig {
            retention: Some(Duration::from_secs(60)),
            purge_interval: Some(Duration::from_secs(10)),
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
            retention: Some(Duration::from_secs(90)),
            purge_interval: Some(Duration::from_secs(1)),
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

        assert!(
            commands.contains(&WriteCommand::Maintenance(MaintenanceOperation::Prune(
                Some(Duration::from_secs(90))
            )))
        );
        assert!(commands.contains(&WriteCommand::Checkpoint(CheckpointMode::Restart)));
        assert!(commands.contains(&WriteCommand::Maintenance(MaintenanceOperation::Analyze)));
        assert!(commands.contains(&WriteCommand::Maintenance(MaintenanceOperation::Vacuum)));
        assert!(commands.contains(&WriteCommand::Maintenance(MaintenanceOperation::RebuildFts)));
    }

    #[test]
    fn rebuild_fts_follows_its_own_schedule() {
        let cfg = MaintenanceConfig {
            retention: None,
            purge_interval: None,
            checkpoint_interval: None,
            optimize_interval: None,
            vacuum_interval: None,
            rebuild_fts_interval: Some(Duration::from_secs(30)),
            ..MaintenanceConfig::default()
        };
        let base = Instant::now();
        let mut scheduler = Scheduler::new(cfg, base);
        let (sink, enqueued) = bounded::<WriteCommand>(16);

        let report = scheduler.tick(base, &sink);
        assert_eq!(report.scheduled, 0);

        let report = scheduler.tick(base + Duration::from_secs(30), &sink);
        assert_eq!(report.scheduled, 1);
        assert_eq!(
            drain(&enqueued),
            vec![WriteCommand::Maintenance(MaintenanceOperation::RebuildFts)]
        );
        assert_eq!(scheduler.enabled_task_count(), 1);
    }
}
