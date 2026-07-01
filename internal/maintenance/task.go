// Package maintenance provides a generic, pluggable maintenance framework
// that schedules and executes database maintenance tasks.
//
// The framework is fully decoupled from any storage implementation.
// Tasks produce commands that are submitted via CommandSubmitter; all
// database mutations flow through the existing Command Queue → SQLite Writer
// pipeline.
package maintenance

import (
	"context"
	"time"

	"codeberg.org/nicknad/otel-sqlite/internal/storage"
)

// MaintenanceTask is the interface that all maintenance tasks must implement.
//
// Tasks are pluggable and independent. Each task manages its own scheduling
// state internally. Tasks must NOT execute SQL or touch the database directly;
// all side effects are produced by submitting storage.Command objects via
// CommandSubmitter.
type MaintenanceTask interface {
	// Name returns a unique, human-readable name for this task.
	// Used in logs and metrics labels.
	Name() string

	// Enabled reports whether this task should be considered for execution.
	// Disabled tasks are skipped by the worker.
	Enabled() bool

	// Due reports whether the task should run at the given time.
	// The task manages its own scheduling state (e.g., last run timestamp).
	Due(now time.Time) bool

	// Run executes the maintenance task. The task must produce its side
	// effects by calling submitter.Submit with storage.Command objects.
	// Run may block; the worker does not impose a timeout.
	Run(ctx context.Context, submitter CommandSubmitter) error
}

// CommandSubmitter is the only allowed interface for producing side effects
// from maintenance tasks. It is intentionally minimal: tasks can submit
// commands but have no access to queues, channels, or database internals.
//
// It is structurally identical to storage.CommandExecutor to allow the
// SQLite writer to satisfy it directly.
type CommandSubmitter interface {
	// Submit enqueues a command for execution by the SQLite writer.
	// Blocks until space is available or the context is canceled.
	Submit(ctx context.Context, cmd storage.Command) error
}

// Clock abstracts time for testability. Production code uses the real clock
// (time.Now); tests inject a fake clock for deterministic scheduling.
type Clock interface {
	Now() time.Time
}

// RealClock implements Clock using the system wall clock.
type RealClock struct{}

func (RealClock) Now() time.Time { return time.Now() }
