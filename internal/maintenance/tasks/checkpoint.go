// Package tasks provides built-in maintenance task implementations.
//
// Each task is self-contained: it knows its own schedule, creates its own
// commands, and submits them through the CommandSubmitter interface.
package tasks

import (
	"context"
	"sync"
	"time"

	"codeberg.org/nicknad/otel-sqlite/internal/maintenance"
	"codeberg.org/nicknad/otel-sqlite/internal/storage/sqlite"
)

// CheckpointTask periodically triggers SQLite WAL checkpoint operations
// to keep the WAL file size bounded.
//
// The task does NOT execute SQL or touch SQLite directly. It creates a
// CheckpointCommand and submits it via CommandSubmitter.
type CheckpointTask struct {
	enabled  bool
	mode     sqlite.CheckpointMode
	interval time.Duration

	mu      sync.Mutex
	lastRun time.Time
}

// NewCheckpointTask creates a new CheckpointTask.
// If mode is empty, CheckpointPassive is used.
func NewCheckpointTask(enabled bool, mode sqlite.CheckpointMode, interval time.Duration) *CheckpointTask {
	if mode == "" {
		mode = sqlite.CheckpointPassive
	}
	return &CheckpointTask{
		enabled:  enabled,
		mode:     mode,
		interval: interval,
	}
}

// Name returns the task name for metrics and logging.
func (t *CheckpointTask) Name() string { return "checkpoint" }

// Enabled reports whether checkpointing is configured to run.
func (t *CheckpointTask) Enabled() bool { return t.enabled }

// Due returns true if enough time has elapsed since the last checkpoint.
func (t *CheckpointTask) Due(now time.Time) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.lastRun.IsZero() {
		return true
	}
	return now.Sub(t.lastRun) >= t.interval
}

// Run creates and submits a CheckpointCommand with the configured mode.
func (t *CheckpointTask) Run(ctx context.Context, submitter maintenance.CommandSubmitter) error {
	cmd := sqlite.NewCheckpointCommand(t.mode)

	if err := submitter.Submit(ctx, cmd); err != nil {
		return err
	}

	t.mu.Lock()
	t.lastRun = time.Now()
	t.mu.Unlock()

	return nil
}

// Ensure CheckpointTask satisfies MaintenanceTask at compile time.
var _ maintenance.MaintenanceTask = (*CheckpointTask)(nil)
