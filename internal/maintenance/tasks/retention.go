// Package tasks provides built-in maintenance task implementations.
//
// Each task is self-contained: it knows its own schedule, creates its own
// commands, and submits them through the CommandSubmitter interface.
// Tasks import storage-specific command types (e.g., sqlite.PurgeLogsCommand)
// but the maintenance framework itself has no such dependency.
package tasks

import (
	"context"
	"sync"
	"time"

	"codeberg.org/nicknad/otel-sqlite/internal/maintenance"
	"codeberg.org/nicknad/otel-sqlite/internal/storage/sqlite"
)

// RetentionTask periodically purges log records older than a configured
// retention period.
type RetentionTask struct {
	enabled         bool
	keepLogs        time.Duration
	cleanupInterval time.Duration
	batchSize       int

	mu      sync.Mutex
	lastRun time.Time
}

// NewRetentionTask creates a new retention task.
func NewRetentionTask(enabled bool, keepLogs, cleanupInterval time.Duration, batchSize int) *RetentionTask {
	return &RetentionTask{
		enabled:         enabled,
		keepLogs:        keepLogs,
		cleanupInterval: cleanupInterval,
		batchSize:       batchSize,
	}
}

// Name returns the task name for metrics and logging.
func (t *RetentionTask) Name() string {
	return "retention"
}

// Enabled reports whether retention is configured to run.
func (t *RetentionTask) Enabled() bool {
	return t.enabled
}

// Due returns true if enough time has elapsed since the last run.
func (t *RetentionTask) Due(now time.Time) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.lastRun.IsZero() {
		return true
	}
	return now.Sub(t.lastRun) >= t.cleanupInterval
}

// Run creates and submits a PurgeLogsCommand.
func (t *RetentionTask) Run(ctx context.Context, submitter maintenance.CommandSubmitter) error {
	cmd := sqlite.NewPurgeLogsCommand(t.keepLogs, t.batchSize)

	if err := submitter.Submit(ctx, cmd); err != nil {
		return err
	}

	t.mu.Lock()
	t.lastRun = time.Now()
	t.mu.Unlock()

	return nil
}

// Ensure RetentionTask satisfies MaintenanceTask at compile time.
var _ maintenance.MaintenanceTask = (*RetentionTask)(nil)
