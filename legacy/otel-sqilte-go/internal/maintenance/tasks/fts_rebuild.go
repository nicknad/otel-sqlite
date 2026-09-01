package tasks

import (
	"context"
	"sync"
	"time"

	"codeberg.org/nicknad/otel-sqlite/internal/maintenance"
	"codeberg.org/nicknad/otel-sqlite/internal/storage/sqlite"
)

// FtsRebuildTask periodically rebuilds the logs_fts contentless full-text
// index from the logs view.  The rebuild runs serialized with ingestion on
// the single writer, guaranteeing consistency between the base tables and
// the FTS index.
//
// Full rebuilds also serve as GC for entries orphaned by retention deletes,
// since contentless FTS5 tables do not support incremental DELETE.
type FtsRebuildTask struct {
	enabled  bool
	interval time.Duration

	mu      sync.Mutex
	lastRun time.Time
}

// NewFtsRebuildTask creates a new FTS rebuild task.
func NewFtsRebuildTask(enabled bool, interval time.Duration) *FtsRebuildTask {
	return &FtsRebuildTask{
		enabled:  enabled,
		interval: interval,
	}
}

// Name returns the task name for metrics and logging.
func (t *FtsRebuildTask) Name() string { return "fts_rebuild" }

// Enabled reports whether FTS rebuild is configured to run.
func (t *FtsRebuildTask) Enabled() bool { return t.enabled }

// Due returns true if enough time has elapsed since the last rebuild.
func (t *FtsRebuildTask) Due(now time.Time) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.lastRun.IsZero() {
		return true
	}
	return now.Sub(t.lastRun) >= t.interval
}

// Run creates and submits a RebuildFtsCommand.
func (t *FtsRebuildTask) Run(ctx context.Context, submitter maintenance.CommandSubmitter) error {
	cmd := sqlite.NewRebuildFtsCommand()

	if err := submitter.Submit(ctx, cmd); err != nil {
		return err
	}

	t.mu.Lock()
	t.lastRun = time.Now()
	t.mu.Unlock()

	return nil
}

// Ensure FtsRebuildTask satisfies MaintenanceTask at compile time.
var _ maintenance.MaintenanceTask = (*FtsRebuildTask)(nil)
