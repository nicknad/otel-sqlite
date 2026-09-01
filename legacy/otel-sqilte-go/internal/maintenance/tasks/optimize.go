package tasks

import (
	"context"
	"sync"
	"time"

	"codeberg.org/nicknad/otel-sqlite/internal/maintenance"
	"codeberg.org/nicknad/otel-sqlite/internal/storage/sqlite"
)

// OptimizeTask periodically triggers SQLite query planner optimization
// (PRAGMA optimize). This is a lightweight maintenance operation that
// updates statistics used by the query planner.
//
// The task does NOT execute SQL or touch SQLite directly. It creates an
// OptimizeCommand and submits it via CommandSubmitter.
type OptimizeTask struct {
	enabled  bool
	interval time.Duration

	mu      sync.Mutex
	lastRun time.Time
}

// NewOptimizeTask creates a new OptimizeTask.
func NewOptimizeTask(enabled bool, interval time.Duration) *OptimizeTask {
	return &OptimizeTask{
		enabled:  enabled,
		interval: interval,
	}
}

// Name returns the task name for metrics and logging.
func (t *OptimizeTask) Name() string { return "optimize" }

// Enabled reports whether optimization is configured to run.
func (t *OptimizeTask) Enabled() bool { return t.enabled }

// Due returns true if enough time has elapsed since the last optimization.
func (t *OptimizeTask) Due(now time.Time) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.lastRun.IsZero() {
		return true
	}
	return now.Sub(t.lastRun) >= t.interval
}

// Run creates and submits an OptimizeCommand.
func (t *OptimizeTask) Run(ctx context.Context, submitter maintenance.CommandSubmitter) error {
	cmd := sqlite.NewOptimizeCommand()

	if err := submitter.Submit(ctx, cmd); err != nil {
		return err
	}

	t.mu.Lock()
	t.lastRun = time.Now()
	t.mu.Unlock()

	return nil
}

// Ensure OptimizeTask satisfies MaintenanceTask at compile time.
var _ maintenance.MaintenanceTask = (*OptimizeTask)(nil)
