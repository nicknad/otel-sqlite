package tasks

import (
	"context"
	"sync"
	"time"

	"codeberg.org/nicknad/otel-sqlite/internal/maintenance"
	"codeberg.org/nicknad/otel-sqlite/internal/storage/sqlite"
)

// VacuumTask periodically reclaims disk space by rebuilding the SQLite
// database file. VACUUM is an expensive operation that requires an
// exclusive lock, so it runs on a low-frequency schedule and is disabled
// by default.
//
// The task does NOT execute SQL or touch SQLite directly. It creates a
// VacuumCommand and submits it via CommandSubmitter.
type VacuumTask struct {
	enabled  bool
	interval time.Duration

	mu      sync.Mutex
	lastRun time.Time
}

// NewVacuumTask creates a new VacuumTask.
func NewVacuumTask(enabled bool, interval time.Duration) *VacuumTask {
	return &VacuumTask{
		enabled:  enabled,
		interval: interval,
	}
}

// Name returns the task name for metrics and logging.
func (t *VacuumTask) Name() string { return "vacuum" }

// Enabled reports whether vacuuming is configured to run.
func (t *VacuumTask) Enabled() bool { return t.enabled }

// Due returns true if enough time has elapsed since the last vacuum.
func (t *VacuumTask) Due(now time.Time) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.lastRun.IsZero() {
		return true
	}
	return now.Sub(t.lastRun) >= t.interval
}

// Run creates and submits a VacuumCommand.
func (t *VacuumTask) Run(ctx context.Context, submitter maintenance.CommandSubmitter) error {
	cmd := sqlite.NewVacuumCommand()

	if err := submitter.Submit(ctx, cmd); err != nil {
		return err
	}

	t.mu.Lock()
	t.lastRun = time.Now()
	t.mu.Unlock()

	return nil
}

// Ensure VacuumTask satisfies MaintenanceTask at compile time.
var _ maintenance.MaintenanceTask = (*VacuumTask)(nil)
