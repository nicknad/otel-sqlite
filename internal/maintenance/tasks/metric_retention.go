package tasks

import (
	"context"
	"sync"
	"time"

	"codeberg.org/nicknad/otel-sqlite/internal/maintenance"
	"codeberg.org/nicknad/otel-sqlite/internal/storage/sqlite"
)

// MetricRetentionTask periodically purges metric data points older than a
// configured retention period — the metric analogue of RetentionTask.
// Orphaned series/metrics/scopes/resources are cleaned by the command.
type MetricRetentionTask struct {
	enabled         bool
	keepMetrics     time.Duration
	cleanupInterval time.Duration
	batchSize       int

	mu      sync.Mutex
	lastRun time.Time
}

// NewMetricRetentionTask creates a new metric retention task.
func NewMetricRetentionTask(
	enabled bool,
	keepMetrics, cleanupInterval time.Duration,
	batchSize int,
) *MetricRetentionTask {
	return &MetricRetentionTask{
		enabled:         enabled,
		keepMetrics:     keepMetrics,
		cleanupInterval: cleanupInterval,
		batchSize:       batchSize,
	}
}

// Name returns the task name for metrics and logging.
func (t *MetricRetentionTask) Name() string {
	return "metric_retention"
}

// Enabled reports whether metric retention is configured to run.
func (t *MetricRetentionTask) Enabled() bool {
	return t.enabled
}

// Due returns true if enough time has elapsed since the last run.
func (t *MetricRetentionTask) Due(now time.Time) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.lastRun.IsZero() {
		return true
	}
	return now.Sub(t.lastRun) >= t.cleanupInterval
}

// Run creates and submits a PurgeMetricDataPointsCommand.
func (t *MetricRetentionTask) Run(ctx context.Context, submitter maintenance.CommandSubmitter) error {
	cmd := sqlite.NewPurgeMetricDataPointsCommand(t.keepMetrics, t.batchSize)

	if err := submitter.Submit(ctx, cmd); err != nil {
		return err
	}

	t.mu.Lock()
	t.lastRun = time.Now()
	t.mu.Unlock()

	return nil
}

// Ensure MetricRetentionTask satisfies MaintenanceTask at compile time.
var _ maintenance.MaintenanceTask = (*MetricRetentionTask)(nil)
