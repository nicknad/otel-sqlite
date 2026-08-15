package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// PurgeMetricDataPointsCommand is a Command that deletes metric data points
// older than a cutoff timestamp — the metric analogue of PurgeLogsCommand.
// It loops internally until all expired rows are removed (or
// MaxPurgeIterations is reached), then cleans up orphaned series, metric
// definitions, scopes and resources that no longer have any data points.
type PurgeMetricDataPointsCommand struct {
	cutoffNanos int64
	batchSize   int
}

// NewPurgeMetricDataPointsCommand creates a command that deletes metric data
// points with timestamp_ns < (now - cutoffAge).
//
// cutoffAge is the retention duration (e.g., 30 days). Points sampled before
// the boundary are eligible for deletion.
// batchSize controls how many points are deleted in a single statement.
func NewPurgeMetricDataPointsCommand(cutoffAge time.Duration, batchSize int) *PurgeMetricDataPointsCommand {
	if batchSize <= 0 {
		batchSize = 10000
	}
	cutoffNanos := time.Now().Add(-cutoffAge).UnixNano()
	return &PurgeMetricDataPointsCommand{
		cutoffNanos: cutoffNanos,
		batchSize:   batchSize,
	}
}

// Execute deletes expired metric data points inside the given transaction.
// It loops until no more expired rows remain (or MaxPurgeIterations is hit)
// so a single retention run catches up to the current ingestion watermark.
func (c *PurgeMetricDataPointsCommand) Execute(ctx context.Context, tx *sql.Tx) error {
	totalDeleted := int64(0)

	for range MaxPurgeIterations {
		result, err := tx.ExecContext(
			ctx,
			`DELETE FROM metric_data_point WHERE id IN (
				SELECT id FROM metric_data_point WHERE timestamp_ns < ?
				ORDER BY id LIMIT ?
			)`,
			c.cutoffNanos, c.batchSize,
		)
		if err != nil {
			return fmt.Errorf("delete expired metric data points: %w", err)
		}

		rowsAffected, _ := result.RowsAffected()
		totalDeleted += rowsAffected

		// No more rows to delete — done.
		if rowsAffected < int64(c.batchSize) {
			break
		}
	}

	// Orphan cleanup, children first. metric_series / metric / scope are
	// referenced only by the metrics tables; log_resource is shared with the
	// log path, so it is only removed when neither signal references it.
	if _, err := tx.ExecContext(
		ctx,
		`DELETE FROM metric_series WHERE id NOT IN (
			SELECT DISTINCT series_id FROM metric_data_point
		)`,
	); err != nil {
		return fmt.Errorf("clean orphaned metric series: %w", err)
	}
	if _, err := tx.ExecContext(
		ctx,
		`DELETE FROM metric WHERE id NOT IN (
			SELECT DISTINCT metric_id FROM metric_series
		)`,
	); err != nil {
		return fmt.Errorf("clean orphaned metrics: %w", err)
	}
	if _, err := tx.ExecContext(
		ctx,
		`DELETE FROM scope WHERE id NOT IN (
			SELECT DISTINCT scope_id FROM metric
		)`,
	); err != nil {
		return fmt.Errorf("clean orphaned scopes: %w", err)
	}
	if _, err := tx.ExecContext(
		ctx,
		`DELETE FROM log_resource WHERE id NOT IN (
			SELECT DISTINCT resource_id FROM log_event
		) AND id NOT IN (
			SELECT DISTINCT resource_id FROM scope
		)`,
	); err != nil {
		return fmt.Errorf("clean orphaned resources: %w", err)
	}

	if totalDeleted > 0 {
		_ = totalDeleted // retained for future instrumentation
	}

	return nil
}
