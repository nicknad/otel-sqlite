package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// MaxPurgeIterations caps the number of delete batches per command execution
// to bound transaction duration while guaranteeing eventual catch-up across
// multiple Due() firings. Each batch deletes up to batchSize rows, so the
// default of 1000 iterations covers 10M rows per run with batchSize=10000.
const MaxPurgeIterations = 1000

// PurgeLogsCommand is a Command that deletes log records older than a
// cutoff timestamp. It loops internally until all expired rows are removed
// (or MaxPurgeIterations is reached), so retention keeps up with ingest
// instead of silently falling behind.
type PurgeLogsCommand struct {
	cutoffNanos int64
	batchSize   int
}

// NewPurgeLogsCommand creates a command that deletes log records with
// timestamp_ns < cutoff (where cutoff is the retention boundary).
//
// cutoffAge is the retention duration (e.g., 30 days). Records older than
// (now - cutoffAge) are eligible for deletion.
// batchSize controls how many records are deleted in a single statement.
func NewPurgeLogsCommand(cutoffAge time.Duration, batchSize int) *PurgeLogsCommand {
	if batchSize <= 0 {
		batchSize = 10000
	}
	cutoffNanos := time.Now().Add(-cutoffAge).UnixNano()
	return &PurgeLogsCommand{
		cutoffNanos: cutoffNanos,
		batchSize:   batchSize,
	}
}

// Execute deletes expired log records inside the given transaction.
// It loops until no more expired rows remain (or MaxPurgeIterations is hit),
// so a single retention run catches up to the current ingestion watermark
// even when the backlog spans many batchSize windows.
func (c *PurgeLogsCommand) Execute(ctx context.Context, tx *sql.Tx) error {
	totalDeleted := int64(0)

	for range MaxPurgeIterations {
		// First delete attributes for expired events.
		_, err := tx.ExecContext(
			ctx,
			`DELETE FROM log_attr WHERE event_id IN (
				SELECT id FROM log_event WHERE timestamp_ns < ?
				ORDER BY id LIMIT ?
			)`,
			c.cutoffNanos, c.batchSize,
		)
		if err != nil {
			return fmt.Errorf("delete expired attributes: %w", err)
		}

		// Then delete the events themselves.
		result, err := tx.ExecContext(
			ctx,
			`DELETE FROM log_event WHERE rowid IN (
				SELECT rowid FROM log_event WHERE timestamp_ns < ?
				ORDER BY rowid LIMIT ?
			)`,
			c.cutoffNanos, c.batchSize,
		)
		if err != nil {
			return fmt.Errorf("delete expired events: %w", err)
		}

		rowsAffected, _ := result.RowsAffected()
		totalDeleted += rowsAffected

		// No more rows to delete — done.
		if rowsAffected < int64(c.batchSize) {
			break
		}
	}

	// Clean up orphaned resources (resources with no remaining events).
	_, err := tx.ExecContext(
		ctx,
		`DELETE FROM log_resource WHERE id NOT IN (
			SELECT DISTINCT resource_id FROM log_event
		)`,
	)
	if err != nil {
		return fmt.Errorf("clean orphaned resources: %w", err)
	}

	if totalDeleted > 0 {
		_ = totalDeleted // retained for future instrumentation
	}

	return nil
}
