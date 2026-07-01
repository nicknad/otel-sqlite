package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// PurgeLogsCommand is a Command that deletes log records older than a
// cutoff timestamp. It deletes in batches to avoid locking the database
// for too long.
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
func (c *PurgeLogsCommand) Execute(ctx context.Context, tx *sql.Tx) error {
	// First delete attributes for expired events.
	_, err := tx.ExecContext(ctx,
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
	result, err := tx.ExecContext(ctx,
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

	// Clean up orphaned resources (resources with no remaining events).
	_, err = tx.ExecContext(ctx,
		`DELETE FROM log_resource WHERE id NOT IN (
			SELECT DISTINCT resource_id FROM log_event
		)`,
	)
	if err != nil {
		return fmt.Errorf("clean orphaned resources: %w", err)
	}

	if rowsAffected > 0 {
		// Log inside the command is acceptable for diagnostics; the maintenance
		// framework logs only task-level start/completion/failure.
		_ = rowsAffected // used in real implementation
	}

	return nil
}
