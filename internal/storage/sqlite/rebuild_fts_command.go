package sqlite

import (
	"context"
	"database/sql"
	"fmt"

	"codeberg.org/nicknad/otel-sqlite/internal/storage"
)

// RebuildFtsCommand drops and recreates the logs_fts contentless FTS5 index
// and repopulates it from the logs view.  This is a full rebuild: it
// serializes with ingestion on the single writer so the FTS index is
// guaranteed consistent with the current on-disk state after the rebuild
// completes.
//
// The operation runs inside a regular SQLite transaction (DROP + CREATE +
// INSERT … SELECT).  Contentless FTS5 tables do not support incremental
// DELETE, so DROP + CREATE is the canonical rebuild path.
type RebuildFtsCommand struct{}

// NewRebuildFtsCommand creates a new RebuildFtsCommand.
func NewRebuildFtsCommand() *RebuildFtsCommand {
	return &RebuildFtsCommand{}
}

// Execute runs the full-text index rebuild inside the given transaction.
func (c *RebuildFtsCommand) Execute(ctx context.Context, tx *sql.Tx) error {
	// Drop and recreate the FTS table atomically.
	_, err := tx.ExecContext(ctx, `DROP TABLE IF EXISTS logs_fts`)
	if err != nil {
		return fmt.Errorf("rebuild fts: drop: %w", err)
	}

	_, err = tx.ExecContext(ctx, `
		CREATE VIRTUAL TABLE logs_fts USING fts5(
			body,
			service_name,
			content=''
		)
	`)
	if err != nil {
		return fmt.Errorf("rebuild fts: create: %w", err)
	}

	// Populate from the logs view.  The rowid is the log_event id so
	// that FTS MATCH queries can map results back to log rows.
	_, err = tx.ExecContext(ctx, `
		INSERT INTO logs_fts(rowid, body, service_name)
		SELECT id, body, service_name FROM logs
	`)
	if err != nil {
		return fmt.Errorf("rebuild fts: populate: %w", err)
	}

	return nil
}

// Compile-time interface checks.
var _ storage.Command = (*RebuildFtsCommand)(nil)
