package sqlite

import (
	"context"
	"database/sql"
	"fmt"

	"codeberg.org/nicknad/otel-sqlite/internal/storage"
)

// OptimizeCommand triggers SQLite query planner optimization
// (PRAGMA optimize). This is a lightweight maintenance operation
// that updates statistics used by the query planner.
//
// PRAGMA optimize is safe to run inside a transaction (it becomes
// a no-op if the connection is not idle). The command works either
// way — the effect is applied when the transaction commits.
type OptimizeCommand struct{}

// NewOptimizeCommand creates a new OptimizeCommand.
func NewOptimizeCommand() *OptimizeCommand {
	return &OptimizeCommand{}
}

// Execute runs PRAGMA optimize inside the given transaction.
func (c *OptimizeCommand) Execute(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, "PRAGMA optimize")
	if err != nil {
		return fmt.Errorf("optimize: %w", err)
	}
	return nil
}

// Compile-time interface checks.
var _ storage.Command = (*OptimizeCommand)(nil)
