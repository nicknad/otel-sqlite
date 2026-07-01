package sqlite

import (
	"context"
	"database/sql"
	"fmt"

	"codeberg.org/nicknad/otel-sqlite/internal/storage"
)

// VacuumCommand reclaims disk space by rebuilding the database file.
//
// VACUUM cannot run inside a SQLite transaction. This command therefore
// implements NonTransactionalCommand so that the writer executes it
// directly on the database connection without wrapping in a transaction.
type VacuumCommand struct{}

// NewVacuumCommand creates a new VacuumCommand.
func NewVacuumCommand() *VacuumCommand {
	return &VacuumCommand{}
}

// Execute is provided to satisfy the Command interface but must NOT be
// called by the writer. VACUUM cannot run inside a transaction; the
// writer detects NonTransactionalCommand and calls ExecuteNonTransactional
// instead.
func (c *VacuumCommand) Execute(ctx context.Context, tx *sql.Tx) error {
	return fmt.Errorf("VACUUM cannot run inside a transaction; writer must use ExecuteNonTransactional")
}

// ExecuteNonTransactional runs VACUUM directly on the database connection.
func (c *VacuumCommand) ExecuteNonTransactional(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, "VACUUM")
	if err != nil {
		return fmt.Errorf("vacuum: %w", err)
	}
	return nil
}

// Compile-time interface checks.
var _ storage.Command = (*VacuumCommand)(nil)
var _ storage.NonTransactionalCommand = (*VacuumCommand)(nil)
