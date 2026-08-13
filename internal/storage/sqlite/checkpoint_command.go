package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"codeberg.org/nicknad/otel-sqlite/internal/storage"
)

// CheckpointMode represents a SQLite WAL checkpoint mode.
// It maps directly to the argument of PRAGMA wal_checkpoint.
type CheckpointMode string

const (
	// CheckpointPassive (default) does not interrupt other database
	// operations and may be a no-op if there are concurrent readers.
	CheckpointPassive CheckpointMode = "PASSIVE"
	// CheckpointFull blocks all other writers and checkpoints fully.
	CheckpointFull CheckpointMode = "FULL"
	// CheckpointRestart is like FULL but also restarts the WAL file.
	CheckpointRestart CheckpointMode = "RESTART"
	// CheckpointTruncate is like RESTART but also truncates the WAL file.
	CheckpointTruncate CheckpointMode = "TRUNCATE"
)

// String returns the mode as a string, suitable for use in PRAGMA.
func (m CheckpointMode) String() string { return string(m) }

// CheckpointCommand triggers a WAL checkpoint on the SQLite database.
//
// It is immutable after construction. The mode is set at creation time
// and passed to PRAGMA wal_checkpoint during execution.
type CheckpointCommand struct {
	Mode CheckpointMode
}

// NewCheckpointCommand creates a new CheckpointCommand with the given mode.
// If mode is empty, CheckpointPassive is used.
func NewCheckpointCommand(mode CheckpointMode) *CheckpointCommand {
	if mode == "" {
		mode = CheckpointPassive
	}
	return &CheckpointCommand{Mode: mode}
}

func (c *CheckpointCommand) Execute(ctx context.Context, tx *sql.Tx) error {
	return errors.New("wal_checkpoint cannot run inside a transaction; writer must use ExecuteNonTransactional")
}

func (c *CheckpointCommand) ExecuteNonTransactional(ctx context.Context, db *sql.DB) error {
	query := fmt.Sprintf("PRAGMA wal_checkpoint(%s)", c.Mode)
	if _, err := db.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("wal_checkpoint(%s): %w", c.Mode, err)
	}
	return nil
}

// Compile-time interface checks.
var (
	_ storage.Command                 = (*CheckpointCommand)(nil)
	_ storage.NonTransactionalCommand = (*CheckpointCommand)(nil)
)
