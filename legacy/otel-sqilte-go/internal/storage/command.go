// Package storage provides storage abstractions for the OTLP collector.
// This package defines interfaces that must be implemented by storage backends.
package storage

import (
	"context"
	"database/sql"
)

// Command is the interface for all database mutation commands.
//
// Implementations are immutable from the caller's perspective—all data is
// set at construction time. Each command encapsulates its own execution
// logic and receives a *sql.Tx from the SQLite writer, which owns the
// transaction lifecycle.
//
// A command must never begin, commit or roll back a transaction.
type Command interface {
	// Execute performs the command's work inside the given transaction.
	// Implementations must not call tx.Commit() or tx.Rollback().
	Execute(ctx context.Context, tx *sql.Tx) error
}

// NonTransactionalCommand is an optional interface that commands may implement
// for operations that cannot run inside a SQLite transaction (e.g., VACUUM).
// The writer detects this interface and executes such commands directly
// on the database connection, bypassing the normal transaction lifecycle.
type NonTransactionalCommand interface {
	Command

	// ExecuteNonTransactional runs the command directly on the database.
	// Implementations must not call Begin/Commit/Rollback.
	ExecuteNonTransactional(ctx context.Context, db *sql.DB) error
}

// CommandExecutor is the interface for submitting commands for execution.
//
// The SQLite Writer implements this interface. The rest of the system
// (batcher, future maintenance components) depends only on this interface
// and has no knowledge of SQLite internals.
type CommandExecutor interface {
	// Submit enqueues a command for execution.
	// Blocks until space is available or context is canceled.
	Submit(ctx context.Context, cmd Command) error
}
