// Package storage provides storage abstractions for the OTLP collector.
// This package defines interfaces that must be implemented by storage backends.
package storage

import (
	"context"

	"github.com/nnadolski/otel-sqlite/internal/model"
)

// LogStorage is the interface that all log storage backends must implement.
type LogStorage interface {
	// Writer returns a LogWriter for writing log records.
	Writer() LogWriter

	// Reader returns a LogReader for reading log records.
	Reader() LogReader

	// Close closes the storage backend and releases resources.
	Close() error
}

// LogWriter is the interface for writing log records to storage.
//
// Deprecated: Prefer CommandExecutor for new code. LogWriter is kept for
// backward compatibility; the SQLite writer now implements CommandExecutor.
type LogWriter interface {
	// WriteBatch writes a batch of log records to storage.
	WriteBatch(ctx context.Context, batch *model.LogBatch) error

	// Flush ensures all buffered data is written to persistent storage.
	Flush(ctx context.Context) error

	// Close closes the writer and releases resources.
	Close() error
}

// LogReader is the interface for reading log records from storage.
// This is a placeholder for future search API support.
type LogReader interface {
	// Query executes a query and returns matching log records.
	Query(ctx context.Context, query *Query) ([]*model.LogRecord, error)

	// GetByID retrieves a specific log record by ID.
	GetByID(ctx context.Context, id int64) (*model.LogRecord, error)

	// ListResources returns all known resources.
	ListResources(ctx context.Context) ([]*model.Resource, error)
}

// Query represents a query for log records.
// This is a placeholder for future search API support.
type Query struct {
	// Time range
	StartTime int64
	EndTime   int64

	// Resource filters
	ResourceIDs  []string
	ServiceNames []string

	// Severity filters
	SeverityNumbers []int
	SeverityTexts   []string

	// Trace context filters
	TraceIDs [][]byte
	SpanIDs  [][]byte

	// Text search
	BodyContains string

	// Attribute filters
	Attributes map[string]string

	// Pagination
	Limit  int
	Offset int

	// Sorting
	SortBy    string
	SortOrder string
}

// NewQuery creates a new Query with default values.
func NewQuery() *Query {
	return &Query{
		Attributes: make(map[string]string),
		Limit:      100,
		SortBy:     "timestamp",
		SortOrder:  "desc",
	}
}
