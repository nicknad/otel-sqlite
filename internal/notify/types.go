// Package notify provides a notification pipeline for the OTLP collector.
// It receives error-level log records from the batcher, runs them through
// a rule engine, tracks state in an embedded KV store, and delivers
// notifications via pluggable notifiers (HTTP webhook, Slack, etc.).
package notify

import (
	"errors"

	"codeberg.org/nicknad/otel-sqlite/internal/model"
)

// Event is the notification payload passed from the batcher.
type Event struct {
	Severity     model.Severity
	SeverityText string
	Body         string
	ResourceID   string
	Resource     *model.Resource
	Timestamp    int64
	TraceID      [16]byte
	SpanID       [8]byte
	Attributes   []model.Attribute
	ScopeName    string
	ScopeVersion string

	// cachedFingerprint is lazily computed by Fingerprint().
	cachedFingerprint string
}

// Fingerprint returns a stable hash of the event body + sorted attributes.
// The result is cached after first computation.
func (e *Event) Fingerprint() string {
	if e.cachedFingerprint != "" {
		return e.cachedFingerprint
	}
	e.cachedFingerprint = eventFingerprint(e)
	return e.cachedFingerprint
}

// EventFromLogRecord creates an Event from a model.LogRecord.
func EventFromLogRecord(record *model.LogRecord) *Event {
	return &Event{
		Severity:     record.SeverityNumber,
		SeverityText: record.SeverityText,
		Body:         record.Body,
		ResourceID:   record.ResourceID,
		Resource:     record.Resource,
		Timestamp:    record.Timestamp,
		TraceID:      record.TraceID,
		SpanID:       record.SpanID,
		Attributes:   record.Attributes,
		ScopeName:    record.ScopeName,
		ScopeVersion: record.ScopeVersion,
	}
}

// IsRetryable checks whether an error from a Notifier is retryable.
// Non-retryable errors are returned when the destination indicates the
// request should not be retried (e.g., 4xx HTTP responses).
func IsRetryable(err error) bool {
	var nr *notRetryableError
	return !errors.As(err, &nr)
}

// notRetryableError wraps an error to mark it as non-retryable.
type notRetryableError struct{ err error }

func (e *notRetryableError) Error() string { return e.err.Error() }
func (e *notRetryableError) Unwrap() error { return e.err }

// NewNotRetryableError wraps an error to mark it as non-retryable.
func NewNotRetryableError(err error) error {
	return &notRetryableError{err: err}
}

// Sentinel errors.
var (
	ErrQueueFull = errors.New("notify: event queue full")
	ErrNotFound  = errors.New("notify: state not found")
)
