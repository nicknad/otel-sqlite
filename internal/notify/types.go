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

// Sentinel errors.
var (
	ErrQueueFull = errors.New("notify: event queue full")
	ErrNotFound  = errors.New("notify: state not found")
)
