// Package model defines the internal domain models for the OTLP collector.
package model

import (
	"time"
)

// Severity represents the severity level of a log record.
type Severity int

// Severity levels as defined by OpenTelemetry.
const (
	SeverityUnspecified Severity = iota
	SeverityTrace
	SeverityTrace2
	SeverityTrace3
	SeverityTrace4
	SeverityDebug
	SeverityDebug2
	SeverityDebug3
	SeverityDebug4
	SeverityInfo
	SeverityInfo2
	SeverityInfo3
	SeverityInfo4
	SeverityWarn
	SeverityWarn2
	SeverityWarn3
	SeverityWarn4
	SeverityError
	SeverityError2
	SeverityError3
	SeverityError4
	SeverityFatal
	SeverityFatal2
	SeverityFatal3
	SeverityFatal4
)

// String returns the string representation of the severity.
func (s Severity) String() string {
	switch s {
	case SeverityTrace:
		return "TRACE"
	case SeverityDebug:
		return "DEBUG"
	case SeverityInfo:
		return "INFO"
	case SeverityWarn:
		return "WARN"
	case SeverityError:
		return "ERROR"
	case SeverityFatal:
		return "FATAL"
	default:
		return "UNSPECIFIED"
	}
}

// ValueType represents the type of an attribute value.
type ValueType uint8

// ValueType constants.
const (
	ValueNull ValueType = iota
	ValueString
	ValueInt
	ValueDouble
	ValueBool
	ValueBytes
)

var valueTypeStrings = map[ValueType]string{
	ValueString: "string",
	ValueInt:    "int",
	ValueDouble: "double",
	ValueBool:   "bool",
	ValueBytes:  "bytes",
}

// String returns the string representation of the value type.
func (t ValueType) String() string {
	if s, ok := valueTypeStrings[t]; ok {
		return s
	}
	return "null"
}

// Attribute represents a key-value pair stored inline (no pointer indirection).
// This struct is designed for zero-allocation storage in slices.
type Attribute struct {
	Key  string
	Str  string  // used when Kind == ValueString
	Num  int64   // used when Kind == ValueInt
	Dbl  float64 // used when Kind == ValueDouble
	Flag bool    // used when Kind == ValueBool
	Raw  []byte  // used when Kind == ValueBytes (rare, still a slice)
	Kind ValueType
}

// LogRecord represents a single log record in the internal domain model.
// This is the canonical representation used throughout the ingestion pipeline.
//
// Allocation strategy: use GetRecord() to obtain a LogRecord. Under the
// default build the returned value comes from a sync.Pool; under
// -tags nopool it is a plain heap allocation. In both cases call
// PutRecord() to release it (no-op under nopool).
type LogRecord struct {
	// Timestamp is the time when the event occurred.
	// Value is UNIX Epoch time in nanoseconds since 00:00:00 UTC on 1 January 1970.
	Timestamp int64

	// ObservedTimestamp is the time when the event was observed by the collection system.
	// Value is UNIX Epoch time in nanoseconds since 00:00:00 UTC on 1 January 1970.
	ObservedTimestamp int64

	// SeverityNumber is the numerical value of the severity.
	SeverityNumber Severity

	// SeverityText is the severity text (also known as log level).
	SeverityText string

	// TraceID is a unique identifier for a trace.
	// All logs from the same trace share the same trace_id.
	// Using fixed-size array eliminates heap allocation.
	TraceID [16]byte

	// SpanID is a unique identifier for a span within a trace.
	// Using fixed-size array eliminates heap allocation.
	SpanID [8]byte

	// HasTrace indicates whether TraceID is valid (non-zero).
	HasTrace bool

	// HasSpan indicates whether SpanID is valid (non-zero).
	HasSpan bool

	// Body is the body of the log record.
	Body string

	// Attributes contains additional attributes that describe the specific event occurrence.
	// Pre-allocated as a slice for better performance than map.
	// For most logs, attribute count is small (0-10), so linear scan is faster than map.
	Attributes []Attribute

	// DroppedAttributesCount is the number of attributes that were dropped.
	DroppedAttributesCount uint32

	// Flags contains trace flags and other bit fields.
	Flags uint32

	// EventName is a unique identifier of event category/type.
	EventName string

	// ResourceID references the resource this log record belongs to.
	ResourceID string

	// Resource is the full resource this log record belongs to.
	// Carried on the record so it survives record-based ingress queues
	// that drop LogBatch.Resource.
	Resource *Resource

	// ScopeName is the instrumentation scope name.
	ScopeName string

	// ScopeVersion is the instrumentation scope version.
	ScopeVersion string
}

// TimestampTime returns the timestamp as a time.Time.
func (r *LogRecord) TimestampTime() time.Time {
	return time.Unix(0, r.Timestamp).UTC()
}

// ObservedTimestampTime returns the observed timestamp as a time.Time.
func (r *LogRecord) ObservedTimestampTime() time.Time {
	return time.Unix(0, r.ObservedTimestamp).UTC()
}

// HasTraceContext returns true if the log record has valid trace/span IDs.
func (r *LogRecord) HasTraceContext() bool {
	return r.HasTrace && r.HasSpan
}

// LogBatch represents a batch of log records.
type LogBatch struct {
	// Records contains the log records in this batch.
	Records []*LogRecord

	// Resource contains the resource information for this batch.
	Resource *Resource

	// SchemaURL is the schema URL for this batch.
	SchemaURL string
}

// NewLogBatch creates a new LogBatch with the given capacity.
func NewLogBatch(capacity int) *LogBatch {
	return &LogBatch{
		Records: make([]*LogRecord, 0, capacity),
	}
}

// AddRecord adds a log record to the batch.
func (b *LogBatch) AddRecord(record *LogRecord) {
	b.Records = append(b.Records, record)
}

// Size returns the number of records in the batch.
func (b *LogBatch) Size() int {
	return len(b.Records)
}

// IsEmpty returns true if the batch contains no records.
func (b *LogBatch) IsEmpty() bool {
	return len(b.Records) == 0
}

// Clear removes all records from the batch.
func (b *LogBatch) Clear() {
	b.Records = b.Records[:0]
}
