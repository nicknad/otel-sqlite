//go:build !nopool

package model

import "sync"

// recordPool is a pool of LogRecord objects.
// Records are allocated once and reused across the pipeline.
var recordPool = sync.Pool{
	New: func() any {
		return &LogRecord{
			Attributes: make([]Attribute, 0, 4),
		}
	},
}

// GetRecord retrieves a LogRecord from the pool.
// The caller must ensure PutRecord is called when the record is no longer needed.
// Ownership: The caller owns the record until it's passed to the next pipeline stage.
func GetRecord() *LogRecord {
	r := recordPool.Get().(*LogRecord)
	// Reset fields to zero values (slice capacity is preserved)
	r.Timestamp = 0
	r.ObservedTimestamp = 0
	r.SeverityNumber = SeverityUnspecified
	r.SeverityText = ""
	r.TraceID = [16]byte{}
	r.SpanID = [8]byte{}
	r.HasTrace = false
	r.HasSpan = false
	r.Body = ""
	r.Attributes = r.Attributes[:0] // Keep capacity, reset length
	r.DroppedAttributesCount = 0
	r.Flags = 0
	r.EventName = ""
	r.ResourceID = ""
	r.Resource = nil
	r.ScopeName = ""
	r.ScopeVersion = ""
	return r
}

// PutRecord returns a LogRecord to the pool.
// IMPORTANT: The record must not be used after calling PutRecord.
// Ownership: Only the SQLite writer should call PutRecord after writing the record.
// Safety: All fields are zeroed to prevent stale data access.
func PutRecord(r *LogRecord) {
	if r == nil {
		return
	}
	// Zero all fields to prevent stale data access if record is accidentally used after put
	r.Timestamp = 0
	r.ObservedTimestamp = 0
	r.SeverityNumber = SeverityUnspecified
	r.SeverityText = ""
	r.TraceID = [16]byte{}
	r.SpanID = [8]byte{}
	r.HasTrace = false
	r.HasSpan = false
	r.Body = ""
	r.DroppedAttributesCount = 0
	r.Flags = 0
	r.EventName = ""
	r.ResourceID = ""
	r.Resource = nil
	r.ScopeName = ""
	r.ScopeVersion = ""
	// Clear attribute values but keep slice capacity
	clear(r.Attributes)
	r.Attributes = r.Attributes[:0]
	recordPool.Put(r)
}
