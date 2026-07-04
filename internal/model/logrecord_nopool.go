//go:build nopool

package model

// GetRecord allocates a new LogRecord from the heap.
// Under the nopool build tag, no sync.Pool is used — every call allocates.
func GetRecord() *LogRecord {
	return &LogRecord{
		Attributes: make([]Attribute, 0, 4),
	}
}

// PutRecord is a no-op under the nopool build tag.
// The record is left for the GC to collect.
func PutRecord(r *LogRecord) {
	// no-op: heap-allocated records are GC'd
}

// NewLogRecord creates a new LogRecord with pre-allocated attribute slice.
// Under nopool this is identical to GetRecord.
func NewLogRecord() *LogRecord {
	return &LogRecord{
		Attributes: make([]Attribute, 0, 4),
	}
}
