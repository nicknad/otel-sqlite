package otlp

import (
	"sync"
	"testing"

	commonV1 "codeberg.org/nicknad/otel-sqlite/internal/generated/opentelemetry/proto/common/v1"
	logsV1 "codeberg.org/nicknad/otel-sqlite/internal/generated/opentelemetry/proto/logs/v1"
	resourceV1 "codeberg.org/nicknad/otel-sqlite/internal/generated/opentelemetry/proto/resource/v1"
)

// ============================================================================
// OPTIMIZED TYPES (Demonstration)
// These show how preallocation and reuse eliminate allocations
// ============================================================================

// OptimizedTraceID uses fixed-size array (zero heap allocation)
type OptimizedTraceID [16]byte

// OptimizedSpanID uses fixed-size array (zero heap allocation)
type OptimizedSpanID [8]byte

// OptimizedAttribute stores key-value inline (no pointer indirection)
type OptimizedAttribute struct {
	Key   string
	Value string
}

// OptimizedLogRecord uses:
// - Fixed-size arrays for TraceID/SpanID
// - Slice for attributes (pre-allocated)
// - No map overhead
type OptimizedLogRecord struct {
	Timestamp         int64
	ObservedTimestamp int64
	SeverityText      string
	Body              string
	TraceID           OptimizedTraceID // Fixed-size array
	SpanID            OptimizedSpanID  // Fixed-size array
	HasTrace          bool
	HasSpan           bool
	Attributes        []OptimizedAttribute // Pre-allocated slice
}

// Pool for reusing OptimizedLogRecord objects
var optimizedRecordPool = sync.Pool{
	New: func() interface{} {
		return &OptimizedLogRecord{
			Attributes: make([]OptimizedAttribute, 0, 4), // Pre-allocate for common case
		}
	},
}

func getOptimizedRecord() *OptimizedLogRecord {
	r := optimizedRecordPool.Get().(*OptimizedLogRecord)
	// Reset fields
	r.Timestamp = 0
	r.ObservedTimestamp = 0
	r.SeverityText = ""
	r.Body = ""
	r.TraceID = OptimizedTraceID{}
	r.SpanID = OptimizedSpanID{}
	r.HasTrace = false
	r.HasSpan = false
	r.Attributes = r.Attributes[:0] // Keep capacity, reset length
	return r
}

func putOptimizedRecord(r *OptimizedLogRecord) {
	if r == nil {
		return
	}
	r.SeverityText = ""
	r.Body = ""
	for i := range r.Attributes {
		r.Attributes[i].Key = ""
		r.Attributes[i].Value = ""
	}
	r.Attributes = r.Attributes[:0]
	optimizedRecordPool.Put(r)
}

// ============================================================================
// BENCHMARKS: Original vs Optimized
// ============================================================================

func makeTestProtoRecord(numAttrs int) *logsV1.LogRecord {
	attrs := make([]*commonV1.KeyValue, numAttrs)
	for i := 0; i < numAttrs; i++ {
		attrs[i] = &commonV1.KeyValue{
			Key: "attr_key",
			Value: &commonV1.AnyValue{
				Value: &commonV1.AnyValue_StringValue{StringValue: "attr_value"},
			},
		}
	}

	return &logsV1.LogRecord{
		TimeUnixNano:         1234567890,
		ObservedTimeUnixNano: 1234567890,
		SeverityNumber:       logsV1.SeverityNumber_SEVERITY_NUMBER_INFO,
		SeverityText:         "INFO",
		Body: &commonV1.AnyValue{
			Value: &commonV1.AnyValue_StringValue{StringValue: "test log message"},
		},
		TraceId:    make([]byte, 16),
		SpanId:     make([]byte, 8),
		Attributes: attrs,
	}
}

// BenchmarkOriginal_SingleRecord_4Attrs measures current implementation
func BenchmarkOriginal_SingleRecord_4Attrs(b *testing.B) {
	mapper := NewMapper()
	protoRecord := makeTestProtoRecord(4)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = mapper.mapLogRecord(protoRecord, nil, "scope", "1.0.0")
	}
}

// BenchmarkOptimized_SingleRecord_4Attrs measures optimized implementation
func BenchmarkOptimized_SingleRecord_4Attrs(b *testing.B) {
	protoRecord := makeTestProtoRecord(4)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		record := getOptimizedRecord()
		
		// Map timestamps
		record.Timestamp = int64(protoRecord.TimeUnixNano)
		record.ObservedTimestamp = int64(protoRecord.ObservedTimeUnixNano)
		
		// Map severity
		record.SeverityText = protoRecord.SeverityText
		
		// Map trace context using fixed-size arrays (NO heap allocation)
		if len(protoRecord.TraceId) == 16 {
			copy(record.TraceID[:], protoRecord.TraceId)
			record.HasTrace = true
		}
		if len(protoRecord.SpanId) == 8 {
			copy(record.SpanID[:], protoRecord.SpanId)
			record.HasSpan = true
		}
		
		// Map body
		if protoRecord.Body != nil {
			if sv, ok := protoRecord.Body.Value.(*commonV1.AnyValue_StringValue); ok {
				record.Body = sv.StringValue
			}
		}
		
		// Map attributes into pre-allocated slice
		if len(protoRecord.Attributes) > 0 {
			if cap(record.Attributes) < len(protoRecord.Attributes) {
				record.Attributes = make([]OptimizedAttribute, 0, len(protoRecord.Attributes))
			}
			record.Attributes = record.Attributes[:0]
			
			for _, kv := range protoRecord.Attributes {
				var value string
				if sv, ok := kv.Value.Value.(*commonV1.AnyValue_StringValue); ok {
					value = sv.StringValue
				}
				record.Attributes = append(record.Attributes, OptimizedAttribute{
					Key:   kv.Key,
					Value: value,
				})
			}
		}
		
		// Return to pool
		putOptimizedRecord(record)
	}
}

// BenchmarkOriginal_100Records_4Attrs measures batch processing (original)
func BenchmarkOriginal_100Records_4Attrs(b *testing.B) {
	mapper := NewMapper()
	resourceLogs := &logsV1.ResourceLogs{
		Resource: &resourceV1.Resource{
			Attributes: []*commonV1.KeyValue{
				{
					Key: "service.name",
					Value: &commonV1.AnyValue{
						Value: &commonV1.AnyValue_StringValue{StringValue: "test-service"},
					},
				},
			},
		},
		ScopeLogs: []*logsV1.ScopeLogs{
			{
				LogRecords: make([]*logsV1.LogRecord, 100),
			},
		},
	}
	for i := 0; i < 100; i++ {
		resourceLogs.ScopeLogs[0].LogRecords[i] = makeTestProtoRecord(4)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = mapper.MapLogsData([]*logsV1.ResourceLogs{resourceLogs})
	}
}

// BenchmarkOptimized_100Records_4Attrs measures batch processing (optimized)
func BenchmarkOptimized_100Records_4Attrs(b *testing.B) {
	protoRecords := make([]*logsV1.LogRecord, 100)
	for i := 0; i < 100; i++ {
		protoRecords[i] = makeTestProtoRecord(4)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		records := make([]*OptimizedLogRecord, 0, 100)
		for _, protoRecord := range protoRecords {
			record := getOptimizedRecord()
			
			record.Timestamp = int64(protoRecord.TimeUnixNano)
			record.ObservedTimestamp = int64(protoRecord.ObservedTimeUnixNano)
			record.SeverityText = protoRecord.SeverityText
			
			if len(protoRecord.TraceId) == 16 {
				copy(record.TraceID[:], protoRecord.TraceId)
				record.HasTrace = true
			}
			if len(protoRecord.SpanId) == 8 {
				copy(record.SpanID[:], protoRecord.SpanId)
				record.HasSpan = true
			}
			
			if protoRecord.Body != nil {
				if sv, ok := protoRecord.Body.Value.(*commonV1.AnyValue_StringValue); ok {
					record.Body = sv.StringValue
				}
			}
			
			if len(protoRecord.Attributes) > 0 {
				if cap(record.Attributes) < len(protoRecord.Attributes) {
					record.Attributes = make([]OptimizedAttribute, 0, len(protoRecord.Attributes))
				}
				record.Attributes = record.Attributes[:0]
				
				for _, kv := range protoRecord.Attributes {
					var value string
					if sv, ok := kv.Value.Value.(*commonV1.AnyValue_StringValue); ok {
						value = sv.StringValue
					}
					record.Attributes = append(record.Attributes, OptimizedAttribute{
						Key:   kv.Key,
						Value: value,
					})
				}
			}
			
			records = append(records, record)
		}
		
		// Return all records to pool
		for _, record := range records {
			putOptimizedRecord(record)
		}
	}
}

// BenchmarkOriginal_100Records_10Attrs measures with more attributes
func BenchmarkOriginal_100Records_10Attrs(b *testing.B) {
	mapper := NewMapper()
	resourceLogs := &logsV1.ResourceLogs{
		Resource: &resourceV1.Resource{
			Attributes: []*commonV1.KeyValue{
				{
					Key: "service.name",
					Value: &commonV1.AnyValue{
						Value: &commonV1.AnyValue_StringValue{StringValue: "test-service"},
					},
				},
			},
		},
		ScopeLogs: []*logsV1.ScopeLogs{
			{
				LogRecords: make([]*logsV1.LogRecord, 100),
			},
		},
	}
	for i := 0; i < 100; i++ {
		resourceLogs.ScopeLogs[0].LogRecords[i] = makeTestProtoRecord(10)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = mapper.MapLogsData([]*logsV1.ResourceLogs{resourceLogs})
	}
}

// BenchmarkOptimized_100Records_10Attrs measures with more attributes
func BenchmarkOptimized_100Records_10Attrs(b *testing.B) {
	protoRecords := make([]*logsV1.LogRecord, 100)
	for i := 0; i < 100; i++ {
		protoRecords[i] = makeTestProtoRecord(10)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		records := make([]*OptimizedLogRecord, 0, 100)
		for _, protoRecord := range protoRecords {
			record := getOptimizedRecord()
			
			record.Timestamp = int64(protoRecord.TimeUnixNano)
			record.ObservedTimestamp = int64(protoRecord.ObservedTimeUnixNano)
			record.SeverityText = protoRecord.SeverityText
			
			if len(protoRecord.TraceId) == 16 {
				copy(record.TraceID[:], protoRecord.TraceId)
				record.HasTrace = true
			}
			if len(protoRecord.SpanId) == 8 {
				copy(record.SpanID[:], protoRecord.SpanId)
				record.HasSpan = true
			}
			
			if protoRecord.Body != nil {
				if sv, ok := protoRecord.Body.Value.(*commonV1.AnyValue_StringValue); ok {
					record.Body = sv.StringValue
				}
			}
			
			if len(protoRecord.Attributes) > 0 {
				if cap(record.Attributes) < len(protoRecord.Attributes) {
					record.Attributes = make([]OptimizedAttribute, 0, len(protoRecord.Attributes))
				}
				record.Attributes = record.Attributes[:0]
				
				for _, kv := range protoRecord.Attributes {
					var value string
					if sv, ok := kv.Value.Value.(*commonV1.AnyValue_StringValue); ok {
						value = sv.StringValue
					}
					record.Attributes = append(record.Attributes, OptimizedAttribute{
						Key:   kv.Key,
						Value: value,
					})
				}
			}
			
			records = append(records, record)
		}
		
		for _, record := range records {
			putOptimizedRecord(record)
		}
	}
}
