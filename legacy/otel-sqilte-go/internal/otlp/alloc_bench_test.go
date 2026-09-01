package otlp

import (
	"testing"

	commonV1 "codeberg.org/nicknad/otel-sqlite/internal/generated/opentelemetry/proto/common/v1"
	logsV1 "codeberg.org/nicknad/otel-sqlite/internal/generated/opentelemetry/proto/logs/v1"
)

// BenchmarkMapSingleLogRecord_NoAttrs isolates the base cost of mapping
// a record with zero attributes — this reveals how many of the 8 allocs
// come from the record itself vs. its attribute map.
func BenchmarkMapSingleLogRecord_NoAttrs(b *testing.B) {
	mapper := NewMapper()
	record := &logsV1.LogRecord{
		TimeUnixNano:         1234567890,
		ObservedTimeUnixNano: 1234567890,
		SeverityNumber:       logsV1.SeverityNumber_SEVERITY_NUMBER_INFO,
		SeverityText:         "INFO",
		Body: &commonV1.AnyValue{
			Value: &commonV1.AnyValue_StringValue{StringValue: "test log message"},
		},
		TraceId: make([]byte, 16),
		SpanId:  make([]byte, 8),
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = mapper.mapLogRecord(record, nil, "scope", "1.0.0")
	}
}

// BenchmarkMapSingleLogRecord_NoTraceNoSpan removes trace/span IDs
// to isolate their allocation cost.
func BenchmarkMapSingleLogRecord_NoTraceNoSpan(b *testing.B) {
	mapper := NewMapper()
	record := &logsV1.LogRecord{
		TimeUnixNano:         1234567890,
		ObservedTimeUnixNano: 1234567890,
		SeverityNumber:       logsV1.SeverityNumber_SEVERITY_NUMBER_INFO,
		SeverityText:         "INFO",
		Body: &commonV1.AnyValue{
			Value: &commonV1.AnyValue_StringValue{StringValue: "test log message"},
		},
		Attributes: []*commonV1.KeyValue{
			{
				Key: "attr1",
				Value: &commonV1.AnyValue{
					Value: &commonV1.AnyValue_StringValue{StringValue: "value1"},
				},
			},
			{
				Key: "attr2",
				Value: &commonV1.AnyValue{
					Value: &commonV1.AnyValue_StringValue{StringValue: "value2"},
				},
			},
		},
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = mapper.mapLogRecord(record, nil, "scope", "1.0.0")
	}
}

// BenchmarkNewLogRecord_MakeMap measures the cost of just creating
// a LogRecord with an empty map — the "always allocated" baseline.
func BenchmarkNewLogRecord_MakeMap(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		r := &logRecordLike{
			attributes: make(map[string]int, 0),
		}
		_ = r
	}
}

type logRecordLike struct {
	attributes map[string]int
}

// BenchmarkMakeMap_WithCap vs _WithoutCap shows map pre-sizing impact.
func BenchmarkMakeMap_WithCap4(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		m := make(map[string]string, 4)
		m["a"] = "1"
		m["b"] = "2"
		_ = m
	}
}

func BenchmarkMakeMap_NoCap(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		m := make(map[string]string)
		m["a"] = "1"
		m["b"] = "2"
		_ = m
	}
}

// BenchmarkNewStringValue measures the pointer-boxing allocation
// cost that AttributeValue incurs for every scalar value.
func BenchmarkNewStringValue(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		s := "test-value"
		v := struct{ s *string }{s: &s}
		_ = v
	}
}

// BenchmarkNewStringValue_NoPointer shows the zero-alloc alternative.
func BenchmarkNewStringValue_NoPointer(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		v := struct{ s string }{s: "test-value"}
		_ = v
	}
}

// BenchmarkMakeByteSlice_Copy shows the cost of the make+copy pattern
// used for TraceID and SpanID.
func BenchmarkMakeByteSlice_Copy16(b *testing.B) {
	src := make([]byte, 16)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dst := make([]byte, 16)
		copy(dst, src)
		_ = dst
	}
}

// BenchmarkFixedArray_Copy shows the zero-alloc alternative.
func BenchmarkFixedArray_Copy16(b *testing.B) {
	src := [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dst := src // value copy, no heap alloc
		_ = dst
	}
}
