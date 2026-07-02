package otlp

import (
	"testing"

	commonV1 "codeberg.org/nicknad/otel-sqlite/internal/generated/opentelemetry/proto/common/v1"
	logsV1 "codeberg.org/nicknad/otel-sqlite/internal/generated/opentelemetry/proto/logs/v1"
	resourceV1 "codeberg.org/nicknad/otel-sqlite/internal/generated/opentelemetry/proto/resource/v1"
)

func makeBenchmarkResource(numAttrs int) *logsV1.ResourceLogs {
	attrs := make([]*commonV1.KeyValue, numAttrs)
	for i := 0; i < numAttrs; i++ {
		attrs[i] = &commonV1.KeyValue{
			Key: "attr_key",
			Value: &commonV1.AnyValue{
				Value: &commonV1.AnyValue_StringValue{StringValue: "attr_value"},
			},
		}
	}

	logRecords := make([]*logsV1.LogRecord, 100)
	for i := 0; i < 100; i++ {
		logRecords[i] = &logsV1.LogRecord{
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

	return &logsV1.ResourceLogs{
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
				Scope: &commonV1.InstrumentationScope{
					Name:    "test-scope",
					Version: "1.0.0",
				},
				LogRecords: logRecords,
			},
		},
	}
}

func BenchmarkMapLogsData(b *testing.B) {
	mapper := NewMapper()
	resourceLogs := makeBenchmarkResource(4)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		batches := mapper.MapLogsData([]*logsV1.ResourceLogs{resourceLogs})
		if len(batches) == 0 {
			b.Fatal("expected batches")
		}
	}
}

func BenchmarkMapLogsData_10Attrs(b *testing.B) {
	mapper := NewMapper()
	resourceLogs := makeBenchmarkResource(10)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		batches := mapper.MapLogsData([]*logsV1.ResourceLogs{resourceLogs})
		if len(batches) == 0 {
			b.Fatal("expected batches")
		}
	}
}

func BenchmarkMapSingleLogRecord(b *testing.B) {
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

	resource := &resourceV1.Resource{
		Attributes: []*commonV1.KeyValue{
			{
				Key: "service.name",
				Value: &commonV1.AnyValue{
					Value: &commonV1.AnyValue_StringValue{StringValue: "test-service"},
				},
			},
		},
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = mapper.mapLogRecord(record, nil, "scope", "1.0.0")
		_ = resource // suppress unused warning
	}
}
