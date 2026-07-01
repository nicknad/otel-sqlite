package otlp

import (
	"testing"

	commonV1 "codeberg.org/nicknad/otel-sqlite/internal/generated/opentelemetry/proto/common/v1"
	logsV1 "codeberg.org/nicknad/otel-sqlite/internal/generated/opentelemetry/proto/logs/v1"
	resourceV1 "codeberg.org/nicknad/otel-sqlite/internal/generated/opentelemetry/proto/resource/v1"
	"codeberg.org/nicknad/otel-sqlite/internal/model"
)

func TestMapLogsData(t *testing.T) {
	m := NewMapper()

	// Build a minimal ResourceLogs
	rl := &logsV1.ResourceLogs{
		Resource: &resourceV1.Resource{
			Attributes: []*commonV1.KeyValue{
				{Key: "service.name", Value: &commonV1.AnyValue{Value: &commonV1.AnyValue_StringValue{StringValue: "test-svc"}}},
				{Key: "host.name", Value: &commonV1.AnyValue{Value: &commonV1.AnyValue_StringValue{StringValue: "test-host"}}},
			},
		},
		ScopeLogs: []*logsV1.ScopeLogs{
			{
				Scope: &commonV1.InstrumentationScope{
					Name:    "my-scope",
					Version: "1.0.0",
				},
				LogRecords: []*logsV1.LogRecord{
					{
						TimeUnixNano:           1000,
						ObservedTimeUnixNano:   2000,
						SeverityNumber:         logsV1.SeverityNumber_SEVERITY_NUMBER_INFO,
						SeverityText:           "INFO",
						Body:                   &commonV1.AnyValue{Value: &commonV1.AnyValue_StringValue{StringValue: "hello"}},
						TraceId:                []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
						SpanId:                 []byte{0, 1, 2, 3, 4, 5, 6, 7},
						Flags:                  1,
						DroppedAttributesCount: 0,
					},
				},
			},
		},
	}

	batches := m.MapLogsData([]*logsV1.ResourceLogs{rl})
	if len(batches) != 1 {
		t.Fatalf("expected 1 batch, got %d", len(batches))
	}

	batch := batches[0]
	if batch.Resource == nil {
		t.Fatal("expected non-nil resource")
	}
	if batch.Resource.GetServiceName() != "test-svc" {
		t.Errorf("service.name = %q, want %q", batch.Resource.GetServiceName(), "test-svc")
	}
	if batch.Resource.GetHostName() != "test-host" {
		t.Errorf("host.name = %q, want %q", batch.Resource.GetHostName(), "test-host")
	}

	if batch.Size() != 1 {
		t.Fatalf("expected 1 record, got %d", batch.Size())
	}

	rec := batch.Records[0]
	if rec.Timestamp != 1000 {
		t.Errorf("Timestamp = %d, want 1000", rec.Timestamp)
	}
	if rec.ObservedTimestamp != 2000 {
		t.Errorf("ObservedTimestamp = %d, want 2000", rec.ObservedTimestamp)
	}
	if rec.SeverityNumber != model.SeverityInfo {
		t.Errorf("SeverityNumber = %d, want %d", rec.SeverityNumber, model.SeverityInfo)
	}
	if rec.SeverityText != "INFO" {
		t.Errorf("SeverityText = %q, want %q", rec.SeverityText, "INFO")
	}
	if rec.Body != "hello" {
		t.Errorf("Body = %q, want %q", rec.Body, "hello")
	}
	if len(rec.TraceID) != 16 {
		t.Errorf("TraceID length = %d, want 16", len(rec.TraceID))
	}
	if len(rec.SpanID) != 8 {
		t.Errorf("SpanID length = %d, want 8", len(rec.SpanID))
	}
	if rec.ScopeName != "my-scope" {
		t.Errorf("ScopeName = %q, want %q", rec.ScopeName, "my-scope")
	}
	if rec.ScopeVersion != "1.0.0" {
		t.Errorf("ScopeVersion = %q, want %q", rec.ScopeVersion, "1.0.0")
	}
}

func TestMapLogsData_NilInput(t *testing.T) {
	m := NewMapper()
	batches := m.MapLogsData(nil)
	if len(batches) != 0 {
		t.Errorf("expected 0 batches, got %d", len(batches))
	}
}

func TestMapLogsData_EmptyInput(t *testing.T) {
	m := NewMapper()
	batches := m.MapLogsData([]*logsV1.ResourceLogs{})
	if len(batches) != 0 {
		t.Errorf("expected 0 batches, got %d", len(batches))
	}
}

func TestMapAnyValue(t *testing.T) {
	tests := []struct {
		name string
		av   *commonV1.AnyValue
		want model.AttributeValue
	}{
		{"string", &commonV1.AnyValue{Value: &commonV1.AnyValue_StringValue{StringValue: "s"}}, model.NewStringValue("s")},
		{"int", &commonV1.AnyValue{Value: &commonV1.AnyValue_IntValue{IntValue: 42}}, model.NewIntValue(42)},
		{"double", &commonV1.AnyValue{Value: &commonV1.AnyValue_DoubleValue{DoubleValue: 3.14}}, model.NewDoubleValue(3.14)},
		{"bool true", &commonV1.AnyValue{Value: &commonV1.AnyValue_BoolValue{BoolValue: true}}, model.NewBoolValue(true)},
		{"bool false", &commonV1.AnyValue{Value: &commonV1.AnyValue_BoolValue{BoolValue: false}}, model.NewBoolValue(false)},
		{"bytes", &commonV1.AnyValue{Value: &commonV1.AnyValue_BytesValue{BytesValue: []byte{1, 2}}}, model.NewBytesValue([]byte{1, 2})}, //nolint:lll
		{"array", &commonV1.AnyValue{Value: &commonV1.AnyValue_ArrayValue{ArrayValue: &commonV1.ArrayValue{Values: []*commonV1.AnyValue{ //nolint:lll
			{Value: &commonV1.AnyValue_StringValue{StringValue: "a"}},
		}}}}, model.NewArrayValue([]model.AttributeValue{model.NewStringValue("a")})},
		{"nil", nil, model.AttributeValue{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := mapAnyValue(tt.av)
			if got.Type() != tt.want.Type() {
				t.Errorf("Type() = %q, want %q", got.Type(), tt.want.Type())
			}
			if got.String() != tt.want.String() {
				t.Errorf("String() = %q, want %q", got.String(), tt.want.String())
			}
		})
	}
}

func TestMapAnyValueToString(t *testing.T) {
	tests := []struct {
		name string
		av   *commonV1.AnyValue
		want string
	}{
		{"string", &commonV1.AnyValue{Value: &commonV1.AnyValue_StringValue{StringValue: "hello"}}, "hello"},
		{"int", &commonV1.AnyValue{Value: &commonV1.AnyValue_IntValue{IntValue: 42}}, "42"},
		{"double", &commonV1.AnyValue{Value: &commonV1.AnyValue_DoubleValue{DoubleValue: 3.14}}, "3.14"},
		{"negative int", &commonV1.AnyValue{Value: &commonV1.AnyValue_IntValue{IntValue: -5}}, "-5"},
		{"large int", &commonV1.AnyValue{Value: &commonV1.AnyValue_IntValue{IntValue: 1000000}}, "1000000"},
		{"bool true", &commonV1.AnyValue{Value: &commonV1.AnyValue_BoolValue{BoolValue: true}}, "true"},
		{"bool false", &commonV1.AnyValue{Value: &commonV1.AnyValue_BoolValue{BoolValue: false}}, "false"},
		{"bytes", &commonV1.AnyValue{Value: &commonV1.AnyValue_BytesValue{BytesValue: []byte("hello")}}, "hello"},
		{"array", &commonV1.AnyValue{Value: &commonV1.AnyValue_ArrayValue{ArrayValue: &commonV1.ArrayValue{}}}, "[array]"},
		{"kvlist", &commonV1.AnyValue{Value: &commonV1.AnyValue_KvlistValue{KvlistValue: &commonV1.KeyValueList{}}}, "[kvlist]"}, //nolint:lll
		{"nil", nil, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := mapAnyValueToString(tt.av); got != tt.want {
				t.Errorf("mapAnyValueToString() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestMapLogRecordNumericBody(t *testing.T) {
	// Demonstrates the full mapper flow: an OTLP log with a numeric Body
	// must produce the correct string representation, not a garbled character.
	m := NewMapper()

	// Integer body
	intRec := &logsV1.LogRecord{
		TimeUnixNano:   1000,
		SeverityNumber: logsV1.SeverityNumber_SEVERITY_NUMBER_INFO,
		Body:           &commonV1.AnyValue{Value: &commonV1.AnyValue_IntValue{IntValue: 42}},
	}

	rl := &logsV1.ResourceLogs{
		ScopeLogs: []*logsV1.ScopeLogs{
			{LogRecords: []*logsV1.LogRecord{intRec}},
		},
	}

	batches := m.MapLogsData([]*logsV1.ResourceLogs{rl})
	if len(batches) != 1 || batches[0].Size() != 1 {
		t.Fatalf("expected 1 batch with 1 record, got %d batches, %d records",
			len(batches), func() int {
				if len(batches) == 0 {
					return 0
				}
				return batches[0].Size()
			}())
	}
	if batches[0].Records[0].Body != "42" {
		t.Errorf("int body = %q, want %q", batches[0].Records[0].Body, "42")
	}

	// Double body
	doubleRec := &logsV1.LogRecord{
		TimeUnixNano:   2000,
		SeverityNumber: logsV1.SeverityNumber_SEVERITY_NUMBER_INFO,
		Body:           &commonV1.AnyValue{Value: &commonV1.AnyValue_DoubleValue{DoubleValue: 3.14}},
	}

	rl2 := &logsV1.ResourceLogs{
		ScopeLogs: []*logsV1.ScopeLogs{
			{LogRecords: []*logsV1.LogRecord{doubleRec}},
		},
	}

	batches2 := m.MapLogsData([]*logsV1.ResourceLogs{rl2})
	if len(batches2) != 1 || batches2[0].Size() != 1 {
		t.Fatalf("expected 1 batch with 1 record")
	}
	if batches2[0].Records[0].Body != "3.14" {
		t.Errorf("double body = %q, want %q", batches2[0].Records[0].Body, "3.14")
	}

	// Negative integer body
	negRec := &logsV1.LogRecord{
		TimeUnixNano:   3000,
		SeverityNumber: logsV1.SeverityNumber_SEVERITY_NUMBER_INFO,
		Body:           &commonV1.AnyValue{Value: &commonV1.AnyValue_IntValue{IntValue: -5}},
	}

	rl3 := &logsV1.ResourceLogs{
		ScopeLogs: []*logsV1.ScopeLogs{
			{LogRecords: []*logsV1.LogRecord{negRec}},
		},
	}

	batches3 := m.MapLogsData([]*logsV1.ResourceLogs{rl3})
	if len(batches3) != 1 || batches3[0].Size() != 1 {
		t.Fatalf("expected 1 batch with 1 record")
	}
	if batches3[0].Records[0].Body != "-5" {
		t.Errorf("negative int body = %q, want %q", batches3[0].Records[0].Body, "-5")
	}
}

func TestMapLogRecordWithAttributes(t *testing.T) {
	m := NewMapper()

	rec := &logsV1.LogRecord{
		TimeUnixNano:   100,
		SeverityNumber: logsV1.SeverityNumber_SEVERITY_NUMBER_ERROR,
		SeverityText:   "ERROR",
		Body:           &commonV1.AnyValue{Value: &commonV1.AnyValue_StringValue{StringValue: "error occurred"}},
		Attributes: []*commonV1.KeyValue{
			{Key: "error.kind", Value: &commonV1.AnyValue{Value: &commonV1.AnyValue_StringValue{StringValue: "panic"}}},
			{Key: "error.count", Value: &commonV1.AnyValue{Value: &commonV1.AnyValue_IntValue{IntValue: 1}}},
		},
		Flags:     0xFF,
		EventName: "exception",
	}

	rl := &logsV1.ResourceLogs{
		Resource: &resourceV1.Resource{},
		ScopeLogs: []*logsV1.ScopeLogs{
			{
				Scope:      &commonV1.InstrumentationScope{},
				LogRecords: []*logsV1.LogRecord{rec},
			},
		},
	}

	batches := m.MapLogsData([]*logsV1.ResourceLogs{rl})
	if len(batches) != 1 {
		t.Fatalf("expected 1 batch, got %d", len(batches))
	}

	r := batches[0].Records[0]
	if r.Body != "error occurred" {
		t.Errorf("Body = %q, want %q", r.Body, "error occurred")
	}
	if len(r.Attributes) != 2 {
		t.Errorf("expected 2 attributes, got %d", len(r.Attributes))
	}
	if r.Attributes["error.kind"].String() != "panic" {
		t.Errorf("error.kind = %q, want %q", r.Attributes["error.kind"].String(), "panic")
	}
	if r.Flags != 0xFF {
		t.Errorf("Flags = %d, want %d", r.Flags, 0xFF)
	}
	if r.EventName != "exception" {
		t.Errorf("EventName = %q, want %q", r.EventName, "exception")
	}
}
