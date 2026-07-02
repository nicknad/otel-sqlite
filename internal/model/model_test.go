package model

import (
	"testing"
	"time"
)

func TestSeverityString(t *testing.T) {
	tests := []struct {
		s    Severity
		want string
	}{
		{SeverityUnspecified, "UNSPECIFIED"},
		{SeverityTrace, "TRACE"},
		{SeverityDebug, "DEBUG"},
		{SeverityInfo, "INFO"},
		{SeverityWarn, "WARN"},
		{SeverityError, "ERROR"},
		{SeverityFatal, "FATAL"},
		{Severity(99), "UNSPECIFIED"},
	}
	for _, tt := range tests {
		if got := tt.s.String(); got != tt.want {
			t.Errorf("Severity(%d).String() = %q, want %q", tt.s, got, tt.want)
		}
	}
}

func TestNewLogRecord(t *testing.T) {
	r := NewLogRecord()
	if r.Attributes == nil {
		t.Error("expected non-nil Attributes map")
	}
	if len(r.Attributes) != 0 {
		t.Error("expected empty Attributes map")
	}
}

func TestLogRecordTimestamps(t *testing.T) {
	now := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	r := &LogRecord{Timestamp: now.UnixNano(), ObservedTimestamp: now.Add(time.Second).UnixNano()}

	ts := r.TimestampTime()
	if !ts.Equal(now) {
		t.Errorf("TimestampTime() = %v, want %v", ts, now)
	}

	ots := r.ObservedTimestampTime()
	if !ots.Equal(now.Add(time.Second)) {
		t.Errorf("ObservedTimestampTime() = %v, want %v", ots, now.Add(time.Second))
	}
}

func TestHasTraceContext(t *testing.T) {
	tests := []struct {
		name     string
		hasTrace bool
		hasSpan  bool
		want     bool
	}{
		{"valid both", true, true, true},
		{"no trace", false, true, false},
		{"no span", true, false, false},
		{"neither", false, false, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &LogRecord{HasTrace: tt.hasTrace, HasSpan: tt.hasSpan}
			if got := r.HasTraceContext(); got != tt.want {
				t.Errorf("HasTraceContext() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLogBatch(t *testing.T) {
	b := NewLogBatch(10)
	if !b.IsEmpty() {
		t.Error("expected empty batch")
	}
	if b.Size() != 0 {
		t.Errorf("expected size 0, got %d", b.Size())
	}

	r1 := NewLogRecord()
	r2 := NewLogRecord()
	b.AddRecord(r1)
	b.AddRecord(r2)

	if b.IsEmpty() {
		t.Error("expected non-empty batch")
	}
	if b.Size() != 2 {
		t.Errorf("expected size 2, got %d", b.Size())
	}

	b.Clear()
	if !b.IsEmpty() {
		t.Error("expected empty after Clear()")
	}
}

func TestResource(t *testing.T) {
	attrs := map[string]AttributeValue{
		"service.name": NewStringValue("my-service"),
		"host.name":    NewStringValue("my-host"),
	}
	r := NewResource(attrs)
	r.ID = "res-123"

	if r.GetServiceName() != "my-service" {
		t.Errorf("GetServiceName() = %q, want %q", r.GetServiceName(), "my-service")
	}
	if r.GetHostName() != "my-host" {
		t.Errorf("GetHostName() = %q, want %q", r.GetHostName(), "my-host")
	}

	// Empty resource
	r2 := NewResource(nil)
	if r2.GetServiceName() != "" {
		t.Errorf("expected empty service name, got %q", r2.GetServiceName())
	}
	if r2.GetHostName() != "" {
		t.Errorf("expected empty host name, got %q", r2.GetHostName())
	}
}

func TestAttributeValueConstructors(t *testing.T) {
	tests := []struct {
		name string
		val  AttributeValue
		typ  string
		str  string
	}{
		{"string", NewStringValue("hello"), "string", "hello"},
		{"int", NewIntValue(42), "int", "42"},
		{"double", NewDoubleValue(3.14), "double", "3.14"},
		{"bool true", NewBoolValue(true), "bool", "true"},
		{"bool false", NewBoolValue(false), "bool", "false"},
		{"bytes", NewBytesValue([]byte{1, 2, 3}), "bytes", "[bytes:3]"},
		{"array", NewArrayValue([]AttributeValue{NewStringValue("a")}), "array", "[array:1]"},
		{"map", NewMapValue(map[string]AttributeValue{"k": NewStringValue("v")}), "map", "[map:1]"},
		{"null", AttributeValue{}, "null", "null"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.val.Type(); got != tt.typ {
				t.Errorf("Type() = %q, want %q", got, tt.typ)
			}
			if got := tt.val.String(); got != tt.str {
				t.Errorf("String() = %q, want %q", got, tt.str)
			}
		})
	}
}
