package events

import (
	"testing"

	"codeberg.org/nicknad/otel-sqlite/internal/model"
)

func TestEventFingerprint_Deterministic(t *testing.T) {
	event := &Event{
		Body:       "disk full",
		ResourceID: "host-1",
		Attributes: []model.Attribute{
			{Key: "host.name", Str: "srv1", Kind: model.ValueString},
		},
	}
	fp1 := event.Fingerprint()
	fp2 := event.Fingerprint()
	if fp1 != fp2 {
		t.Errorf("fingerprint not idempotent: %q != %q", fp1, fp2)
	}
}

func TestEventFingerprint_DifferentBody(t *testing.T) {
	e1 := &Event{Body: "error A"}
	e2 := &Event{Body: "error B"}
	if e1.Fingerprint() == e2.Fingerprint() {
		t.Error("different bodies should produce different fingerprints")
	}
}

func TestEventFingerprint_DifferentAttributes(t *testing.T) {
	e1 := &Event{
		Body: "error",
		Attributes: []model.Attribute{
			{Key: "svc", Str: "auth", Kind: model.ValueString},
		},
	}
	e2 := &Event{
		Body: "error",
		Attributes: []model.Attribute{
			{Key: "svc", Str: "pay", Kind: model.ValueString},
		},
	}
	if e1.Fingerprint() == e2.Fingerprint() {
		t.Error("different attributes should produce different fingerprints")
	}
}

func TestEventFingerprint_Caching(t *testing.T) {
	e := &Event{Body: "test"}
	fp1 := e.Fingerprint()
	// Modify the event (this shouldn't change the cached fingerprint).
	e.Body = "changed"
	fp2 := e.Fingerprint()
	if fp1 != fp2 {
		t.Error("cached fingerprint was recomputed after mutation")
	}
}

func TestEventFromLogRecord(t *testing.T) {
	record := &model.LogRecord{
		SeverityNumber: model.SeverityError,
		SeverityText:   "ERROR",
		Body:           "something broke",
		ResourceID:     "res-42",
		Timestamp:      1234567890,
		ScopeName:      "test-scope",
	}

	event := EventFromLogRecord(record)
	if event.Severity != model.SeverityError {
		t.Errorf("severity = %v", event.Severity)
	}
	if event.Body != "something broke" {
		t.Errorf("body = %q", event.Body)
	}
	if event.ResourceID != "res-42" {
		t.Errorf("resourceID = %q", event.ResourceID)
	}
}
