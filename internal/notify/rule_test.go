package notify

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"codeberg.org/nicknad/otel-sqlite/internal/model"
)

func TestDefaultRuleEngine_MatchSeverity(t *testing.T) {
	engine, err := NewRuleEngine([]Rule{
		{
			Name:          "errors",
			MatchSeverity: model.SeverityError,
			Destination:   "log",
		},
	})
	if err != nil {
		t.Fatalf("NewRuleEngine: %v", err)
	}

	// Error event matches.
	event := &Event{
		Severity: model.SeverityError,
		Body:     "something went wrong",
	}
	rule, key, matched := engine.Evaluate(context.Background(), event)
	if !matched {
		t.Fatal("expected match for error event")
	}
	if rule.Name != "errors" {
		t.Errorf("rule name = %q, want %q", rule.Name, "errors")
	}
	if key == "" {
		t.Error("key should not be empty")
	}

	// Info event doesn't match.
	event.Severity = model.SeverityInfo
	_, _, matched = engine.Evaluate(context.Background(), event)
	if matched {
		t.Fatal("expected no match for info event")
	}
}

func TestDefaultRuleEngine_ResourceFilter(t *testing.T) {
	engine, err := NewRuleEngine([]Rule{
		{
			Name:           "prod-errors",
			MatchSeverity:  model.SeverityError,
			ResourceFilter: "prod-.*",
			Destination:    "log",
		},
	})
	if err != nil {
		t.Fatalf("NewRuleEngine: %v", err)
	}

	// Matching resource.
	event := &Event{
		Severity:   model.SeverityError,
		ResourceID: "prod-us-east-1",
		Body:       "error",
	}
	_, _, matched := engine.Evaluate(context.Background(), event)
	if !matched {
		t.Fatal("expected match for prod resource")
	}

	// Non-matching resource.
	event.ResourceID = "staging-us-east-1"
	_, _, matched = engine.Evaluate(context.Background(), event)
	if matched {
		t.Fatal("expected no match for staging resource")
	}
}

func TestDefaultRuleEngine_BodyFilter(t *testing.T) {
	engine, err := NewRuleEngine([]Rule{
		{
			Name:          "panic-errors",
			MatchSeverity: model.SeverityError,
			BodyFilter:    "(?i)panic",
			Destination:   "log",
		},
	})
	if err != nil {
		t.Fatalf("NewRuleEngine: %v", err)
	}

	event := &Event{
		Severity: model.SeverityError,
		Body:     "PANIC: runtime error",
	}
	_, _, matched := engine.Evaluate(context.Background(), event)
	if !matched {
		t.Fatal("expected match for panic body")
	}

	event.Body = "normal error"
	_, _, matched = engine.Evaluate(context.Background(), event)
	if matched {
		t.Fatal("expected no match for normal body")
	}
}

func TestDefaultRuleEngine_AttributeFilter(t *testing.T) {
	engine, err := NewRuleEngine([]Rule{
		{
			Name:             "service-errors",
			MatchSeverity:    model.SeverityError,
			AttributeFilters: map[string]string{"service.name": "payment"},
			Destination:      "log",
		},
	})
	if err != nil {
		t.Fatalf("NewRuleEngine: %v", err)
	}

	event := &Event{
		Severity: model.SeverityError,
		Body:     "error",
		Attributes: []model.Attribute{
			{Key: "service.name", Str: "payment-service", Kind: model.ValueString},
		},
	}
	_, _, matched := engine.Evaluate(context.Background(), event)
	if !matched {
		t.Fatal("expected match for payment service")
	}

	event.Attributes = []model.Attribute{
		{Key: "service.name", Str: "auth-service", Kind: model.ValueString},
	}
	_, _, matched = engine.Evaluate(context.Background(), event)
	if matched {
		t.Fatal("expected no match for auth service")
	}
}

func TestDefaultRuleEngine_NoMatchNoRules(t *testing.T) {
	engine, err := NewRuleEngine(nil)
	if err != nil {
		t.Fatalf("NewRuleEngine: %v", err)
	}

	event := &Event{Severity: model.SeverityError}
	_, _, matched := engine.Evaluate(context.Background(), event)
	if matched {
		t.Fatal("no rules should mean no match")
	}
}

func TestDefaultRuleEngine_FirstMatchWins(t *testing.T) {
	engine, err := NewRuleEngine([]Rule{
		{
			Name:          "first",
			MatchSeverity: model.SeverityError,
			Destination:   "log",
		},
		{
			Name:          "second",
			MatchSeverity: model.SeverityError,
			Destination:   "http",
		},
	})
	if err != nil {
		t.Fatalf("NewRuleEngine: %v", err)
	}

	event := &Event{Severity: model.SeverityError, Body: "err"}
	rule, _, matched := engine.Evaluate(context.Background(), event)
	if !matched {
		t.Fatal("expected match")
	}
	if rule.Name != "first" {
		t.Errorf("expected rule %q to match, got %q", "first", rule.Name)
	}
}

func TestDefaultRuleEngine_KeyConsistency(t *testing.T) {
	engine, err := NewRuleEngine([]Rule{
		{
			Name:          "errors",
			MatchSeverity: model.SeverityError,
			Destination:   "log",
		},
	})
	if err != nil {
		t.Fatalf("NewRuleEngine: %v", err)
	}

	event1 := &Event{
		Severity:   model.SeverityError,
		ResourceID: "res1",
		Body:       "same error",
		Attributes: []model.Attribute{
			{Key: "a", Str: "1", Kind: model.ValueString},
		},
	}
	event2 := &Event{
		Severity:   model.SeverityError,
		ResourceID: "res1",
		Body:       "same error",
		Attributes: []model.Attribute{
			{Key: "a", Str: "1", Kind: model.ValueString},
		},
	}

	_, key1, _ := engine.Evaluate(context.Background(), event1)
	_, key2, _ := engine.Evaluate(context.Background(), event2)

	if key1 != key2 {
		t.Errorf("keys should be identical for identical events: %q != %q", key1, key2)
	}

	// Different body changes the key.
	event3 := &Event{
		Severity:   model.SeverityError,
		ResourceID: "res1",
		Body:       "different error",
	}
	_, key3, _ := engine.Evaluate(context.Background(), event3)
	if key1 == key3 {
		t.Error("keys should differ for different events")
	}
}

func TestRuleDefaults(t *testing.T) {
	engine, err := NewRuleEngine([]Rule{
		{
			Name:          "defaults-test",
			MatchSeverity: model.SeverityError,
			Destination:   "log",
		},
	})
	if err != nil {
		t.Fatalf("NewRuleEngine: %v", err)
	}

	rules := engine.Rules()
	if len(rules) != 1 {
		t.Fatalf("expected 1 rule, got %d", len(rules))
	}
	r := rules[0]
	if r.MaxRetries != 3 {
		t.Errorf("MaxRetries = %d, want 3", r.MaxRetries)
	}
	if r.RetryBackoff != 30*time.Second {
		t.Errorf("RetryBackoff = %v, want 30s", r.RetryBackoff)
	}
}

// ---------------------------------------------------------------------------
// Regression tests for fixes
// ---------------------------------------------------------------------------

func TestBackoffDuration(t *testing.T) {
	base := 10 * time.Second

	tests := []struct {
		attempt int
		want    int64
	}{
		{0, int64(10 * time.Second)},
		{1, int64(10 * time.Second)},       // 10 * 2^0
		{2, int64(20 * time.Second)},       // 10 * 2^1
		{3, int64(40 * time.Second)},       // 10 * 2^2
		{4, int64(80 * time.Second)},       // 10 * 2^3
		{12, int64(10*time.Second) * 1024}, // 10 * 2^10 (max cap)
		{50, int64(10*time.Second) * 1024}, // overflow capped at 2^10
	}

	for _, tt := range tests {
		got := backoffDuration(base, tt.attempt)
		if got != tt.want {
			t.Errorf("backoffDuration(%v, %d) = %d, want %d", base, tt.attempt, got, tt.want)
		}
	}
}

func TestIsRetryable(t *testing.T) {
	// Regular error is retryable.
	if !IsRetryable(errors.New("connection refused")) {
		t.Error("regular errors should be retryable")
	}

	// Wrapped non-retryable is not retryable.
	nr := NewNotRetryableError(errors.New("401 unauthorized"))
	if IsRetryable(nr) {
		t.Error("non-retryable errors should not be retryable")
	}

	// fmt.Errorf wrapping a non-retryable preserves non-retryability.
	wrapped := fmt.Errorf("send failed: %w", nr)
	if IsRetryable(wrapped) {
		t.Error("wrapped non-retryable errors should not be retryable")
	}

	// nil is retryable (no error to block).
	if !IsRetryable(nil) {
		t.Error("nil should be retryable (no error)")
	}
}

func TestStateKeyRoundtrip(t *testing.T) {
	tests := []StateKey{
		{RuleName: "errors", ResourceID: "res-1", Fingerprint: "abc123"},
		{RuleName: "panic-rule", ResourceID: "res-2", Fingerprint: "deadbeef"},
		{RuleName: "r", ResourceID: "", Fingerprint: "f"},
	}

	for _, sk := range tests {
		encoded := sk.Encode()
		decoded := DecodeStateKey(encoded)
		if decoded != sk {
			t.Errorf("StateKey roundtrip failed: %+v → %q → %+v", sk, encoded, decoded)
		}
	}

	// Malformed keys.
	if got := DecodeStateKey("no-colons"); got != (StateKey{}) {
		t.Errorf("malformed key should return zero StateKey, got %+v", got)
	}
	if got := DecodeStateKey("one:colon"); got != (StateKey{}) {
		t.Errorf("one-colon key should return zero StateKey, got %+v", got)
	}
}

func TestEventFingerprintCaching(t *testing.T) {
	event := &Event{
		Body:       "test error",
		ResourceID: "res-1",
		Attributes: []model.Attribute{
			{Key: "host", Str: "srv1", Kind: model.ValueString},
		},
	}

	fp1 := event.Fingerprint()
	fp2 := event.Fingerprint()

	if fp1 != fp2 {
		t.Errorf("Fingerprint() not idempotent: %q != %q", fp1, fp2)
	}
	if fp1 == "" {
		t.Error("Fingerprint() should not be empty")
	}

	// Different body → different fingerprint.
	event2 := &Event{
		Body:       "different error",
		ResourceID: "res-1",
	}
	if event2.Fingerprint() == fp1 {
		t.Error("different events should have different fingerprints")
	}

	// Same body + same attributes = same fingerprint (deterministic).
	event3 := &Event{
		Body:       "test error",
		ResourceID: "res-1",
		Attributes: []model.Attribute{
			{Key: "host", Str: "srv1", Kind: model.ValueString},
		},
	}
	if event3.Fingerprint() != fp1 {
		t.Error("identical events should have identical fingerprints")
	}
}
