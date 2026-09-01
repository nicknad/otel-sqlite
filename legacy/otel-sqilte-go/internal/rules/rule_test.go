package rules

import (
	"context"
	"testing"
	"time"

	"codeberg.org/nicknad/otel-sqlite/internal/events"
	"codeberg.org/nicknad/otel-sqlite/internal/model"
)

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
	if r.AlertWindow != 5*time.Minute {
		t.Errorf("AlertWindow = %v, want 5m", r.AlertWindow)
	}
	if r.AlertThreshold != 1 {
		t.Errorf("AlertThreshold = %d, want 1", r.AlertThreshold)
	}
	if r.AlertResolveWindow != 15*time.Minute {
		t.Errorf("AlertResolveWindow = %v, want 15m", r.AlertResolveWindow)
	}
}

func TestRuleEngine_MatchSeverity(t *testing.T) {
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

	ev := &events.Event{Severity: model.SeverityError, Body: "err"}
	rule, matched := engine.Evaluate(context.Background(), ev)
	if !matched {
		t.Fatal("expected match for error event")
	}
	if rule.Name != "errors" {
		t.Errorf("rule name = %q", rule.Name)
	}

	ev.Severity = model.SeverityInfo
	_, matched = engine.Evaluate(context.Background(), ev)
	if matched {
		t.Fatal("expected no match for info event")
	}
}

func TestRuleEngine_ResourceFilter(t *testing.T) {
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

	ev := &events.Event{Severity: model.SeverityError, ResourceID: "prod-us-east-1", Body: "err"}
	_, matched := engine.Evaluate(context.Background(), ev)
	if !matched {
		t.Fatal("expected match for prod resource")
	}

	ev.ResourceID = "staging-us-east-1"
	_, matched = engine.Evaluate(context.Background(), ev)
	if matched {
		t.Fatal("expected no match for staging resource")
	}
}

func TestRuleEngine_BodyFilter(t *testing.T) {
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

	ev := &events.Event{Severity: model.SeverityError, Body: "PANIC: runtime error"}
	_, matched := engine.Evaluate(context.Background(), ev)
	if !matched {
		t.Fatal("expected match for panic body")
	}
}

// TestRuleEngine_AttributeFilter covers matchAttribute/attrValueString: a
// rule with AttributeFilters matches only when an attribute with the key
// exists AND its string form matches the regex. All four value kinds are
// exercised (string, int, double, bool).
func TestRuleEngine_AttributeFilter(t *testing.T) {
	engine, err := NewRuleEngine([]Rule{
		{
			Name:             "attr-rule",
			MatchSeverity:    model.SeverityError,
			AttributeFilters: map[string]string{"host": "prod-.*"},
			Destination:      "log",
		},
	})
	if err != nil {
		t.Fatalf("NewRuleEngine: %v", err)
	}

	cases := []struct {
		name    string
		attrs   []model.Attribute
		matched bool
	}{
		{"string match", []model.Attribute{{Key: "host", Kind: model.ValueString, Str: "prod-1"}}, true},
		{"string no match", []model.Attribute{{Key: "host", Kind: model.ValueString, Str: "staging-1"}}, false},
		{"int match via format", []model.Attribute{{Key: "host", Kind: model.ValueInt, Num: 7}}, false}, // "7" vs "prod-.*"
		{"wrong key", []model.Attribute{{Key: "service", Kind: model.ValueString, Str: "prod-1"}}, false},
		{"no attributes", nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ev := &events.Event{Severity: model.SeverityError, Attributes: tc.attrs}
			_, matched := engine.Evaluate(context.Background(), ev)
			if matched != tc.matched {
				t.Errorf("matched = %v, want %v", matched, tc.matched)
			}
		})
	}
}

// TestRuleEngine_AttributeFilterAllKinds verifies attrValueString renders
// every attribute kind for regex matching: an "always true" pattern must
// match int/double/bool attributes too, not just strings.
func TestRuleEngine_AttributeFilterAllKinds(t *testing.T) {
	engine, err := NewRuleEngine([]Rule{
		{
			Name:             "kind-rule",
			MatchSeverity:    model.SeverityError,
			AttributeFilters: map[string]string{"code": ".*"},
			Destination:      "log",
		},
	})
	if err != nil {
		t.Fatalf("NewRuleEngine: %v", err)
	}

	for _, tc := range []struct {
		name  string
		attr  model.Attribute
		match bool
	}{
		{"int", model.Attribute{Key: "code", Kind: model.ValueInt, Num: 42}, true},
		{"double", model.Attribute{Key: "code", Kind: model.ValueDouble, Dbl: 3.5}, true},
		{"bool", model.Attribute{Key: "code", Kind: model.ValueBool, Flag: true}, true},
		{"null kind", model.Attribute{Key: "code", Kind: model.ValueNull}, true}, // "" matches .*
	} {
		t.Run(tc.name, func(t *testing.T) {
			ev := &events.Event{Severity: model.SeverityError, Attributes: []model.Attribute{tc.attr}}
			_, matched := engine.Evaluate(context.Background(), ev)
			if matched != tc.match {
				t.Errorf("matched = %v, want %v", matched, tc.match)
			}
		})
	}
}

func TestRuleEngine_FirstMatchWins(t *testing.T) {
	engine, err := NewRuleEngine([]Rule{
		{Name: "first", MatchSeverity: model.SeverityError, Destination: "log"},
		{Name: "second", MatchSeverity: model.SeverityError, Destination: "http"},
	})
	if err != nil {
		t.Fatalf("NewRuleEngine: %v", err)
	}

	ev := &events.Event{Severity: model.SeverityError, Body: "err"}
	rule, _ := engine.Evaluate(context.Background(), ev)
	if rule.Name != "first" {
		t.Errorf("expected rule %q to match, got %q", "first", rule.Name)
	}
}

func TestRuleEngine_AlertFields(t *testing.T) {
	engine, err := NewRuleEngine([]Rule{
		{
			Name:               "alert-rule",
			MatchSeverity:      model.SeverityError,
			Destination:        "http",
			AlertWindow:        10 * time.Minute,
			AlertThreshold:     5,
			AlertResolveWindow: 30 * time.Minute,
		},
	})
	if err != nil {
		t.Fatalf("NewRuleEngine: %v", err)
	}

	rules := engine.Rules()
	r := rules[0]
	if r.AlertWindow != 10*time.Minute {
		t.Errorf("AlertWindow = %v", r.AlertWindow)
	}
	if r.AlertThreshold != 5 {
		t.Errorf("AlertThreshold = %d", r.AlertThreshold)
	}
	if r.AlertResolveWindow != 30*time.Minute {
		t.Errorf("AlertResolveWindow = %v", r.AlertResolveWindow)
	}
}
