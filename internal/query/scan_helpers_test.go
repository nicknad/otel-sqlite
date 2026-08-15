package query

import (
	"database/sql"
	"math"
	"testing"
)

// In-package tests for the small scan/parse helpers, covering the invalid
// (NULL) branches that are hard to reach through full-store tests.

func TestScanHelpers_NullBranches(t *testing.T) {
	if got := scanUint64(sql.NullInt64{Valid: false}); got != nil {
		t.Errorf("scanUint64(NULL) = %v, want nil", got)
	}
	if got := scanBytes(sql.NullString{Valid: false}); got != nil {
		t.Errorf("scanBytes(NULL) = %v, want nil", got)
	}
	if got := scanInt64(sql.NullInt64{Valid: false}); got != nil {
		t.Errorf("scanInt64(NULL) = %v, want nil", got)
	}
}

func TestScanHelpers_ValidBranches(t *testing.T) {
	if got := scanUint64(sql.NullInt64{Valid: true, Int64: 7}); got == nil || *got != 7 {
		t.Errorf("scanUint64(7) = %v, want 7", got)
	}
	if got := scanBytes(sql.NullString{Valid: true, String: "abc"}); string(got) != "abc" {
		t.Errorf("scanBytes(abc) = %q, want abc", got)
	}
	if got := scanInt64(sql.NullInt64{Valid: true, Int64: -3}); got == nil || *got != -3 {
		t.Errorf("scanInt64(-3) = %v, want -3", got)
	}
}

// TestParseJSONFloat_MarkerForms pins the JSON float encoding contract for
// both extraction styles: bare markers ("NaN", "+Inf", "-Inf" — as produced
// by json_each's ->> operator) and quoted markers (as produced by the ->
// operator). Go's %g verb parses the bare forms natively; the quoted forms
// are handled explicitly.
func TestParseJSONFloat_MarkerForms(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want func(float64) bool
	}{
		{"bare NaN", "NaN", math.IsNaN},
		{"bare +Inf", "+Inf", func(f float64) bool { return math.IsInf(f, 1) }},
		{"bare -Inf", "-Inf", func(f float64) bool { return math.IsInf(f, -1) }},
		{"quoted NaN", `"NaN"`, math.IsNaN},
		{"quoted +Inf", `"+Inf"`, func(f float64) bool { return math.IsInf(f, 1) }},
		{"quoted -Inf", `"-Inf"`, func(f float64) bool { return math.IsInf(f, -1) }},
		{"finite", "7.5", func(f float64) bool { return f == 7.5 }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseJSONFloat(tt.in)
			if err != nil {
				t.Fatalf("parseJSONFloat(%q) error: %v", tt.in, err)
			}
			if !tt.want(got) {
				t.Errorf("parseJSONFloat(%q) = %v, unexpected", tt.in, got)
			}
		})
	}
}

func TestParseJSONFloat_Invalid(t *testing.T) {
	if _, err := parseJSONFloat("not-a-number"); err == nil {
		t.Error("parseJSONFloat(garbage) should error")
	}
}
