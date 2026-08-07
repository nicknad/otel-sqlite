package sqlite

import (
	"encoding/json"
	"math"
	"testing"

	"codeberg.org/nicknad/otel-sqlite/internal/model"
)

func TestStoredSeverityText(t *testing.T) {
	tests := []struct {
		name     string
		number   model.Severity
		text     string
		expected any
	}{
		{"empty", model.SeverityInfo, "", nil},
		{"standard", model.SeverityInfo, "INFO", nil},
		{"unspecified", model.SeverityUnspecified, "UNSPECIFIED", nil},
		{"custom", model.SeverityInfo, "informational", "informational"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual := storedSeverityText(&model.LogRecord{
				SeverityNumber: test.number,
				SeverityText:   test.text,
			})
			if actual != test.expected {
				t.Fatalf("stored severity text = %#v, want %#v", actual, test.expected)
			}
		})
	}
}

func TestMarshalEventAttrs(t *testing.T) {
	encoded, err := marshalEventAttrs([]model.Attribute{
		{Key: "string", Str: "value", Kind: model.ValueString},
		{Key: "int", Num: 42, Kind: model.ValueInt},
		{Key: "double", Dbl: 3.5, Kind: model.ValueDouble},
		{Key: "bool", Flag: true, Kind: model.ValueBool},
		{Key: "bytes", Raw: []byte{0, 255}, Kind: model.ValueBytes},
		{Key: "null", Kind: model.ValueNull},
		{Key: "duplicate", Str: "first", Kind: model.ValueString},
		{Key: "duplicate", Str: "last", Kind: model.ValueString},
	})
	if err != nil {
		t.Fatal(err)
	}

	var values map[string]interface{}
	if err := json.Unmarshal([]byte(encoded), &values); err != nil {
		t.Fatal(err)
	}
	if values["string"] != "value" || values["int"] != float64(42) || values["double"] != 3.5 || values["bool"] != true || values["null"] != nil || values["duplicate"] != "last" {
		t.Fatalf("unexpected JSON: %s", encoded)
	}
	bytes, ok := values["bytes"].(map[string]interface{})
	if !ok || bytes["$b"] != "AP8=" {
		t.Fatalf("unexpected bytes: %#v", values["bytes"])
	}

	empty, err := marshalEventAttrs(nil)
	if err != nil || empty != "{}" {
		t.Fatalf("empty attributes = %q, err = %v", empty, err)
	}
}

func TestMarshalEventAttrsRejectsNonFiniteDouble(t *testing.T) {
	for _, value := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		if _, err := marshalEventAttrs([]model.Attribute{{Key: "bad", Dbl: value, Kind: model.ValueDouble}}); err == nil {
			t.Errorf("marshalEventAttrs(%v) returned no error", value)
		}
	}
}
