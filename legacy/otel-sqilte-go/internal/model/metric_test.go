package model

import (
	"testing"
)

func TestCanonicalAttributesKeyOrderIndependent(t *testing.T) {
	a := []Attribute{
		{Key: "method", Str: "GET", Kind: ValueString},
		{Key: "status_code", Num: 200, Kind: ValueInt},
	}
	b := []Attribute{
		{Key: "status_code", Num: 200, Kind: ValueInt},
		{Key: "method", Str: "GET", Kind: ValueString},
	}
	keyA := CanonicalAttributesKey(a)
	keyB := CanonicalAttributesKey(b)
	if string(keyA) != string(keyB) {
		t.Errorf("canonical keys differ by order:\n  %s\n  %s", keyA, keyB)
	}
	if string(keyA) != `{"method":"GET","status_code":200}` {
		t.Errorf("unexpected canonical key: %s", keyA)
	}
}

func TestCanonicalAttributesKeyLastWins(t *testing.T) {
	key := CanonicalAttributesKey([]Attribute{
		{Key: "dup", Str: "first", Kind: ValueString},
		{Key: "dup", Str: "last", Kind: ValueString},
	})
	if string(key) != `{"dup":"last"}` {
		t.Errorf("duplicate keys must be last-write-wins, got %s", key)
	}
}

func TestCanonicalAttributesKeyEmpty(t *testing.T) {
	if got := CanonicalAttributesKey(nil); string(got) != "{}" {
		t.Errorf("empty canonical key = %s, want {}", got)
	}
}

func TestCanonicalAttributesKeyValueTypes(t *testing.T) {
	key := CanonicalAttributesKey([]Attribute{
		{Key: "s", Str: "v", Kind: ValueString},
		{Key: "i", Num: -7, Kind: ValueInt},
		{Key: "d", Dbl: 3.5, Kind: ValueDouble},
		{Key: "b", Flag: true, Kind: ValueBool},
		{Key: "n", Kind: ValueNull},
		{Key: "e", Kind: ValueBytes, Raw: []byte{1, 2}},
	})
	if string(key) != `{"b":true,"d":3.5,"e":"\u0001\u0002","i":-7,"n":null,"s":"v"}` {
		t.Errorf("unexpected canonical key: %s", key)
	}
}

func TestMetricBatchSize(t *testing.T) {
	batch := NewMetricBatch(2)
	if !batch.IsEmpty() {
		t.Error("new batch must be empty")
	}
	if batch.Size() != 0 {
		t.Errorf("empty batch size = %d", batch.Size())
	}

	m := &Metric{
		Series: []*MetricSeries{
			{DataPoints: []*DataPoint{{}, {}}},
			{DataPoints: []*DataPoint{}},
			{DataPoints: []*DataPoint{{}, {}, {}}},
		},
	}
	batch.AddMetric(m)
	if batch.Size() != 5 {
		t.Errorf("batch size = %d, want 5 (data points, not metrics)", batch.Size())
	}
	if batch.IsEmpty() {
		t.Error("batch with points must not be empty")
	}
	batch.Clear()
	if !batch.IsEmpty() {
		t.Error("cleared batch must be empty")
	}
}

func TestMetricTypeString(t *testing.T) {
	cases := map[MetricType]string{
		MetricTypeGauge:                "gauge",
		MetricTypeSum:                  "sum",
		MetricTypeHistogram:            "histogram",
		MetricTypeExponentialHistogram: "exponential_histogram",
		MetricTypeSummary:              "summary",
		MetricType(99):                 "unknown",
	}
	for typ, want := range cases {
		if got := typ.String(); got != want {
			t.Errorf("MetricType(%d).String() = %q, want %q", typ, got, want)
		}
	}
}

func TestTemporalityString(t *testing.T) {
	cases := map[AggregationTemporality]string{
		TemporalityUnspecified: "unspecified",
		TemporalityDelta:       "delta",
		TemporalityCumulative:  "cumulative",
	}
	for tmp, want := range cases {
		if got := tmp.String(); got != want {
			t.Errorf("AggregationTemporality(%d).String() = %q, want %q", tmp, got, want)
		}
	}
}
