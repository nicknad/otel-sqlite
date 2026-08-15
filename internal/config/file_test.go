package config

import (
	"os"
	"path/filepath"
	"testing"
)

// writeTempConfig writes content to a temp YAML file and returns its path.
func writeTempConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write config file: %v", err)
	}
	return path
}

// TestLoadFile_metricsSQLitePathRoundTrip verifies that metrics_sqlite_path
// round-trips through YAML and that it can be disabled by an explicit
// metrics_enabled: false in the same file.
func TestLoadFile_metricsSQLitePathRoundTrip(t *testing.T) {
	path := writeTempConfig(t, "metrics_sqlite_path: \"otel-metrics.db\"\n")

	c := DefaultConfig()
	if err := c.LoadFile(path); err != nil {
		t.Fatalf("LoadFile: %v", err)
	}
	if c.MetricsSQLitePath != "otel-metrics.db" {
		t.Errorf("MetricsSQLitePath = %q, want %q", c.MetricsSQLitePath, "otel-metrics.db")
	}
	// metrics_enabled defaults to true, so the path enables separate-DB mode.
	if !c.MetricsSeparateDB() {
		t.Error("expected MetricsSeparateDB() to be true")
	}
}

// TestLoadFile_metricsEnabledFalseIsApplied guards the pre-existing gap where
// YAML metrics_enabled was silently ignored (only the default and the env
// var worked). Now that it is wired, an explicit false must disable metrics
// and separate-DB mode.
func TestLoadFile_metricsEnabledFalseIsApplied(t *testing.T) {
	path := writeTempConfig(t, "metrics_enabled: false\n")

	c := DefaultConfig()
	if err := c.LoadFile(path); err != nil {
		t.Fatalf("LoadFile: %v", err)
	}
	if c.MetricsEnabled {
		t.Error("metrics_enabled: false was not applied from YAML")
	}
}

// TestLoadFile_metricsSQLitePathWithDisabledMetrics verifies the warning
// path: metrics_sqlite_path set while metrics_enabled is false does not
// error, does not enable separate-DB mode, and produces a startup warning.
func TestLoadFile_metricsSQLitePathWithDisabledMetrics(t *testing.T) {
	path := writeTempConfig(t, "metrics_enabled: false\nmetrics_sqlite_path: \"otel-metrics.db\"\n")

	c := DefaultConfig()
	if err := c.LoadFile(path); err != nil {
		t.Fatalf("LoadFile: %v", err)
	}
	if c.MetricsEnabled {
		t.Error("metrics_enabled: false was not applied from YAML")
	}
	if c.MetricsSQLitePath != "otel-metrics.db" {
		t.Errorf("MetricsSQLitePath = %q, want %q", c.MetricsSQLitePath, "otel-metrics.db")
	}
	if c.MetricsSeparateDB() {
		t.Error("expected shared mode when metrics are disabled")
	}
	if err := c.Validate(); err != nil {
		t.Errorf("Validate() = %v, want nil (warning, not error)", err)
	}
	if w := c.MetricsSeparateDBWarning(); w == "" {
		t.Error("expected a startup warning for metrics_sqlite_path with metrics disabled")
	}
}

// TestLoadFile_emptyMetricsSQLitePathKeepsSharedMode verifies the default:
// an empty (or absent) metrics_sqlite_path keeps metrics on the shared log DB.
func TestLoadFile_emptyMetricsSQLitePathKeepsSharedMode(t *testing.T) {
	path := writeTempConfig(t, "metrics_enabled: true\n")

	c := DefaultConfig()
	if err := c.LoadFile(path); err != nil {
		t.Fatalf("LoadFile: %v", err)
	}
	if c.MetricsSeparateDB() {
		t.Error("expected shared mode with empty metrics_sqlite_path")
	}
}
