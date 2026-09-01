package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
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

// TestParseOptionalDuration covers all three branches of the file-loading
// duration helper: empty (leave target), invalid (error), valid (set).
func TestParseOptionalDuration(t *testing.T) {
	var d time.Duration

	if err := parseOptionalDuration("", &d, "empty"); err != nil {
		t.Errorf("empty: %v", err)
	}
	if d != 0 {
		t.Errorf("empty: target = %v, want unchanged", d)
	}

	if err := parseOptionalDuration("soon", &d, "bad"); err == nil {
		t.Error("invalid: expected error")
	}

	if err := parseOptionalDuration("45s", &d, "valid"); err != nil {
		t.Errorf("valid: %v", err)
	}
	if d != 45*time.Second {
		t.Errorf("valid: target = %v, want 45s", d)
	}
}

// TestParseRuleDurationField_UnknownFieldIsNoop guards the field-map lookup:
// an unrecognized field name must be a no-op, not an error.
func TestParseRuleDurationField_UnknownFieldIsNoop(t *testing.T) {
	fr := &FileRuleConfig{Cooldown: "1m"}
	called := false
	err := parseRuleDurationField(fr, "not_a_field", func(string) { called = true })
	if err != nil {
		t.Fatalf("parseRuleDurationField: %v", err)
	}
	if called {
		t.Error("setter called for unknown field")
	}
}

// TestLoadFile_metricsSQLitePathRoundTrip verifies that metrics_sqlite_path
// round-trips through YAML and that it can be disabled by an explicit
// metrics_enabled: false in the same file.
// TestLoadFile_notificationFull exercises the notification section: scalar
// durations (incl. the "d" days suffix), notifiers, and rule duration
// fields all route through parseDurationExt/parseRuleDurationField.
func TestLoadFile_notificationFull(t *testing.T) {
	path := writeTempConfig(t, `
notification:
  enabled: true
  event_queue_depth: 128
  store_path: "/tmp/notif.db"
  alert_store_path: "/tmp/alerts.db"
  retry_interval: 30s
  gc_interval: 10m
  alert_idle_ttl: 2d
  resolved_alert_retention: 1d
  dlq_retention: 7d
  bbolt_compaction_enabled: true
  bbolt_compaction_interval: 12h
  notifiers:
    webhook:
      type: http
      url: "http://localhost:9000/hook"
      timeout: 3s
      auth_header: "Bearer x"
  rules:
    - name: high-errors
      match_severity: error
      resource_filter: "prod-.*"
      attribute_filters:
        region: "us-.*"
      cooldown: 1m
      max_retries: 5
      retry_backoff: 30s
      destination: webhook
      alert_window: 5m
      alert_threshold: 3
      alert_resolve_window: 15m
`)

	c := DefaultConfig()
	if err := c.LoadFile(path); err != nil {
		t.Fatalf("LoadFile: %v", err)
	}

	nc := c.Notification
	if nc == nil || !nc.Enabled {
		t.Fatal("notification not loaded")
	}
	if nc.EventQueueDepth != 128 || nc.StorePath != "/tmp/notif.db" || nc.AlertStorePath != "/tmp/alerts.db" {
		t.Errorf("scalars = %d/%q/%q", nc.EventQueueDepth, nc.StorePath, nc.AlertStorePath)
	}
	if nc.RetryInterval != 30*time.Second {
		t.Errorf("RetryInterval = %v, want 30s", nc.RetryInterval)
	}
	if nc.AlertIdleTTL != 2*24*time.Hour {
		t.Errorf("AlertIdleTTL = %v, want 2d", nc.AlertIdleTTL)
	}
	if nc.DLQRetention != 7*24*time.Hour {
		t.Errorf("DLQRetention = %v, want 7d", nc.DLQRetention)
	}
	if !nc.BboltCompactionEnabled || nc.BboltCompactionInterval != 12*time.Hour {
		t.Errorf("compaction = %v/%v", nc.BboltCompactionEnabled, nc.BboltCompactionInterval)
	}

	if len(nc.Notifiers) != 1 {
		t.Fatalf("notifiers = %d, want 1", len(nc.Notifiers))
	}
	nf := nc.Notifiers["webhook"]
	if nf.Type != "http" || nf.URL != "http://localhost:9000/hook" || nf.Timeout != 3*time.Second || nf.AuthHeader != "Bearer x" {
		t.Errorf("notifier = %+v", nf)
	}

	if len(nc.Rules) != 1 {
		t.Fatalf("rules = %d, want 1", len(nc.Rules))
	}
	rc := nc.Rules[0]
	if rc.Name != "high-errors" || rc.Cooldown != "1m0s" || rc.RetryBackoff != "30s" ||
		rc.AlertWindow != "5m0s" || rc.AlertResolveWindow != "15m0s" || rc.MaxRetries != 5 {
		t.Errorf("rule = %+v", rc)
	}
}

// TestLoadFile_notificationErrors verifies that invalid duration strings in
// the notification section surface as errors rather than being ignored.
func TestLoadFile_notificationErrors(t *testing.T) {
	cases := []struct {
		name    string
		yaml    string
		wantErr string
	}{
		{"retry_interval", "notification:\n  retry_interval: soon\n", "retry_interval"},
		{"notifier timeout", "notification:\n  notifiers:\n    h:\n      type: http\n      timeout: nope\n", "notifiers[h].timeout"},
		{"rule duration", "notification:\n  rules:\n    - name: r\n      cooldown: bad\n", "cooldown"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := writeTempConfig(t, tc.yaml)
			c := DefaultConfig()
			err := c.LoadFile(path)
			if err == nil {
				t.Fatal("LoadFile: expected error")
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("error = %q, want substring %q", err, tc.wantErr)
			}
		})
	}
}

// TestLoadFile_noNotificationSection keeps the existing behavior: a config
// without a notification section loads fine and leaves the defaults intact.
func TestLoadFile_noNotificationSection(t *testing.T) {
	path := writeTempConfig(t, "listen_address: \":9999\"\n")
	c := DefaultConfig()
	if err := c.LoadFile(path); err != nil {
		t.Fatalf("LoadFile: %v", err)
	}
	nc := c.Notification
	if nc == nil {
		t.Fatal("default Notification config must exist")
	}
	// Defaults must survive: no overrides from the file.
	if nc.Enabled || nc.RetryInterval != 30*time.Second {
		t.Errorf("defaults clobbered: %+v", nc)
	}
}

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
