package tasks

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"codeberg.org/nicknad/otel-sqlite/internal/maintenance"
	"codeberg.org/nicknad/otel-sqlite/internal/storage"
	"codeberg.org/nicknad/otel-sqlite/internal/storage/sqlite"
)

func TestMetricRetentionTask_Name(t *testing.T) {
	task := NewMetricRetentionTask(true, 30*24*time.Hour, 24*time.Hour, 10000)
	if task.Name() != "metric_retention" {
		t.Errorf("expected name 'metric_retention', got %q", task.Name())
	}
}

func TestMetricRetentionTask_Enabled(t *testing.T) {
	enabled := NewMetricRetentionTask(true, 30*24*time.Hour, 24*time.Hour, 10000)
	if !enabled.Enabled() {
		t.Error("expected task to be enabled")
	}

	disabled := NewMetricRetentionTask(false, 30*24*time.Hour, 24*time.Hour, 10000)
	if disabled.Enabled() {
		t.Error("expected task to be disabled")
	}
}

func TestMetricRetentionTask_Due(t *testing.T) {
	task := NewMetricRetentionTask(true, 30*24*time.Hour, 1*time.Hour, 10000)

	// First call: should be due (lastRun is zero value).
	if !task.Due(time.Now()) {
		t.Error("expected task to be due on first call")
	}

	// Run the task to set lastRun.
	submitter := &stubSubmitter{}
	if err := task.Run(context.Background(), submitter); err != nil {
		t.Fatalf("Run() error: %v", err)
	}

	// Right after running, should not be due.
	if task.Due(time.Now()) {
		t.Error("expected task not to be due immediately after running")
	}
}

func TestMetricRetentionTask_DueAfterInterval(t *testing.T) {
	task := NewMetricRetentionTask(true, 30*24*time.Hour, 1*time.Hour, 10000)
	submitter := &stubSubmitter{}
	if err := task.Run(context.Background(), submitter); err != nil {
		t.Fatalf("Run() error: %v", err)
	}

	if !task.Due(time.Now().Add(2 * time.Hour)) {
		t.Error("expected task to be due after the cleanup interval")
	}
}

func TestMetricRetentionTask_RunSubmitsPurgeCommand(t *testing.T) {
	submitter := &stubSubmitter{}
	task := NewMetricRetentionTask(true, 30*24*time.Hour, 24*time.Hour, 10000)

	if err := task.Run(context.Background(), submitter); err != nil {
		t.Fatalf("Run() error: %v", err)
	}

	if submitter.submittedCount() != 1 {
		t.Fatalf("expected 1 command submitted, got %d", submitter.submittedCount())
	}
	cmd := submitter.commands[0]
	if _, ok := cmd.(*sqlite.PurgeMetricDataPointsCommand); !ok {
		t.Errorf("expected *sqlite.PurgeMetricDataPointsCommand, got %T", cmd)
	}
}

func TestMetricRetentionTask_SatisfiesInterface(t *testing.T) {
	var _ interface {
		Name() string
		Enabled() bool
	} = NewMetricRetentionTask(true, time.Hour, time.Hour, 10)
}

// TestMetricRetentionTask_WithSubmitter verifies the per-task submitter
// override: nil by default (worker default), settable via WithSubmitter and
// reported through Submitter() for the worker's SubmitterTask routing.
func TestMetricRetentionTask_WithSubmitter(t *testing.T) {
	defaultTask := NewMetricRetentionTask(true, time.Hour, time.Hour, 10)
	if defaultTask.Submitter() != nil {
		t.Error("expected nil default submitter")
	}

	// Ensure it satisfies maintenance.SubmitterTask.
	var _ maintenance.SubmitterTask = defaultTask

	custom := &stubSubmitter{}
	returned := defaultTask.WithSubmitter(custom)
	if returned != defaultTask {
		t.Error("WithSubmitter should return the receiver for chaining")
	}
	if defaultTask.Submitter() != custom {
		t.Error("expected WithSubmitter to set the task submitter")
	}
}

// TestMetricRetentionTask_WorkerRoutesPurgeToMetricsWriter is the retention
// routing test from plan §8.3: with two writers, the metric-retention task
// must submit PurgeMetricDataPointsCommand to the metrics writer (via
// WithSubmitter) so it purges the metrics DB only, while log retention keeps
// targeting the log DB through the worker's default submitter.
func TestMetricRetentionTask_WorkerRoutesPurgeToMetricsWriter(t *testing.T) {
	dir := t.TempDir()
	logPath := dir + "/logs.db"
	metricsPath := dir + "/metrics.db"

	// Create both DBs (runs the full 001–007 migration set on each), then
	// stop the writers so the seed rows can be inserted directly.
	createWriter := func(path string) {
		q := storage.NewCommandQueue(10)
		w, err := sqlite.NewWriter(q, &sqlite.WriterConfig{
			Path: path, BatchSize: 10, FlushInterval: 100 * time.Millisecond, WALMode: true,
		})
		if err != nil {
			t.Fatalf("NewWriter(%s): %v", path, err)
		}
		// Start then Stop so the run loop closes the DB handle.
		w.Start(context.Background())
		w.Stop()
		w.Wait()
	}
	createWriter(logPath)
	createWriter(metricsPath)

	// Seed: an expired metric data point in the metrics DB and a stale log
	// row in the log DB (both far older than the retention window).
	old := time.Now().Add(-48 * time.Hour).UnixNano()
	insertMetricDataPoint(t, metricsPath, old)
	insertLogEvent(t, logPath, old)

	// Re-open both writers for command execution.
	logWriter := startWriter(t, logPath)
	metricWriter := startWriter(t, metricsPath)
	t.Cleanup(func() {
		logWriter.Stop()
		logWriter.Wait()
		metricWriter.Stop()
		metricWriter.Wait()
	})

	cfg := maintenance.DefaultConfig()
	cfg.RetentionKeepLogs = 24 * time.Hour
	cfg.RetentionKeepMetrics = 24 * time.Hour

	// Worker default submitter is the LOG writer. The metric-retention task
	// overrides it to the METRICS writer — exactly what the collector wires
	// when metrics_sqlite_path is set.
	w := maintenance.NewWorker(cfg, logWriter, nil)
	w.Register(NewMetricRetentionTask(
		cfg.MetricRetentionEnabled, cfg.RetentionKeepMetrics,
		cfg.RetentionCleanupInterval, cfg.RetentionDeleteBatchSize,
	).WithSubmitter(metricWriter))
	w.Register(NewRetentionTask(
		cfg.RetentionEnabled, cfg.RetentionKeepLogs,
		cfg.RetentionCleanupInterval, cfg.RetentionDeleteBatchSize,
	))

	// Start the worker: it evaluates all registered tasks immediately at
	// startup (both are due on first run).
	w.Start(context.Background())
	defer w.Stop()

	// Poll until both writers have executed their purge commands.
	deadline := time.Now().Add(5 * time.Second)
	for {
		mp := taskDBCount(t, metricsPath, "metric_data_point")
		le := taskDBCount(t, logPath, "log_event")
		if mp == 0 && le == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for retention: metric_data_point=%d log_event=%d", mp, le)
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Stop the worker before the writers so no task submits to a stopped
	// writer; t.Cleanup then stops both writers.
	w.Stop()
	logWriter.Stop()
	logWriter.Wait()
	metricWriter.Stop()
	metricWriter.Wait()

	// Final assertions on the closed DBs.
	assertTaskDBCount(t, metricsPath, "metric_data_point", 0)
	assertTaskDBCount(t, logPath, "log_event", 0)
}

// insertMetricDataPoint seeds a full resource→scope→metric→series→point
// chain with one data point at ts (nanoseconds) into the DB at path.
func insertMetricDataPoint(t *testing.T, path string, ts int64) {
	t.Helper()
	db := openTaskDB(t, path)
	defer func() { _ = db.Close() }()
	execTask(t, db, `INSERT INTO log_resource (id, service_name, attributes_json)
		VALUES ('r1', 'ret-svc', '{}')`)
	execTask(t, db, `INSERT INTO scope (id, resource_id, name, version, schema_url)
		VALUES ('s1', 'r1', 'scope', NULL, NULL)`)
	execTask(t, db, `INSERT INTO metric (id, scope_id, name, type, is_monotonic, aggregation_temporality)
		VALUES ('m1', 's1', 'ret.metric', 1, 0, 0)`)
	execTask(t, db, `INSERT INTO metric_series (id, metric_id, attributes_json)
		VALUES ('ms1', 'm1', '{}')`)
	execTask(t, db, `INSERT INTO metric_data_point (series_id, timestamp_ns, double_value)
		VALUES ('ms1', ?, 1.0)`, ts)
}

// insertLogEvent seeds one log_event row at ts (nanoseconds) into the DB at path.
func insertLogEvent(t *testing.T, path string, ts int64) {
	t.Helper()
	db := openTaskDB(t, path)
	defer func() { _ = db.Close() }()
	execTask(t, db, `INSERT INTO log_resource (id, service_name, attributes_json)
		VALUES ('lr1', 'ret-svc', '{}')`)
	execTask(t, db, `INSERT INTO log_event (resource_id, timestamp_ns, observed_timestamp_ns,
		severity_number, severity_text, body, flags, dropped_attributes_count, attributes_json)
		VALUES ('lr1', ?, ?, 9, 'ERROR', 'old log', 0, 0, '{}')`, ts, ts)
}

func openTaskDB(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	return db
}

func execTask(t *testing.T, db *sql.DB, query string, args ...any) {
	t.Helper()
	if _, err := db.Exec(query, args...); err != nil {
		t.Fatalf("exec %q: %v", query, err)
	}
}

// startWriter opens a writer on path and starts it.
func startWriter(t *testing.T, path string) *sqlite.Writer {
	t.Helper()
	q := storage.NewCommandQueue(10)
	w, err := sqlite.NewWriter(q, &sqlite.WriterConfig{
		Path: path, BatchSize: 10, FlushInterval: 100 * time.Millisecond, WALMode: true,
	})
	if err != nil {
		t.Fatalf("NewWriter(%s): %v", path, err)
	}
	w.Start(context.Background())
	return w
}

// taskDBCount returns COUNT(*) on table in the DB at path, or -1 on error.
func taskDBCount(t *testing.T, path, table string) int {
	t.Helper()
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return -1
	}
	defer func() { _ = db.Close() }()
	var n int
	if err := db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&n); err != nil {
		return -1
	}
	return n
}

// assertTaskDBCount asserts COUNT(*) on table in the DB at path.
func assertTaskDBCount(t *testing.T, path, table string, want int) {
	t.Helper()
	db := openTaskDB(t, path)
	defer func() { _ = db.Close() }()
	var n int
	if err := db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&n); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	if n != want {
		t.Errorf("%s count in %s = %d, want %d", table, path, n, want)
	}
}
