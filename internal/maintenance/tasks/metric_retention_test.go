package tasks

import (
	"context"
	"testing"
	"time"

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
