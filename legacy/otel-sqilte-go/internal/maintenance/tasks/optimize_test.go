package tasks

import (
	"context"
	"testing"
	"time"

	"codeberg.org/nicknad/otel-sqlite/internal/maintenance"
	"codeberg.org/nicknad/otel-sqlite/internal/storage/sqlite"
)

func TestOptimizeTask_Name(t *testing.T) {
	task := NewOptimizeTask(true, 24*time.Hour)
	if task.Name() != "optimize" {
		t.Errorf("expected name 'optimize', got %q", task.Name())
	}
}

func TestOptimizeTask_Enabled(t *testing.T) {
	enabled := NewOptimizeTask(true, 24*time.Hour)
	if !enabled.Enabled() {
		t.Error("expected task to be enabled")
	}

	disabled := NewOptimizeTask(false, 24*time.Hour)
	if disabled.Enabled() {
		t.Error("expected task to be disabled")
	}
}

func TestOptimizeTask_Due(t *testing.T) {
	task := NewOptimizeTask(true, 1*time.Hour)

	// First call: should be due (lastRun is zero value).
	if !task.Due(time.Now()) {
		t.Error("expected task to be due on first call")
	}

	// Run the task to set lastRun.
	submitter := &stubSubmitter2{}
	if err := task.Run(context.Background(), submitter); err != nil {
		t.Fatalf("Run() error: %v", err)
	}

	// Right after running, should not be due.
	if task.Due(time.Now()) {
		t.Error("expected task not to be due immediately after running")
	}
}

func TestOptimizeTask_DueAfterInterval(t *testing.T) {
	task := NewOptimizeTask(true, 1*time.Hour)
	now := time.Now()

	submitter := &stubSubmitter2{}
	if err := task.Run(context.Background(), submitter); err != nil {
		t.Fatalf("Run() error: %v", err)
	}

	// Immediately: not due.
	if task.Due(now) {
		t.Error("expected not due immediately after run")
	}

	// After the interval: due.
	future := now.Add(2 * time.Hour)
	if !task.Due(future) {
		t.Error("expected due after interval")
	}
}

func TestOptimizeTask_RunSubmitsCommand(t *testing.T) {
	submitter := &stubSubmitter2{}
	task := NewOptimizeTask(true, 24*time.Hour)

	err := task.Run(context.Background(), submitter)
	if err != nil {
		t.Fatalf("Run() error: %v", err)
	}

	if submitter.submittedCount() != 1 {
		t.Errorf("expected 1 command submitted, got %d", submitter.submittedCount())
	}
}

func TestOptimizeTask_RunSubmitsOptimizeCommand(t *testing.T) {
	submitter := &stubSubmitter2{}
	task := NewOptimizeTask(true, 24*time.Hour)

	if err := task.Run(context.Background(), submitter); err != nil {
		t.Fatalf("Run() error: %v", err)
	}

	cmd := submitter.submittedCommand(0)
	if cmd == nil {
		t.Fatal("expected a command to be submitted")
	}
	if _, ok := cmd.(*sqlite.OptimizeCommand); !ok {
		t.Errorf("expected *sqlite.OptimizeCommand, got %T", cmd)
	}
}

func TestOptimizeTask_SatisfiesInterface(t *testing.T) {
	var task maintenance.MaintenanceTask = NewOptimizeTask(true, 24*time.Hour)
	_ = task
}
