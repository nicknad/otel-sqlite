package tasks

import (
	"context"
	"testing"
	"time"

	"codeberg.org/nicknad/otel-sqlite/internal/maintenance"
	"codeberg.org/nicknad/otel-sqlite/internal/storage/sqlite"
)

func TestVacuumTask_Name(t *testing.T) {
	task := NewVacuumTask(true, 7*24*time.Hour)
	if task.Name() != "vacuum" {
		t.Errorf("expected name 'vacuum', got %q", task.Name())
	}
}

func TestVacuumTask_Enabled(t *testing.T) {
	enabled := NewVacuumTask(true, 7*24*time.Hour)
	if !enabled.Enabled() {
		t.Error("expected task to be enabled")
	}

	disabled := NewVacuumTask(false, 7*24*time.Hour)
	if disabled.Enabled() {
		t.Error("expected task to be disabled")
	}
}

func TestVacuumTask_Due(t *testing.T) {
	task := NewVacuumTask(true, 1*time.Hour)

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

func TestVacuumTask_DueAfterInterval(t *testing.T) {
	task := NewVacuumTask(true, 1*time.Hour)
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

func TestVacuumTask_RunSubmitsCommand(t *testing.T) {
	submitter := &stubSubmitter2{}
	task := NewVacuumTask(true, 7*24*time.Hour)

	err := task.Run(context.Background(), submitter)
	if err != nil {
		t.Fatalf("Run() error: %v", err)
	}

	if submitter.submittedCount() != 1 {
		t.Errorf("expected 1 command submitted, got %d", submitter.submittedCount())
	}
}

func TestVacuumTask_RunSubmitsVacuumCommand(t *testing.T) {
	submitter := &stubSubmitter2{}
	task := NewVacuumTask(true, 7*24*time.Hour)

	if err := task.Run(context.Background(), submitter); err != nil {
		t.Fatalf("Run() error: %v", err)
	}

	cmd := submitter.submittedCommand(0)
	if cmd == nil {
		t.Fatal("expected a command to be submitted")
	}
	if _, ok := cmd.(*sqlite.VacuumCommand); !ok {
		t.Errorf("expected *sqlite.VacuumCommand, got %T", cmd)
	}
}

func TestVacuumTask_DisabledByDefault(t *testing.T) {
	// Vacuum is expensive and should be disabled by default.
	task := NewVacuumTask(false, 7*24*time.Hour)
	if task.Enabled() {
		t.Error("vacuum should be disabled by default")
	}
}

func TestVacuumTask_SatisfiesInterface(t *testing.T) {
	var task maintenance.MaintenanceTask = NewVacuumTask(true, 7*24*time.Hour)
	_ = task
}
