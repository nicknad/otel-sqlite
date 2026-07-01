package tasks

import (
	"context"
	"sync"
	"testing"
	"time"

	"codeberg.org/nicknad/otel-sqlite/internal/maintenance"
	"codeberg.org/nicknad/otel-sqlite/internal/storage"
)

// stubSubmitter is a test double for CommandSubmitter.
type stubSubmitter struct {
	mu       sync.Mutex
	commands []storage.Command
}

func (s *stubSubmitter) Submit(ctx context.Context, cmd storage.Command) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.commands = append(s.commands, cmd)
	return nil
}

func (s *stubSubmitter) submittedCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.commands)
}

func TestRetentionTask_Name(t *testing.T) {
	task := NewRetentionTask(true, 30*24*time.Hour, 24*time.Hour, 10000)
	if task.Name() != "retention" {
		t.Errorf("expected name 'retention', got %q", task.Name())
	}
}

func TestRetentionTask_Enabled(t *testing.T) {
	enabled := NewRetentionTask(true, 30*24*time.Hour, 24*time.Hour, 10000)
	if !enabled.Enabled() {
		t.Error("expected task to be enabled")
	}

	disabled := NewRetentionTask(false, 30*24*time.Hour, 24*time.Hour, 10000)
	if disabled.Enabled() {
		t.Error("expected task to be disabled")
	}
}

func TestRetentionTask_Due(t *testing.T) {
	task := NewRetentionTask(true, 30*24*time.Hour, 1*time.Hour, 10000)

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

func TestRetentionTask_DueAfterInterval(t *testing.T) {
	task := NewRetentionTask(true, 30*24*time.Hour, 1*time.Hour, 10000)
	now := time.Now()

	// First due check triggers lastRun update via Run().
	submitter := &stubSubmitter{}
	if err := task.Run(context.Background(), submitter); err != nil {
		t.Fatalf("Run() error: %v", err)
	}

	// Immediately: not due.
	if task.Due(now) {
		t.Error("expected not due immediately after run")
	}

	// After the cleanup interval: due.
	future := now.Add(2 * time.Hour)
	if !task.Due(future) {
		t.Error("expected due after cleanup interval")
	}
}

func TestRetentionTask_RunSubmitsCommand(t *testing.T) {
	submitter := &stubSubmitter{}
	task := NewRetentionTask(true, 30*24*time.Hour, 24*time.Hour, 10000)

	err := task.Run(context.Background(), submitter)
	if err != nil {
		t.Fatalf("Run() error: %v", err)
	}

	if submitter.submittedCount() != 1 {
		t.Errorf("expected 1 command submitted, got %d", submitter.submittedCount())
	}
}

func TestRetentionTask_SatisfiesInterface(t *testing.T) {
	var task maintenance.MaintenanceTask = NewRetentionTask(true, 30*24*time.Hour, 24*time.Hour, 10000)
	_ = task // compile-time assertion
}
