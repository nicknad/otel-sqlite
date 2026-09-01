package tasks

import (
	"context"
	"sync"
	"testing"
	"time"

	"codeberg.org/nicknad/otel-sqlite/internal/maintenance"
	"codeberg.org/nicknad/otel-sqlite/internal/storage"
	"codeberg.org/nicknad/otel-sqlite/internal/storage/sqlite"
)

func TestCheckpointTask_Name(t *testing.T) {
	task := NewCheckpointTask(true, sqlite.CheckpointPassive, 24*time.Hour)
	if task.Name() != "checkpoint" {
		t.Errorf("expected name 'checkpoint', got %q", task.Name())
	}
}

func TestCheckpointTask_Enabled(t *testing.T) {
	enabled := NewCheckpointTask(true, sqlite.CheckpointPassive, 24*time.Hour)
	if !enabled.Enabled() {
		t.Error("expected task to be enabled")
	}

	disabled := NewCheckpointTask(false, sqlite.CheckpointPassive, 24*time.Hour)
	if disabled.Enabled() {
		t.Error("expected task to be disabled")
	}
}

func TestCheckpointTask_Due(t *testing.T) {
	task := NewCheckpointTask(true, sqlite.CheckpointPassive, 1*time.Hour)

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

func TestCheckpointTask_DueAfterInterval(t *testing.T) {
	task := NewCheckpointTask(true, sqlite.CheckpointPassive, 1*time.Hour)
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

func TestCheckpointTask_RunSubmitsCommand(t *testing.T) {
	submitter := &stubSubmitter2{}
	task := NewCheckpointTask(true, sqlite.CheckpointPassive, 24*time.Hour)

	err := task.Run(context.Background(), submitter)
	if err != nil {
		t.Fatalf("Run() error: %v", err)
	}

	if submitter.submittedCount() != 1 {
		t.Errorf("expected 1 command submitted, got %d", submitter.submittedCount())
	}
}

func TestCheckpointTask_DefaultMode(t *testing.T) {
	task := NewCheckpointTask(true, "", 24*time.Hour)
	// The empty string should default to PASSIVE.
	// We can verify by running and checking the submitted command type.
	submitter := &stubSubmitter2{}
	if err := task.Run(context.Background(), submitter); err != nil {
		t.Fatalf("Run() error: %v", err)
	}

	cmd := submitter.submittedCommand(0)
	if cmd == nil {
		t.Fatal("expected a command to be submitted")
	}
	// Check it's a *sqlite.CheckpointCommand.
	if _, ok := cmd.(*sqlite.CheckpointCommand); !ok {
		t.Errorf("expected *sqlite.CheckpointCommand, got %T", cmd)
	}
}

func TestCheckpointTask_ModeOptions(t *testing.T) {
	modes := []sqlite.CheckpointMode{
		sqlite.CheckpointPassive,
		sqlite.CheckpointFull,
		sqlite.CheckpointRestart,
		sqlite.CheckpointTruncate,
	}

	for _, mode := range modes {
		t.Run(string(mode), func(t *testing.T) {
			task := NewCheckpointTask(true, mode, 24*time.Hour)
			submitter := &stubSubmitter2{}
			if err := task.Run(context.Background(), submitter); err != nil {
				t.Fatalf("Run() error: %v", err)
			}
			if submitter.submittedCount() != 1 {
				t.Errorf("expected 1 command, got %d", submitter.submittedCount())
			}
		})
	}
}

func TestCheckpointTask_SatisfiesInterface(t *testing.T) {
	var task maintenance.MaintenanceTask = NewCheckpointTask(true, sqlite.CheckpointPassive, 24*time.Hour)
	_ = task
}

// stubSubmitter2 is a test double for CommandSubmitter used in this file.
type stubSubmitter2 struct {
	mu       sync.Mutex
	commands []storage.Command
}

func (s *stubSubmitter2) Submit(ctx context.Context, cmd storage.Command) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.commands = append(s.commands, cmd)
	return nil
}

func (s *stubSubmitter2) submittedCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.commands)
}

func (s *stubSubmitter2) submittedCommand(index int) storage.Command {
	s.mu.Lock()
	defer s.mu.Unlock()
	if index < 0 || index >= len(s.commands) {
		return nil
	}
	return s.commands[index]
}
