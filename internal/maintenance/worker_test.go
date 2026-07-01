package maintenance

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"testing"
	"time"

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

// stubCommand implements storage.Command for testing.
type stubCommand struct {
	name string
}

func (c *stubCommand) Execute(ctx context.Context, tx *sql.Tx) error {
	return nil
}

// Ensure stubCommand satisfies storage.Command (compile-time check from storage).
var _ storage.Command = (*stubCommand)(nil)

// dummyTask is a simple MaintenanceTask for testing the worker.
type dummyTask struct {
	name    string
	enabled bool
	due     bool
	runFn   func(ctx context.Context, submitter CommandSubmitter) error
}

func (t *dummyTask) Name() string           { return t.name }
func (t *dummyTask) Enabled() bool          { return t.enabled }
func (t *dummyTask) Due(now time.Time) bool { return t.due }
func (t *dummyTask) Run(ctx context.Context, submitter CommandSubmitter) error {
	if t.runFn != nil {
		return t.runFn(ctx, submitter)
	}
	return nil
}

func TestWorker_RunsEnabledDueTasks(t *testing.T) {
	submitter := &stubSubmitter{}

	var runCalled bool
	task := &dummyTask{
		name:    "test-task",
		enabled: true,
		due:     true,
		runFn: func(ctx context.Context, s CommandSubmitter) error {
			runCalled = true
			return s.Submit(ctx, &stubCommand{name: "test"})
		},
	}

	cfg := DefaultConfig()
	cfg.CheckInterval = 1 * time.Hour // doesn't matter for manual eval

	w := NewWorker(cfg, submitter, nil)
	w.Register(task)

	// Don't start the worker loop; directly call evaluateAndRun.
	w.evaluateAndRun(time.Now())

	if !runCalled {
		t.Error("expected task.Run to be called")
	}
	if submitter.submittedCount() != 1 {
		t.Errorf("expected 1 command submitted, got %d", submitter.submittedCount())
	}
}

func TestWorker_SkipsDisabledTasks(t *testing.T) {
	task := &dummyTask{
		name:    "disabled-task",
		enabled: false,
		due:     true,
	}

	cfg := DefaultConfig()
	w := NewWorker(cfg, &stubSubmitter{}, nil)
	w.Register(task)

	var runCalled bool
	task.runFn = func(ctx context.Context, s CommandSubmitter) error {
		runCalled = true
		return nil
	}

	w.evaluateAndRun(time.Now())

	if runCalled {
		t.Error("disabled task should not have been run")
	}
}

func TestWorker_SkipsNotDueTasks(t *testing.T) {
	task := &dummyTask{
		name:    "not-due-task",
		enabled: true,
		due:     false,
	}

	cfg := DefaultConfig()
	w := NewWorker(cfg, &stubSubmitter{}, nil)
	w.Register(task)

	var runCalled bool
	task.runFn = func(ctx context.Context, s CommandSubmitter) error {
		runCalled = true
		return nil
	}

	w.evaluateAndRun(time.Now())

	if runCalled {
		t.Error("not-due task should not have been run")
	}
}

func TestWorker_ContinuesAfterFailure(t *testing.T) {
	submitter := &stubSubmitter{}

	failingTask := &dummyTask{
		name:    "failing-task",
		enabled: true,
		due:     true,
		runFn: func(ctx context.Context, s CommandSubmitter) error {
			return errors.New("task failure")
		},
	}

	successTask := &dummyTask{
		name:    "success-task",
		enabled: true,
		due:     true,
		runFn: func(ctx context.Context, s CommandSubmitter) error {
			return s.Submit(ctx, &stubCommand{name: "ok"})
		},
	}

	cfg := DefaultConfig()
	w := NewWorker(cfg, submitter, nil)
	w.Register(failingTask)
	w.Register(successTask)

	w.evaluateAndRun(time.Now())

	if submitter.submittedCount() != 1 {
		t.Errorf("expected 1 command submitted (from success task), got %d", submitter.submittedCount())
	}
}

func TestWorker_DisabledMaintenanceDoesNotRun(t *testing.T) {
	cfg := DefaultConfig()
	cfg.MaintenanceEnabled = false

	var runCalled bool
	task := &dummyTask{
		name:    "should-not-run",
		enabled: true,
		due:     true,
		runFn: func(ctx context.Context, s CommandSubmitter) error {
			runCalled = true
			return nil
		},
	}

	w := NewWorker(cfg, &stubSubmitter{}, nil)
	w.Register(task)

	// evaluateAndRun checks enabled at the worker level.
	w.evaluateAndRun(time.Now())

	if runCalled {
		t.Error("task should not run when maintenance is disabled")
	}
}

func TestWorker_RegisterAddsTask(t *testing.T) {
	cfg := DefaultConfig()
	w := NewWorker(cfg, &stubSubmitter{}, nil)

	if w.TaskCount() != 0 {
		t.Errorf("expected 0 tasks, got %d", w.TaskCount())
	}

	w.Register(&dummyTask{name: "t1", enabled: true})
	if w.TaskCount() != 1 {
		t.Errorf("expected 1 task, got %d", w.TaskCount())
	}

	w.Register(&dummyTask{name: "t2", enabled: false})
	if w.TaskCount() != 2 {
		t.Errorf("expected 2 tasks, got %d", w.TaskCount())
	}
}

// TestWorker_Extensibility verifies that adding a new task type requires
// only registration — no scheduler or worker changes.
func TestWorker_Extensibility(t *testing.T) {
	// A completely new task type: only registration required.
	task := &dummyTask{
		name:    "new-ext-task",
		enabled: true,
		due:     true,
		runFn: func(ctx context.Context, s CommandSubmitter) error {
			return s.Submit(ctx, &stubCommand{name: "from-new-task"})
		},
	}

	submitter := &stubSubmitter{}
	cfg := DefaultConfig()
	w := NewWorker(cfg, submitter, nil)

	// Only registration required — no worker/scheduler modifications.
	w.Register(task)
	w.evaluateAndRun(time.Now())

	if submitter.submittedCount() != 1 {
		t.Errorf("new task type should work without scheduler changes; got %d commands", submitter.submittedCount())
	}
}

func TestWorker_Metrics(t *testing.T) {
	metrics := NewMetrics()
	submitter := &stubSubmitter{}

	task := &dummyTask{
		name:    "metric-task",
		enabled: true,
		due:     true,
		runFn: func(ctx context.Context, s CommandSubmitter) error {
			return nil
		},
	}

	cfg := DefaultConfig()
	w := NewWorker(cfg, submitter, metrics)
	w.Register(task)

	// Run twice.
	w.evaluateAndRun(time.Now())
	w.evaluateAndRun(time.Now())

	// Metrics are registered with prometheus auto-register; we just verify no panic.
	if w.TaskCount() != 1 {
		t.Errorf("expected 1 task, got %d", w.TaskCount())
	}
}

func TestWorker_NilMetricsSafe(t *testing.T) {
	cfg := DefaultConfig()
	w := NewWorker(cfg, &stubSubmitter{}, nil)

	task := &dummyTask{
		name:    "nil-metrics-task",
		enabled: true,
		due:     true,
	}

	w.Register(task)

	// Should not panic with nil metrics.
	w.evaluateAndRun(time.Now())
}

func TestWorker_StartStop(t *testing.T) {
	cfg := DefaultConfig()
	cfg.CheckInterval = 50 * time.Millisecond

	w := NewWorker(cfg, &stubSubmitter{}, nil)
	w.Register(&dummyTask{
		name:    "start-stop-task",
		enabled: true,
		due:     true,
	})

	ctx, cancel := context.WithCancel(context.Background())
	w.Start(ctx)

	// Let it run at least one cycle.
	time.Sleep(150 * time.Millisecond)
	cancel()
	w.Stop()

	// Worker should stop cleanly without panic.
}
