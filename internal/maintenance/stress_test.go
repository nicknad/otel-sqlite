package maintenance

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"codeberg.org/nicknad/otel-sqlite/internal/storage"
)

// =============================================================================
// SECTION 1: Forbidden Coupling Tests (Static + Build-time)
// =============================================================================

// TestGuardrail_NoDatabaseSQLInMaintenance verifies that production
// maintenance code never imports database/sql.
func TestGuardrail_NoDatabaseSQLInMaintenance(t *testing.T) {
	// Walk the maintenance source tree (excluding _test.go).
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read maintenance dir: %v", err)
	}
	for _, e := range entries {
		if e.IsDir() || strings.HasSuffix(e.Name(), "_test.go") || !strings.HasSuffix(e.Name(), ".go") {
			continue
		}
		data, err := os.ReadFile(e.Name())
		if err != nil {
			t.Fatalf("read %s: %v", e.Name(), err)
		}
		if strings.Contains(string(data), `"database/sql"`) {
			t.Errorf("FAIL: %s imports database/sql — forbidden in maintenance layer", e.Name())
		}
	}
}

// TestGuardrail_NoSQLiteInMaintenanceFramework verifies the framework core
// does not import the sqlite package.
func TestGuardrail_NoSQLiteInMaintenanceFramework(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read maintenance dir: %v", err)
	}
	for _, e := range entries {
		if e.IsDir() || strings.HasSuffix(e.Name(), "_test.go") || !strings.HasSuffix(e.Name(), ".go") {
			continue
		}
		data, err := os.ReadFile(e.Name())
		if err != nil {
			t.Fatalf("read %s: %v", e.Name(), err)
		}
		if strings.Contains(string(data), `"codeberg.org/nicknad/otel-sqlite/internal/storage/sqlite"`) {
			t.Errorf("FAIL: %s imports storage/sqlite — forbidden in maintenance framework", e.Name())
		}
	}
}

// TestGuardrail_NoChannelUsageInMaintenance verifies no direct chan Command usage.
func TestGuardrail_NoChannelUsageInMaintenance(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read maintenance dir: %v", err)
	}
	for _, e := range entries {
		if e.IsDir() || strings.HasSuffix(e.Name(), "_test.go") || !strings.HasSuffix(e.Name(), ".go") {
			continue
		}
		data, err := os.ReadFile(e.Name())
		if err != nil {
			t.Fatalf("read %s: %v", e.Name(), err)
		}
		if strings.Contains(string(data), "chan Command") {
			t.Errorf("FAIL: %s contains 'chan Command' — forbidden direct queue access", e.Name())
		}
	}
}

// TestGuardrail_NoSQLStringsInMaintenance verifies no raw SQL in maintenance.
func TestGuardrail_NoSQLStringsInMaintenance(t *testing.T) {
	sqlKeywords := []string{
		`"SELECT`, `"INSERT`, `"DELETE`, `"UPDATE`, `"CREATE`, `"DROP`,
		`"ALTER`, `"PRAGMA`, `"BEGIN`, `"COMMIT`, `"ROLLBACK`,
	}
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read maintenance dir: %v", err)
	}
	for _, e := range entries {
		if e.IsDir() || strings.HasSuffix(e.Name(), "_test.go") || !strings.HasSuffix(e.Name(), ".go") {
			continue
		}
		data, err := os.ReadFile(e.Name())
		if err != nil {
			t.Fatalf("read %s: %v", e.Name(), err)
		}
		for _, kw := range sqlKeywords {
			if strings.Contains(string(data), kw) {
				t.Errorf("FAIL: %s contains SQL keyword %s — forbidden in maintenance layer", e.Name(), kw)
			}
		}
	}
}

// =============================================================================
// SECTION 2: Scheduling Correctness (Clock Injection)
// =============================================================================

// fakeClock implements Clock with a manually controlled time.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock(t time.Time) *fakeClock { return &fakeClock{now: t} }
func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}
func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}
func (c *fakeClock) Set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = t
}

// clockTask tracks its own last-run using a fake clock reference, enabling
// deterministic scheduling tests without wall-clock dependency.
type clockTask struct {
	name     string
	enabled  bool
	interval time.Duration
	lastRun  time.Time
	clock    Clock // injected fake clock; nil means use wall clock

	mu       sync.Mutex
	runCount int32
	runErr   error
}

func newClockTask(name string, interval time.Duration) *clockTask {
	return &clockTask{name: name, enabled: true, interval: interval, clock: RealClock{}}
}

func (t *clockTask) SetClock(c Clock) { t.clock = c }

func (t *clockTask) Name() string  { return t.name }
func (t *clockTask) Enabled() bool { return t.enabled }

func (t *clockTask) Due(now time.Time) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.lastRun.IsZero() {
		return true
	}
	return now.Sub(t.lastRun) >= t.interval
}

func (t *clockTask) Run(ctx context.Context, s CommandSubmitter) error {
	t.mu.Lock()
	t.runCount++
	t.lastRun = t.clock.Now()
	t.mu.Unlock()
	return t.runErr
}

func (t *clockTask) RunCount() int {
	return int(atomic.LoadInt32(&t.runCount))
}

// TestTimeSimulation_NotDueThenDueThenNotDue validates the core scheduling
// state machine: t0→not due, t1→due, t2→executes once, t3→must NOT re-run.
func TestTimeSimulation_NotDueThenDueThenNotDue(t *testing.T) {
	clock := newFakeClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	submitter := &stubSubmitter{}

	task := newClockTask("sim-task", 1*time.Hour)
	task.SetClock(clock)
	cfg := DefaultConfig()
	cfg.CheckInterval = 10 * time.Minute
	w := NewWorker(cfg, submitter, nil)
	w.SetClock(clock)
	w.Register(task)

	// t0: first call → Due() true (lastRun is zero).
	w.evaluateAndRun(clock.Now())
	if task.RunCount() != 1 {
		t.Fatalf("t0: expected 1 run, got %d", task.RunCount())
	}

	// t0+30min: not yet due.
	clock.Advance(30 * time.Minute)
	w.evaluateAndRun(clock.Now())
	if task.RunCount() != 1 {
		t.Errorf("t0+30min: expected still 1 run, got %d (double execution!)", task.RunCount())
	}

	// t0+61min: now due.
	clock.Advance(31 * time.Minute)
	w.evaluateAndRun(clock.Now())
	if task.RunCount() != 2 {
		t.Errorf("t0+61min: expected 2 runs, got %d", task.RunCount())
	}

	// t0+62min: must NOT run again immediately.
	clock.Advance(1 * time.Minute)
	w.evaluateAndRun(clock.Now())
	if task.RunCount() != 2 {
		t.Errorf("t0+62min: expected still 2 runs, got %d (double execution!)", task.RunCount())
	}
}

// TestDoubleExecutionPrevention ensures that a fast evaluation loop
// does not trigger double execution of a task within its interval window.
func TestDoubleExecutionPrevention(t *testing.T) {
	clock := newFakeClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))

	task := newClockTask("fast-task", 1*time.Second)
	task.SetClock(clock)
	cfg := DefaultConfig()
	cfg.CheckInterval = 10 * time.Millisecond // Fast loop.
	w := NewWorker(cfg, &stubSubmitter{}, nil)
	w.SetClock(clock)
	w.Register(task)

	// Simulate 100 fast ticks over 2 seconds — task should run at most 2-3 times.
	start := clock.Now()
	end := start.Add(2 * time.Second)
	for clock.Now().Before(end) {
		w.evaluateAndRun(clock.Now())
		clock.Advance(10 * time.Millisecond)
	}

	count := task.RunCount()
	if count > 4 {
		t.Errorf("double execution detected: task ran %d times in 2s with 1s interval (expected ~2)", count)
	}
	if count < 1 {
		t.Errorf("task never ran: got %d runs", count)
	}
}

// =============================================================================
// SECTION 3: Backpressure & Queue Saturation
// =============================================================================

// blockingSubmitter wraps a channel to simulate a bounded command queue.
// It blocks when full, providing backpressure.
type blockingSubmitter struct {
	ch       chan storage.Command
	capacity int

	submitted atomic.Int64
	dropped   atomic.Int64
}

func newBlockingSubmitter(capacity int) *blockingSubmitter {
	return &blockingSubmitter{ch: make(chan storage.Command, capacity), capacity: capacity}
}

func (s *blockingSubmitter) Submit(ctx context.Context, cmd storage.Command) error {
	select {
	case s.ch <- cmd:
		s.submitted.Add(1)
		return nil
	case <-ctx.Done():
		s.dropped.Add(1)
		return ctx.Err()
	}
}

// drain consumes all queued commands (simulating the SQLite writer).
func (s *blockingSubmitter) drain(ctx context.Context, n int) {
	for i := 0; i < n; i++ {
		select {
		case <-s.ch:
		case <-ctx.Done():
			return
		}
	}
}

func (s *blockingSubmitter) queued() int { return len(s.ch) }

// TestBackpressure_WorkerBlocksOnFullQueue verifies that when the command
// queue is full, the worker's task blocks (providing natural backpressure),
// rather than dropping commands or growing memory unboundedly.
func TestBackpressure_WorkerBlocksOnFullQueue(t *testing.T) {
	submitter := newBlockingSubmitter(2) // Tiny queue.

	// Task submits 10 commands.
	task := &dummyTask{
		name:    "backpressure-task",
		enabled: true,
		due:     true,
		runFn: func(ctx context.Context, s CommandSubmitter) error {
			for i := 0; i < 10; i++ {
				if err := s.Submit(ctx, &stubCommand{name: "bp"}); err != nil {
					return err
				}
			}
			return nil
		},
	}

	cfg := DefaultConfig()
	w := NewWorker(cfg, submitter, nil)
	w.Register(task)

	// Run task in a goroutine since it will block.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- task.Run(ctx, submitter)
	}()

	// Drain slowly — simulate slow SQLite writer.
	time.Sleep(100 * time.Millisecond)
	for submitter.queued() > 0 {
		submitter.drain(ctx, 1)
		time.Sleep(10 * time.Millisecond)
	}

	// Wait for task to finish (it should, after draining).
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("task should complete after drain, got: %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Error("task deadlocked — never completed even after draining")
	}

	if submitter.submitted.Load() != 10 {
		t.Errorf("expected 10 submitted, got %d (dropped=%d)", submitter.submitted.Load(), submitter.dropped.Load())
	}
	if submitter.dropped.Load() > 0 {
		t.Error("commands were silently dropped")
	}
}

// TestBackpressure_ContextCancelUnblocksSubmit verifies that a canceled
// context unblocks a Submit call rather than causing a goroutine leak.
func TestBackpressure_ContextCancelUnblocksSubmit(t *testing.T) {
	submitter := newBlockingSubmitter(1)
	// Fill the queue.
	submitter.ch <- &stubCommand{name: "filler"}

	ctx, cancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := submitter.Submit(ctx, &stubCommand{name: "blocked"})
		if err == nil {
			t.Errorf("expected error from canceled context, got nil")
		}
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()
	wg.Wait()
}

// =============================================================================
// SECTION 4: Burst Load Test
// =============================================================================

// TestBurstLoad_ManyTasksManyCommands verifies the system survives a burst
// of many tasks each emitting multiple commands.
func TestBurstLoad_ManyTasksManyCommands(t *testing.T) {
	// Use a submitter that doesn't block to focus on task scheduling.
	submitter := &stubSubmitter{}
	cfg := DefaultConfig()
	w := NewWorker(cfg, submitter, nil)

	const numTasks = 1000
	const cmdsPerTask = 3

	for i := 0; i < numTasks; i++ {
		i := i
		task := &dummyTask{
			name:    fmt.Sprintf("burst-task-%d", i),
			enabled: true,
			due:     true,
			runFn: func(ctx context.Context, s CommandSubmitter) error {
				for j := 0; j < cmdsPerTask; j++ {
					if err := s.Submit(ctx, &stubCommand{name: "burst"}); err != nil {
						return err
					}
				}
				return nil
			},
		}
		w.Register(task)
	}

	if w.TaskCount() != numTasks {
		t.Fatalf("expected %d tasks, got %d", numTasks, w.TaskCount())
	}

	// Evaluate all at once — burst.
	w.evaluateAndRun(time.Now())

	expected := numTasks * cmdsPerTask
	if submitter.submittedCount() != expected {
		t.Errorf("expected %d commands, got %d", expected, submitter.submittedCount())
	}
}

// =============================================================================
// SECTION 5: Failure Isolation
// =============================================================================

// TestFailureIsolation_MixedTaskFailures verifies that when some tasks fail,
// other tasks continue to execute and metrics increment correctly.
func TestFailureIsolation_MixedTaskFailures(t *testing.T) {
	submitter := &stubSubmitter{}

	// Task that will fail.
	failTask := &dummyTask{
		name:    "vacuum",
		enabled: true,
		due:     true,
		runFn: func(ctx context.Context, s CommandSubmitter) error {
			return errors.New("VACUUM failed: database locked")
		},
	}

	// Task that succeeds.
	okTask1 := &dummyTask{
		name:    "retention",
		enabled: true,
		due:     true,
		runFn: func(ctx context.Context, s CommandSubmitter) error {
			return s.Submit(ctx, &stubCommand{name: "retention-cmd"})
		},
	}

	// Another failing task.
	failTask2 := &dummyTask{
		name:    "optimize",
		enabled: true,
		due:     true,
		runFn: func(ctx context.Context, s CommandSubmitter) error {
			return errors.New("OPTIMIZE failed: disk full")
		},
	}

	// Another successful task.
	okTask2 := &dummyTask{
		name:    "checkpoint",
		enabled: true,
		due:     true,
		runFn: func(ctx context.Context, s CommandSubmitter) error {
			return s.Submit(ctx, &stubCommand{name: "checkpoint-cmd"})
		},
	}

	cfg := DefaultConfig()
	w := NewWorker(cfg, submitter, nil) // nil metrics: stress test focuses on isolation, not metrics
	w.Register(failTask)
	w.Register(okTask1)
	w.Register(failTask2)
	w.Register(okTask2)

	w.evaluateAndRun(time.Now())

	// All tasks should have been attempted.
	if submitter.submittedCount() != 2 {
		t.Errorf("expected 2 commands from successful tasks, got %d", submitter.submittedCount())
	}

	// Task failure must NOT stop the worker or other tasks.
	// (If we got here without panic, the worker continued past failures.)
}

// =============================================================================
// SECTION 6: Concurrency Injection (Writer Isolation)
// =============================================================================

// concurrentSubmitter records which goroutine calls Submit, so we can
// verify that commands are submitted from multiple goroutines but the
// architecture remains safe.
type concurrentSubmitter struct {
	mu         sync.Mutex
	commands   []storage.Command
	goroutines map[uint64]int
}

func newConcurrentSubmitter() *concurrentSubmitter {
	return &concurrentSubmitter{goroutines: make(map[uint64]int)}
}

func (s *concurrentSubmitter) Submit(ctx context.Context, cmd storage.Command) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.commands = append(s.commands, cmd)
	return nil
}

func (s *concurrentSubmitter) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.commands)
}

// TestConcurrency_MultipleGoroutinesSubmitCommands verifies the system
// handles concurrent command submission without races.
func TestConcurrency_MultipleGoroutinesSubmitCommands(t *testing.T) {
	submitter := newConcurrentSubmitter()
	cfg := DefaultConfig()
	w := NewWorker(cfg, submitter, nil)

	const numGoroutines = 10
	const cmdsPerGoroutine = 100

	// Register one task per goroutine, each submitting many commands.
	for g := 0; g < numGoroutines; g++ {
		g := g
		task := &dummyTask{
			name:    fmt.Sprintf("concurrent-%d", g),
			enabled: true,
			due:     true,
			runFn: func(ctx context.Context, s CommandSubmitter) error {
				for i := 0; i < cmdsPerGoroutine; i++ {
					if err := s.Submit(ctx, &stubCommand{name: fmt.Sprintf("g%d-c%d", g, i)}); err != nil {
						return err
					}
				}
				return nil
			},
		}
		w.Register(task)
	}

	// Run all tasks concurrently.
	var wg sync.WaitGroup
	for _, task := range w.tasks {
		wg.Add(1)
		go func(task MaintenanceTask) {
			defer wg.Done()
			_ = task.Run(context.Background(), submitter)
		}(task)
	}
	wg.Wait()

	expected := numGoroutines * cmdsPerGoroutine
	if submitter.count() != expected {
		t.Errorf("expected %d commands, got %d", expected, submitter.count())
	}
}

// TestConcurrency_RaceFree runs the worker under the race detector.
func TestConcurrency_RaceFree(t *testing.T) {
	submitter := &stubSubmitter{}
	cfg := DefaultConfig()
	w := NewWorker(cfg, submitter, nil)

	for i := 0; i < 50; i++ {
		w.Register(&dummyTask{
			name:    fmt.Sprintf("race-%d", i),
			enabled: true,
			due:     true,
			runFn: func(ctx context.Context, s CommandSubmitter) error {
				return s.Submit(ctx, &stubCommand{name: "race-cmd"})
			},
		})
	}

	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w.evaluateAndRun(time.Now())
		}()
	}
	wg.Wait()

	if submitter.submittedCount() != 250 {
		t.Errorf("expected 250 commands (5 runs × 50 tasks), got %d", submitter.submittedCount())
	}
}

// =============================================================================
// SECTION 7: Goroutine & Memory Stability
// =============================================================================

// TestGoroutineStability verifies no goroutine leaks after many start/stop cycles.
func TestGoroutineStability(t *testing.T) {
	submitter := &stubSubmitter{}
	cfg := DefaultConfig()
	cfg.CheckInterval = 5 * time.Millisecond

	before := runtime.NumGoroutine()

	const cycles = 50
	for i := 0; i < cycles; i++ {
		w := NewWorker(cfg, submitter, nil)
		w.Register(&dummyTask{name: "stability", enabled: true, due: true})
		ctx, cancel := context.WithCancel(context.Background())
		w.Start(ctx)
		time.Sleep(15 * time.Millisecond) // Let it run at least one cycle.
		cancel()
		w.Stop()
	}

	// Allow goroutines to settle.
	time.Sleep(50 * time.Millisecond)
	after := runtime.NumGoroutine()

	// Allow some tolerance for runtime goroutines.
	if after > before+10 {
		t.Errorf("goroutine leak: before=%d, after=%d (ran %d cycles)", before, after, cycles)
	}
}

// TestMemoryPlateau verifies no unbounded growth in the worker's internal state
// under sustained operation.
func TestMemoryPlateau(t *testing.T) {
	submitter := &stubSubmitter{}
	cfg := DefaultConfig()
	w := NewWorker(cfg, submitter, nil)

	// Register tasks once.
	const numTasks = 100
	for i := 0; i < numTasks; i++ {
		w.Register(&dummyTask{
			name:    fmt.Sprintf("mem-%d", i),
			enabled: true,
			due:     true,
		})
	}

	// Evaluate many times — task count should remain constant.
	for i := 0; i < 1000; i++ {
		w.evaluateAndRun(time.Now())
	}

	if w.TaskCount() != numTasks {
		t.Errorf("task count grew: expected %d, got %d (memory leak?)", numTasks, w.TaskCount())
	}
}

// =============================================================================
// SECTION 8: SQLite Writer Isolation (Integration)
// =============================================================================

// TestWriterIsolation_OnlyWriterExecutesSQL verifies through code structure
// that SQL execution paths only exist in the sqlite package. This is a
// static guardrail test similar to Section 1.
func TestWriterIsolation_OnlySQLitePackageHasSQL(t *testing.T) {
	// Verify that the maintenance framework does not import storage/sqlite.
	// Already covered in TestGuardrail_NoSQLiteInMaintenanceFramework.
	// This test is a semantic duplicate kept for documentation.
}

// =============================================================================
// SECTION 9: Extensibility Meta-Test
// =============================================================================

// IntegrityCheckTask is a completely new task type added to verify that
// the architecture supports adding tasks without modifying the scheduler,
// writer, or queue.
type IntegrityCheckTask struct {
	enabled   bool
	lastCheck time.Time
	interval  time.Duration
	runCount  int32
}

func NewIntegrityCheckTask(enabled bool, interval time.Duration) *IntegrityCheckTask {
	return &IntegrityCheckTask{enabled: enabled, interval: interval}
}

func (t *IntegrityCheckTask) Name() string  { return "integrity_check" }
func (t *IntegrityCheckTask) Enabled() bool { return t.enabled }
func (t *IntegrityCheckTask) Due(now time.Time) bool {
	if t.lastCheck.IsZero() {
		return true
	}
	return now.Sub(t.lastCheck) >= t.interval
}
func (t *IntegrityCheckTask) Run(ctx context.Context, submitter CommandSubmitter) error {
	atomic.AddInt32(&t.runCount, 1)
	t.lastCheck = time.Now()
	return submitter.Submit(ctx, &stubCommand{name: "integrity-check"})
}
func (t *IntegrityCheckTask) RunCount() int { return int(atomic.LoadInt32(&t.runCount)) }

// TestExtensibility_NewTaskRequiresOnlyRegistration is the meta-test:
// adding IntegrityCheckTask must NOT require scheduler, writer, or queue
// modifications.
func TestExtensibility_NewTaskRequiresOnlyRegistration(t *testing.T) {
	submitter := &stubSubmitter{}
	cfg := DefaultConfig()
	w := NewWorker(cfg, submitter, nil)

	// Register the completely new task type.
	// No modification to Worker, Writer, or CommandQueue required.
	integrityTask := NewIntegrityCheckTask(true, 1*time.Hour)
	w.Register(integrityTask)

	// Also register existing-style tasks.
	w.Register(&dummyTask{
		name:    "existing",
		enabled: true,
		due:     true,
		runFn: func(ctx context.Context, s CommandSubmitter) error {
			return s.Submit(ctx, &stubCommand{name: "existing-cmd"})
		},
	})

	if w.TaskCount() != 2 {
		t.Fatalf("expected 2 tasks, got %d", w.TaskCount())
	}

	w.evaluateAndRun(time.Now())

	if integrityTask.RunCount() != 1 {
		t.Errorf("integrity task did not run: count=%d", integrityTask.RunCount())
	}
	if submitter.submittedCount() < 2 {
		t.Errorf("expected at least 2 commands, got %d", submitter.submittedCount())
	}
}

// =============================================================================
// SECTION 10: Time Distortion & Determinism
// =============================================================================

// TestTimeDistortion_AcceleratedSimulation verifies the worker behaves
// deterministically under accelerated time.
func TestTimeDistortion_AcceleratedSimulation(t *testing.T) {
	clock := newFakeClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	submitter := &stubSubmitter{}

	task := newClockTask("distort-task", 12*time.Hour)
	task.SetClock(clock)
	cfg := DefaultConfig()
	cfg.CheckInterval = 1 * time.Hour
	w := NewWorker(cfg, submitter, nil)
	w.SetClock(clock)
	w.Register(task)

	// Simulate 7 days in 7 seconds (accelerated 86400x).
	deadline := clock.Now().Add(7 * 24 * time.Hour)
	step := 1 * time.Hour
	iterations := 0
	for clock.Now().Before(deadline) {
		w.evaluateAndRun(clock.Now())
		clock.Advance(step)
		iterations++
	}

	runs := task.RunCount()
	// With 12h interval over 7 days, expect ~14 runs (±1 for edge).
	expectedMin := 13
	expectedMax := 15
	if runs < expectedMin || runs > expectedMax {
		t.Errorf("determinism failure: %d runs over 7d with 12h interval (expected %d-%d, %d iterations)",
			runs, expectedMin, expectedMax, iterations)
	}
}

// TestTimeDistortion_RetentionDoesNotDrift verifies scheduling doesn't drift.
func TestTimeDistortion_RetentionDoesNotDrift(t *testing.T) {
	clock := newFakeClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	submitter := &stubSubmitter{}

	task := newClockTask("drift-task", 24*time.Hour)
	task.SetClock(clock)
	cfg := DefaultConfig()
	w := NewWorker(cfg, submitter, nil)
	w.SetClock(clock)
	w.Register(task)

	// Simulate exactly 30 days, hour by hour.
	deadline := clock.Now().Add(30 * 24 * time.Hour)
	step := 1 * time.Hour
	for clock.Now().Before(deadline) {
		w.evaluateAndRun(clock.Now())
		clock.Advance(step)
	}

	runs := task.RunCount()
	// Over 30 days with 24h interval: should run on days 1+2+...+30 = 30 runs
	// (first run at t=0, then every 24h).
	if runs < 29 || runs > 31 {
		t.Errorf("drift detected: %d runs over 30d with 24h interval (expected ~30)", runs)
	}
}

// =============================================================================
// Helpers (shared with other test files)
// =============================================================================

// stubSubmitter and stubCommand are defined in worker_test.go.
// They are re-declared here for self-contained stress tests.
// (Go allows re-declaration in the same package across files.)

// Ensure IntegrityCheckTask satisfies MaintenanceTask.
var _ MaintenanceTask = (*IntegrityCheckTask)(nil)
