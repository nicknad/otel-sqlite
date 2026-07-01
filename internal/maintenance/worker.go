package maintenance

import (
	"context"
	"log"
	"sync"
	"time"
)

// Worker is the maintenance scheduler. It periodically evaluates registered
// tasks and executes those that are due.
//
// The worker is fully generic: it has no knowledge of what tasks do, what
// commands they produce, or how they determine their schedule. It only
// calls the MaintenanceTask interface methods.
//
// The worker runs in its own goroutine. It must NOT execute SQL, touch
// SQLite, or access internal queues directly. All side effects go through
// CommandSubmitter.
type Worker struct {
	config  *Config
	metrics *Metrics
	clock   Clock

	// tasks is the set of registered maintenance tasks.
	tasks []MaintenanceTask

	// submitter sends commands to the SQLite writer.
	submitter CommandSubmitter

	// control
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewWorker creates a new maintenance worker.
// tasks may be nil; call Register to add tasks before starting.
func NewWorker(config *Config, submitter CommandSubmitter, metrics *Metrics) *Worker {
	if config == nil {
		config = DefaultConfig()
	}
	return &Worker{
		config:    config,
		metrics:   metrics,
		clock:     RealClock{},
		submitter: submitter,
		tasks:     make([]MaintenanceTask, 0),
	}
}

// SetClock overrides the clock used for time. Intended for testing.
// Must be called before Start.
func (w *Worker) SetClock(c Clock) {
	w.clock = c
}

// Register adds a maintenance task to the worker.
// Must be called before Start. Disabled tasks are still registered but
// skipped during evaluation.
func (w *Worker) Register(task MaintenanceTask) {
	w.tasks = append(w.tasks, task)
}

// Start begins the maintenance worker loop in a new goroutine.
// The worker runs until ctx is canceled.
func (w *Worker) Start(ctx context.Context) {
	w.ctx, w.cancel = context.WithCancel(ctx)
	w.wg.Add(1)
	go w.run()
}

// Stop cancels the worker and waits for it to finish.
func (w *Worker) Stop() {
	if w.cancel != nil {
		w.cancel()
	}
	w.wg.Wait()
}

// run is the main worker loop.
func (w *Worker) run() {
	defer w.wg.Done()

	if !w.config.MaintenanceEnabled {
		log.Println("maintenance worker: disabled, exiting")
		return
	}

	log.Printf("maintenance worker: starting with check_interval=%s, %d tasks registered",
		w.config.CheckInterval, len(w.tasks))

	// Run immediately on startup so tasks that are overdue run right away.
	w.evaluateAndRun(w.clock.Now())

	ticker := time.NewTicker(w.config.CheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-w.ctx.Done():
			log.Println("maintenance worker: shutting down")
			return
		case now := <-ticker.C:
			w.evaluateAndRun(now)
		}
	}
}

// evaluateAndRun iterates over all registered tasks and executes those
// that are both enabled and due. Failures in one task do not prevent
// other tasks from running.
func (w *Worker) evaluateAndRun(now time.Time) {
	if !w.config.MaintenanceEnabled {
		return
	}
	w.metrics.incRunsTotal()

	for _, task := range w.tasks {
		if !task.Enabled() {
			continue
		}
		if !task.Due(now) {
			continue
		}

		name := task.Name()
		log.Printf("maintenance: starting task %q", name)

		startTime := time.Now()
		err := task.Run(w.ctx, w.submitter)
		elapsed := time.Since(startTime).Seconds()

		if err != nil {
			log.Printf("maintenance: task %q failed: %v", name, err)
			w.metrics.incFailuresTotal(name)
		} else {
			log.Printf("maintenance: task %q completed (%.3fs)", name, elapsed)
			w.metrics.setLastRunTimestamp(name, float64(now.Unix()))
		}

		w.metrics.incTaskRunsTotal(name)
		w.metrics.observeDuration(name, elapsed)
	}
}

// TaskCount returns the number of registered tasks.
func (w *Worker) TaskCount() int {
	return len(w.tasks)
}
