package notify

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	"codeberg.org/nicknad/otel-sqlite/internal/model"
)

// Worker receives events from the batcher, runs them through the rule engine,
// manages state/retries in the store, and calls the appropriate Notifier.
type Worker struct {
	store     Store
	engine    RuleEngine
	notifiers map[string]Notifier

	// incoming events from the batcher
	eventQueue chan *Event

	// background loop intervals
	retryInterval time.Duration

	// control
	ctx    context.Context
	cancel context.CancelCauseFunc
	wg     sync.WaitGroup
}

// WorkerConfig holds configuration for the notification Worker.
type WorkerConfig struct {
	// Store is the persistent state store.
	Store Store

	// Engine evaluates rules against events.
	Engine RuleEngine

	// Notifiers is a map of destination name → Notifier implementation.
	Notifiers map[string]Notifier

	// EventQueueDepth is the buffered channel capacity for incoming events.
	// Defaults to 1000 if <= 0.
	EventQueueDepth int

	// RetryInterval is how often the retry goroutine scans for retryable events.
	// Defaults to 30s if <= 0.
	RetryInterval time.Duration
}

// NewWorker creates a notification Worker.
func NewWorker(cfg *WorkerConfig) *Worker {
	if cfg == nil {
		panic("notify: WorkerConfig must not be nil")
	}
	if cfg.Store == nil {
		panic("notify: Store must not be nil")
	}
	if cfg.Engine == nil {
		panic("notify: RuleEngine must not be nil")
	}
	if cfg.Notifiers == nil {
		cfg.Notifiers = make(map[string]Notifier)
	}

	qDepth := cfg.EventQueueDepth
	if qDepth <= 0 {
		qDepth = 1000
	}
	retryInterval := cfg.RetryInterval
	if retryInterval <= 0 {
		retryInterval = 30 * time.Second
	}

	return &Worker{
		store:         cfg.Store,
		engine:        cfg.Engine,
		notifiers:     cfg.Notifiers,
		eventQueue:    make(chan *Event, qDepth),
		retryInterval: retryInterval,
	}
}

// Send enqueues an event for processing (non-blocking if queue not full).
// Returns ErrQueueFull if the event queue is at capacity.
func (w *Worker) Send(ctx context.Context, event *Event) error {
	select {
	case w.eventQueue <- event:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	default:
		return ErrQueueFull
	}
}

// SendRecord converts a model.LogRecord to an Event and enqueues it.
// Implements the batcher.ErrorNotifier interface.
func (w *Worker) SendRecord(ctx context.Context, record *model.LogRecord) error {
	return w.Send(ctx, EventFromLogRecord(record))
}

// Start launches the worker goroutines (main processor + retry loop).
func (w *Worker) Start(ctx context.Context) {
	w.ctx, w.cancel = context.WithCancelCause(ctx)
	w.wg.Add(2)
	go w.processLoop()
	go w.retryLoop()
	log.Printf("notify worker: started (queue_depth=%d, retry_interval=%s)",
		cap(w.eventQueue), w.retryInterval)
}

// Stop signals shutdown. Returns after in-flight events are handled.
func (w *Worker) Stop() {
	if w.cancel != nil {
		w.cancel(errors.New("notify worker stopped"))
	}
	w.wg.Wait()
	log.Println("notify worker: stopped")
}

// processLoop is the main event processing goroutine.
func (w *Worker) processLoop() {
	defer w.wg.Done()

	for {
		select {
		case <-w.ctx.Done():
			log.Printf("notify worker: process loop shutting down: %v", context.Cause(w.ctx))
			return
		case event := <-w.eventQueue:
			w.processEvent(event)
		}
	}
}

// retryLoop periodically scans the store for retryable events.
func (w *Worker) retryLoop() {
	defer w.wg.Done()

	ticker := time.NewTicker(w.retryInterval)
	defer ticker.Stop()

	for {
		select {
		case <-w.ctx.Done():
			log.Printf("notify worker: retry loop shutting down: %v", context.Cause(w.ctx))
			return
		case <-ticker.C:
			w.retryEvents()
		}
	}
}

// processEvent evaluates an event and delivers it if rules match.
func (w *Worker) processEvent(event *Event) {
	ctx := w.ctx

	rule, key, matched := w.engine.Evaluate(ctx, event)
	if !matched {
		return
	}

	notifier, ok := w.notifiers[rule.Destination]
	if !ok {
		log.Printf("notify worker: no notifier registered for destination %q", rule.Destination)
		return
	}

	// Load or create state.
	state, err := w.store.GetState(ctx, key)
	if err != nil && !errors.Is(err, ErrNotFound) {
		log.Printf("notify worker: get state %q: %v", key, err)
		return
	}
	if state == nil {
		state = &NotificationState{}
	}

	now := time.Now().UnixNano()

	// Check cooldown.
	if now < state.CooldownUntil {
		return
	}

	// Check dedup.
	if rule.DedupWindow > 0 {
		fingerprint := eventFingerprint(event)
		if state.EventDigest == fingerprint &&
			now-state.LastAttempt < int64(rule.DedupWindow) {
			return
		}
		state.EventDigest = fingerprint
	}

	// Check rate limit.
	if rule.RateLimit > 0 && rule.RateWindow > 0 {
		windowStart := now - int64(rule.RateWindow)
		bucket := pruneTimestamps(state.ErrorRateBucket, windowStart)
		if len(bucket) >= rule.RateLimit {
			return // rate limited
		}
		state.ErrorRateBucket = append(bucket, now)
	}

	// Deliver.
	state.LastAttempt = now
	state.RetryCount = 0 // reset on fresh attempt

	err = notifier.Send(ctx, event)
	if err != nil {
		log.Printf("notify worker: send to %q failed: %v", rule.Destination, err)
		state.RetryCount++
		state.NextRetry = now + backoffDuration(rule.RetryBackoff, state.RetryCount)

		if state.RetryCount >= rule.MaxRetries {
			state.DeadLettered = true
			if dlqErr := w.store.EnqueueDLQ(ctx, event, err.Error()); dlqErr != nil {
				log.Printf("notify worker: enqueue dlq: %v", dlqErr)
			}
			// Remove state so the retry loop won't pick it up again.
			if delErr := w.store.DeleteState(ctx, key); delErr != nil {
				log.Printf("notify worker: delete dlq state %q: %v", key, delErr)
			}
			return
		}
	} else {
		state.LastSuccess = now
		state.CooldownUntil = now + int64(rule.Cooldown)
		state.ErrorRateBucket = nil
		state.RetryCount = 0
		state.NextRetry = 0
	}

	// Persist state.
	if putErr := w.store.PutState(ctx, key, state); putErr != nil {
		log.Printf("notify worker: put state %q: %v", key, putErr)
	}
}

// retryEvents scans the store for retryable events and re-attempts delivery.
func (w *Worker) retryEvents() {
	ctx := w.ctx

	entries, err := w.store.ScanRetryable(ctx)
	if err != nil {
		log.Printf("notify worker: scan retryable: %v", err)
		return
	}

	for _, entry := range entries {
		state, err := w.store.GetState(ctx, entry.Key)
		if err != nil {
			log.Printf("notify worker: get retryable state %q: %v", entry.Key, err)
			continue
		}

		// Parse the key to get rule name. Key format: "ruleName:resourceID:fingerprint"
		ruleName := parseRuleFromKey(entry.Key)
		rule := w.findRule(ruleName)
		if rule == nil {
			log.Printf("notify worker: no rule found for key %q", entry.Key)
			continue
		}

		notifier, ok := w.notifiers[rule.Destination]
		if !ok {
			log.Printf("notify worker: no notifier for destination %q", rule.Destination)
			continue
		}

		// Construct a minimal event from state (we don't have the full event on retry).
		// The DLQ stores full events; retry entries only have state.
		// For a retry, we create a synthetic event with the key info.
		event := &Event{
			Body:       entry.Key,
			ResourceID: parseResourceFromKey(entry.Key),
		}

		now := time.Now().UnixNano()
		state.LastAttempt = now

		err = notifier.Send(ctx, event)
		if err != nil {
			log.Printf("notify worker: retry %q failed: %v", entry.Key, err)
			state.RetryCount++
			state.NextRetry = now + backoffDuration(rule.RetryBackoff, state.RetryCount)

			if state.RetryCount >= rule.MaxRetries {
				state.DeadLettered = true
				if dlqErr := w.store.EnqueueDLQ(ctx, event, err.Error()); dlqErr != nil {
					log.Printf("notify worker: enqueue dlq: %v", dlqErr)
				}
				// Remove state so it won't be retried again.
				if delErr := w.store.DeleteState(ctx, entry.Key); delErr != nil {
					log.Printf("notify worker: delete dlq state %q: %v", entry.Key, delErr)
				}
				continue
			}
		} else {
			state.LastSuccess = now
			state.RetryCount = 0
			state.NextRetry = 0
			state.CooldownUntil = now + int64(rule.Cooldown)
		}

		if putErr := w.store.PutState(ctx, entry.Key, state); putErr != nil {
			log.Printf("notify worker: put retry state %q: %v", entry.Key, putErr)
		}
	}
}

// parseRuleFromKey extracts the rule name from a state key.
// Key format: "ruleName:resourceID:fingerprint"
func parseRuleFromKey(key string) string {
	for i := range len(key) {
		if key[i] == ':' {
			return key[:i]
		}
	}
	return key
}

// parseResourceFromKey extracts the resource ID from a state key.
func parseResourceFromKey(key string) string {
	first := -1
	second := -1
	for i := range len(key) {
		if key[i] == ':' {
			if first == -1 {
				first = i
			} else {
				second = i
				break
			}
		}
	}
	if first >= 0 && second > first {
		return key[first+1 : second]
	}
	return key
}

// findRule returns the rule with the given name, or nil.
func (w *Worker) findRule(name string) *Rule {
	rules := w.engine.Rules()
	for i := range rules {
		if rules[i].Name == name {
			return &rules[i]
		}
	}
	return nil
}

// pruneTimestamps removes timestamps older than windowStart from the slice.
func pruneTimestamps(bucket []int64, windowStart int64) []int64 {
	cut := 0
	for _, ts := range bucket {
		if ts >= windowStart {
			bucket[cut] = ts
			cut++
		}
	}
	return bucket[:cut]
}

// backoffDuration computes exponential backoff: base * 2^attempt.
func backoffDuration(base time.Duration, attempt int) int64 {
	if attempt <= 0 {
		return int64(base)
	}
	shift := min(attempt-1,
		// cap to avoid overflow
		10)
	return int64(base) * (1 << shift)
}
