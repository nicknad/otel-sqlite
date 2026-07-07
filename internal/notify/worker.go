package notify

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	"codeberg.org/nicknad/otel-sqlite/internal/metrics"
	"codeberg.org/nicknad/otel-sqlite/internal/model"
)

// Worker receives events from the batcher, runs them through the rule engine,
// manages state/retries in the store, and calls the appropriate Notifier.
type Worker struct {
	store     Store
	engine    RuleEngine
	notifiers map[string]Notifier
	aMetrics  *metrics.Metrics

	// incoming events from the batcher
	eventQueue chan *Event

	// background loop intervals
	retryInterval time.Duration

	// control
	ctx       context.Context
	cancel    context.CancelCauseFunc
	wg        sync.WaitGroup
	quiescent chan struct{} // closed after eventQueue is drained
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

	// Metrics is an optional metrics collector for instrumentation.
	Metrics *metrics.Metrics
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
		aMetrics:      cfg.Metrics,
		eventQueue:    make(chan *Event, qDepth),
		retryInterval: retryInterval,
	}
}

// Send enqueues an event for processing (non-blocking if queue not full).
// Returns ErrQueueFull if the event queue is at capacity.
func (w *Worker) Send(ctx context.Context, event *Event) error {
	select {
	case w.eventQueue <- event:
		if w.aMetrics != nil {
			w.aMetrics.IncrementNotifyEventsReceived()
		}
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
	w.quiescent = make(chan struct{})
	w.wg.Add(2)
	go w.processLoop()
	go w.retryLoop()
	log.Printf("notify worker: started (queue_depth=%d, retry_interval=%s)",
		cap(w.eventQueue), w.retryInterval)
}

// Stop signals shutdown. Drains the event queue before returning so
// that in-flight events are not lost.
func (w *Worker) Stop() {
	if w.cancel != nil {
		w.cancel(errors.New("notify worker stopped"))
	}
	<-w.quiescent // wait for drain to complete
	w.wg.Wait()
	log.Println("notify worker: stopped")
}

// processLoop is the main event processing goroutine.
func (w *Worker) processLoop() {
	defer w.wg.Done()

	for {
		select {
		case <-w.ctx.Done():
			log.Printf("notify worker: draining event queue before shutdown")
			w.drainQueue()
			close(w.quiescent)
			return
		case event := <-w.eventQueue:
			w.processEvent(event)
		}
	}
}

// drainQueue processes any remaining events in the queue without blocking.
func (w *Worker) drainQueue() {
	for {
		select {
		case event := <-w.eventQueue:
			w.processEvent(event)
		default:
			log.Printf("notify worker: event queue drained")
			return
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

	if w.aMetrics != nil {
		w.aMetrics.IncrementNotifyEventsMatched(rule.Name)
	}

	notifier, ok := w.notifiers[rule.Destination]
	if !ok {
		log.Printf("notify worker: no notifier registered for destination %q", rule.Destination)
		return
	}

	state := w.loadOrCreateState(ctx, key)
	if state == nil {
		return // error already logged
	}

	now := time.Now().UnixNano()

	if !w.checkCooldown(state, now) {
		return
	}
	if !w.checkDedup(state, rule, event, now) {
		return
	}
	if !w.checkRateLimit(state, rule, now) {
		return
	}

	// Store the event for potential retries.
	state.StoredEvent = event
	state.LastAttempt = now
	state.RetryCount = 0

	w.deliver(ctx, state, rule, key, event, notifier, now)
}

// loadOrCreateState fetches existing state or returns a fresh one.
// Returns nil on store errors (caller should abort).
func (w *Worker) loadOrCreateState(ctx context.Context, key string) *NotificationState {
	state, err := w.store.GetState(ctx, key)
	if err != nil && !errors.Is(err, ErrNotFound) {
		log.Printf("notify worker: get state %q: %v", key, err)
		return nil
	}
	if state == nil {
		state = &NotificationState{}
	}
	return state
}

// checkCooldown returns false if the event is within the cooldown window.
func (w *Worker) checkCooldown(state *NotificationState, now int64) bool {
	return now >= state.CooldownUntil
}

// checkDedup returns false if the event is a duplicate within the dedup window.
func (w *Worker) checkDedup(state *NotificationState, rule *Rule, event *Event, now int64) bool {
	if rule.DedupWindow <= 0 {
		return true
	}
	fingerprint := event.Fingerprint()
	if state.EventDigest == fingerprint &&
		now-state.LastAttempt < int64(rule.DedupWindow) {
		return false
	}
	state.EventDigest = fingerprint
	return true
}

// checkRateLimit returns false if the event would exceed the rate limit.
func (w *Worker) checkRateLimit(state *NotificationState, rule *Rule, now int64) bool {
	if rule.RateLimit <= 0 || rule.RateWindow <= 0 {
		return true
	}
	windowStart := now - int64(rule.RateWindow)
	bucket := pruneTimestamps(state.ErrorRateBucket, windowStart)
	if len(bucket) >= rule.RateLimit {
		return false
	}
	state.ErrorRateBucket = append(bucket, now)
	return true
}

// deliver sends the event to the notifier and updates state accordingly.
func (w *Worker) deliver(ctx context.Context, state *NotificationState, rule *Rule,
	key string, event *Event, notifier Notifier, now int64,
) {
	err := notifier.Send(ctx, event)
	if err != nil {
		log.Printf("notify worker: send to %q failed: %v", rule.Destination, err)

		if w.aMetrics != nil {
			w.aMetrics.IncrementNotifyEventsFailed(rule.Destination)
		}

		if !IsRetryable(err) {
			w.handleNonRetryable(ctx, state, key, event, err)
			return
		}

		state.RetryCount++
		state.NextRetry = now + backoffDuration(rule.RetryBackoff, state.RetryCount)

		if state.RetryCount >= rule.MaxRetries {
			w.handleDeadLetter(ctx, state, key, event, err)
			return
		}
	} else {
		if w.aMetrics != nil {
			w.aMetrics.IncrementNotifyEventsDelivered(rule.Destination)
		}
		state.LastSuccess = now
		state.CooldownUntil = now + int64(rule.Cooldown)
		state.ErrorRateBucket = nil
		state.RetryCount = 0
		state.NextRetry = 0
		state.StoredEvent = nil // no longer needed
	}

	if putErr := w.store.PutState(ctx, key, state); putErr != nil {
		log.Printf("notify worker: put state %q: %v", key, putErr)
	}
}

// handleNonRetryable sends a non-retryable failure directly to DLQ and removes state.
func (w *Worker) handleNonRetryable(ctx context.Context, state *NotificationState,
	key string, event *Event, err error,
) {
	state.DeadLettered = true
	if dlqErr := w.store.EnqueueDLQ(ctx, event, err.Error()); dlqErr != nil {
		log.Printf("notify worker: enqueue dlq: %v", dlqErr)
	}
	if delErr := w.store.DeleteState(ctx, key); delErr != nil {
		log.Printf("notify worker: delete state %q: %v", key, delErr)
	}
	if w.aMetrics != nil {
		w.aMetrics.IncrementNotifyEventsDeadLettered()
	}
}

// handleDeadLetter moves a max-retry event to DLQ and removes state.
func (w *Worker) handleDeadLetter(ctx context.Context, state *NotificationState,
	key string, event *Event, err error,
) {
	state.DeadLettered = true
	if dlqErr := w.store.EnqueueDLQ(ctx, event, err.Error()); dlqErr != nil {
		log.Printf("notify worker: enqueue dlq: %v", dlqErr)
	}
	if delErr := w.store.DeleteState(ctx, key); delErr != nil {
		log.Printf("notify worker: delete state %q: %v", key, delErr)
	}
	if w.aMetrics != nil {
		w.aMetrics.IncrementNotifyEventsDeadLettered()
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
	if w.aMetrics != nil {
		w.aMetrics.UpdateNotifyRetryQueueDepth(len(entries))
	}

	for _, entry := range entries {
		state, err := w.store.GetState(ctx, entry.Key)
		if err != nil {
			log.Printf("notify worker: get retryable state %q: %v", entry.Key, err)
			continue
		}

		sk := DecodeStateKey(entry.Key)
		rule := w.findRule(sk.RuleName)
		if rule == nil {
			log.Printf("notify worker: no rule found for key %q", entry.Key)
			continue
		}

		notifier, ok := w.notifiers[rule.Destination]
		if !ok {
			log.Printf("notify worker: no notifier for destination %q", rule.Destination)
			continue
		}

		// Reconstruct the event from stored state.
		event := state.StoredEvent
		if event == nil {
			// Fallback for legacy state entries without StoredEvent.
			event = &Event{
				Body:       entry.Key,
				ResourceID: sk.ResourceID,
			}
		}

		now := time.Now().UnixNano()
		state.LastAttempt = now

		err = notifier.Send(ctx, event)
		if err != nil {
			log.Printf("notify worker: retry %q failed: %v", entry.Key, err)

			if !IsRetryable(err) {
				w.handleNonRetryable(ctx, state, entry.Key, event, err)
				continue
			}

			state.RetryCount++
			state.NextRetry = now + backoffDuration(rule.RetryBackoff, state.RetryCount)

			if state.RetryCount >= rule.MaxRetries {
				w.handleDeadLetter(ctx, state, entry.Key, event, err)
				continue
			}
		} else {
			state.LastSuccess = now
			state.RetryCount = 0
			state.NextRetry = 0
			state.CooldownUntil = now + int64(rule.Cooldown)
			state.StoredEvent = nil
		}

		if putErr := w.store.PutState(ctx, entry.Key, state); putErr != nil {
			log.Printf("notify worker: put retry state %q: %v", entry.Key, putErr)
		}
	}
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

// backoffDuration computes exponential backoff: base * 2^(attempt-1).
func backoffDuration(base time.Duration, attempt int) int64 {
	if attempt <= 0 {
		return int64(base)
	}
	// Cap the exponent at 10 to avoid int64 overflow.
	shift := min(attempt-1, 10)
	return int64(base) * (1 << shift)
}
