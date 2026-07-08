package notifications

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	"codeberg.org/nicknad/otel-sqlite/internal/alerts"
	"codeberg.org/nicknad/otel-sqlite/internal/events"
	"codeberg.org/nicknad/otel-sqlite/internal/metrics"
	"codeberg.org/nicknad/otel-sqlite/internal/model"
	"codeberg.org/nicknad/otel-sqlite/internal/rules"
)

// Worker receives events from the batcher, runs them through the rule engine,
// maintains alert state (window → counter → state → alert), and delivers
// alert notifications via pluggable notifiers with retry and DLQ support.
type Worker struct {
	alertStore alerts.AlertStore
	notifStore Store
	engine     rules.RuleEngine
	notifiers  map[string]Notifier
	aMetrics   *metrics.Metrics

	eventQueue chan *events.Event

	retryInterval time.Duration
	gcInterval    time.Duration

	ctx       context.Context
	cancel    context.CancelCauseFunc
	wg        sync.WaitGroup
	quiescent chan struct{}
}

// WorkerConfig holds configuration for the notification Worker.
type WorkerConfig struct {
	// AlertStore persists alert objects.
	AlertStore alerts.AlertStore

	// NotifStore persists delivery state and DLQ.
	NotifStore Store

	// Engine evaluates rules against events.
	Engine rules.RuleEngine

	// Notifiers is a map of destination name → Notifier implementation.
	Notifiers map[string]Notifier

	// EventQueueDepth is the buffered channel capacity for incoming events.
	// Defaults to 1000 if <= 0.
	EventQueueDepth int

	// RetryInterval is how often the retry goroutine scans for retryable deliveries.
	// Defaults to 30s if <= 0.
	RetryInterval time.Duration

	// GCInterval is how often the worker scans for garbage-collectible resolved alerts.
	// Defaults to 5m if <= 0.
	GCInterval time.Duration

	// Metrics is an optional metrics collector.
	Metrics *metrics.Metrics
}

// NewWorker creates a notification Worker.
func NewWorker(cfg *WorkerConfig) *Worker {
	if cfg == nil {
		panic("notifications: WorkerConfig must not be nil")
	}
	if cfg.AlertStore == nil {
		panic("notifications: AlertStore must not be nil")
	}
	if cfg.NotifStore == nil {
		panic("notifications: NotifStore must not be nil")
	}
	if cfg.Engine == nil {
		panic("notifications: RuleEngine must not be nil")
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
	gcInterval := cfg.GCInterval
	if gcInterval <= 0 {
		gcInterval = 5 * time.Minute
	}

	return &Worker{
		alertStore:    cfg.AlertStore,
		notifStore:    cfg.NotifStore,
		engine:        cfg.Engine,
		notifiers:     cfg.Notifiers,
		aMetrics:      cfg.Metrics,
		eventQueue:    make(chan *events.Event, qDepth),
		retryInterval: retryInterval,
		gcInterval:    gcInterval,
	}
}

// Send enqueues an event for processing (non-blocking if queue not full).
// Returns ErrQueueFull if the event queue is at capacity.
func (w *Worker) Send(ctx context.Context, event *events.Event) error {
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
	return w.Send(ctx, events.EventFromLogRecord(record))
}

// Start launches the worker goroutines: process, retry, and GC.
func (w *Worker) Start(ctx context.Context) {
	w.ctx, w.cancel = context.WithCancelCause(ctx)
	w.quiescent = make(chan struct{})
	w.wg.Add(3)
	go w.processLoop()
	go w.retryLoop()
	go w.gcLoop()
	log.Printf("notifications worker: started (queue_depth=%d, retry_interval=%s, gc_interval=%s)",
		cap(w.eventQueue), w.retryInterval, w.gcInterval)
}

// Stop signals shutdown and drains the event queue.
func (w *Worker) Stop() {
	if w.cancel != nil {
		w.cancel(errors.New("notifications worker stopped"))
	}
	<-w.quiescent
	w.wg.Wait()
	log.Println("notifications worker: stopped")
}

// processLoop is the main event processing goroutine.
func (w *Worker) processLoop() {
	defer w.wg.Done()

	for {
		select {
		case <-w.ctx.Done():
			log.Printf("notifications worker: draining event queue before shutdown")
			w.drainQueue()
			close(w.quiescent)
			return
		case event := <-w.eventQueue:
			w.processEvent(event)
		}
	}
}

// drainQueue processes remaining events without blocking.
func (w *Worker) drainQueue() {
	for {
		select {
		case event := <-w.eventQueue:
			w.processEvent(event)
		default:
			log.Printf("notifications worker: event queue drained")
			return
		}
	}
}

// processEvent evaluates an event against rules and updates alert state.
func (w *Worker) processEvent(event *events.Event) {
	ctx := w.ctx

	rule, matched := w.engine.Evaluate(ctx, event)
	if !matched {
		return
	}

	if w.aMetrics != nil {
		w.aMetrics.IncrementNotifyEventsMatched(rule.Name)
	}

	now := time.Now().UnixNano()
	alertID := alerts.AlertID(rule.Name, event.ResourceID)

	// Load or create alert.
	alert, err := w.alertStore.Get(ctx, alertID)
	if err != nil {
		log.Printf("notifications worker: get alert %q: %v", alertID, err)
		return
	}

	if alert == nil {
		alert = alerts.NewAlert(rule.Name, event.ResourceID, event.Severity, now)
	} else {
		alert.LastMatched = now
		alert.Severity = event.Severity
	}

	// Build a counter from the stored window timestamps + the new event.
	counter := alerts.NewCounter(rule.AlertWindow, rule.AlertThreshold)
	if len(alert.WindowTimestamps) > 0 {
		counter.LoadFrom(alert.WindowTimestamps)
	}
	count, _ := counter.Hit(now)

	// Save window timestamps back to alert.
	alert.WindowTimestamps = counter.Snapshot()

	// Evaluate state transition.
	transition := alerts.EvaluateAlert(alert, count, now, rule.AlertThreshold, rule.AlertResolveWindow)

	switch transition {
	case alerts.TransitionGarbage:
		// Alert was pending with no recent activity — delete.
		if delErr := w.alertStore.Delete(ctx, alertID); delErr != nil {
			log.Printf("notifications worker: delete garbage alert %q: %v", alertID, delErr)
		}
		return

	case alerts.TransitionNone:
		// No state change: just update count/timestamps.
		alert.Count = count
		alert.UpdatedAt = now
		if putErr := w.alertStore.Put(ctx, alert); putErr != nil {
			log.Printf("notifications worker: put alert %q: %v", alertID, putErr)
		}
		return
	}

	// State transition occurred: Firing or Resolved.
	alerts.ApplyTransition(alert, transition, count, now)

	if putErr := w.alertStore.Put(ctx, alert); putErr != nil {
		log.Printf("notifications worker: put alert %q: %v", alertID, putErr)
		return
	}

	// Deliver notification for the state change.
	w.deliverAlert(ctx, alert, rule)
}

// deliverAlert sends an alert notification via the appropriate notifier.
func (w *Worker) deliverAlert(ctx context.Context, alert *alerts.Alert, rule *rules.Rule) {
	notifier, ok := w.notifiers[rule.Destination]
	if !ok {
		log.Printf("notifications worker: no notifier for destination %q", rule.Destination)
		return
	}

	now := time.Now().UnixNano()

	// Check cooldown before delivering.
	state, err := w.notifStore.GetNotificationState(ctx, alert.ID)
	if err != nil && !errors.Is(err, ErrNotFound) {
		log.Printf("notifications worker: get notification state %q: %v", alert.ID, err)
		return
	}
	if state != nil && now < state.CooldownUntil {
		return // within cooldown
	}

	err = notifier.Send(ctx, alert)
	if err != nil {
		log.Printf("notifications worker: send alert %q to %q failed: %v", alert.ID, rule.Destination, err)

		if w.aMetrics != nil {
			w.aMetrics.IncrementNotifyEventsFailed(rule.Destination)
		}

		if !IsRetryable(err) {
			w.handleNonRetryable(ctx, alert, err)
			return
		}

		// Schedule retry.
		if state == nil {
			state = &NotificationState{}
		}
		state.RetryCount++
		state.LastAttempt = now
		state.NextRetry = now + backoffDuration(rule.RetryBackoff, state.RetryCount)

		if state.RetryCount >= rule.MaxRetries {
			w.handleDeadLetter(ctx, alert, state, err)
			return
		}

		if putErr := w.notifStore.PutNotificationState(ctx, alert.ID, state); putErr != nil {
			log.Printf("notifications worker: put notification state %q: %v", alert.ID, putErr)
		}
	} else {
		if w.aMetrics != nil {
			w.aMetrics.IncrementNotifyEventsDelivered(rule.Destination)
		}

		if state == nil {
			state = &NotificationState{}
		}
		state.LastSuccess = now
		state.CooldownUntil = now + int64(rule.Cooldown)
		state.RetryCount = 0
		state.NextRetry = 0

		if putErr := w.notifStore.PutNotificationState(ctx, alert.ID, state); putErr != nil {
			log.Printf("notifications worker: put notification state %q: %v", alert.ID, putErr)
		}
	}
}

// handleNonRetryable sends a non-retryable failure directly to DLQ.
func (w *Worker) handleNonRetryable(ctx context.Context, alert *alerts.Alert, err error) {
	if dlqErr := w.notifStore.EnqueueDLQ(ctx, alert, err.Error()); dlqErr != nil {
		log.Printf("notifications worker: enqueue dlq: %v", dlqErr)
	}
	if delErr := w.notifStore.DeleteNotificationState(ctx, alert.ID); delErr != nil {
		log.Printf("notifications worker: delete notification state %q: %v", alert.ID, delErr)
	}
	if w.aMetrics != nil {
		w.aMetrics.IncrementNotifyEventsDeadLettered()
	}
}

// handleDeadLetter moves a max-retry alert to DLQ.
func (w *Worker) handleDeadLetter(ctx context.Context, alert *alerts.Alert, state *NotificationState, err error) {
	state.DeadLettered = true
	if dlqErr := w.notifStore.EnqueueDLQ(ctx, alert, err.Error()); dlqErr != nil {
		log.Printf("notifications worker: enqueue dlq: %v", dlqErr)
	}
	if delErr := w.notifStore.DeleteNotificationState(ctx, alert.ID); delErr != nil {
		log.Printf("notifications worker: delete notification state %q: %v", alert.ID, delErr)
	}
	if w.aMetrics != nil {
		w.aMetrics.IncrementNotifyEventsDeadLettered()
	}
}

// retryLoop periodically scans for retryable alert deliveries.
func (w *Worker) retryLoop() {
	defer w.wg.Done()

	ticker := time.NewTicker(w.retryInterval)
	defer ticker.Stop()

	for {
		select {
		case <-w.ctx.Done():
			return
		case <-ticker.C:
			w.retryDeliveries()
		}
	}
}

// retryDeliveries scans and re-attempts failed alert deliveries.
func (w *Worker) retryDeliveries() {
	ctx := w.ctx

	entries, err := w.notifStore.ScanRetryable(ctx)
	if err != nil {
		log.Printf("notifications worker: scan retryable: %v", err)
		return
	}
	if w.aMetrics != nil {
		w.aMetrics.UpdateNotifyRetryQueueDepth(len(entries))
	}

	for _, entry := range entries {
		state, err := w.notifStore.GetNotificationState(ctx, entry.AlertID)
		if err != nil {
			log.Printf("notifications worker: get retryable state %q: %v", entry.AlertID, err)
			continue
		}

		// Load the alert to re-deliver.
		alert, err := w.alertStore.Get(ctx, entry.AlertID)
		if err != nil {
			log.Printf("notifications worker: get retryable alert %q: %v", entry.AlertID, err)
			continue
		}
		if alert == nil {
			// Alert deleted — clean up state.
			_ = w.notifStore.DeleteNotificationState(ctx, entry.AlertID)
			continue
		}

		// Find the rule for this alert.
		rule := w.findRule(alert.RuleID)
		if rule == nil {
			log.Printf("notifications worker: no rule %q for alert %q", alert.RuleID, entry.AlertID)
			continue
		}

		notifier, ok := w.notifiers[rule.Destination]
		if !ok {
			log.Printf("notifications worker: no notifier for %q", rule.Destination)
			continue
		}

		now := time.Now().UnixNano()
		state.LastAttempt = now

		err = notifier.Send(ctx, alert)
		if err != nil {
			log.Printf("notifications worker: retry %q failed: %v", entry.AlertID, err)

			if !IsRetryable(err) {
				w.handleNonRetryable(ctx, alert, err)
				continue
			}

			state.RetryCount++
			state.NextRetry = now + backoffDuration(rule.RetryBackoff, state.RetryCount)

			if state.RetryCount >= rule.MaxRetries {
				w.handleDeadLetter(ctx, alert, state, err)
				continue
			}
		} else {
			state.LastSuccess = now
			state.CooldownUntil = now + int64(rule.Cooldown)
			state.RetryCount = 0
			state.NextRetry = 0
		}

		if putErr := w.notifStore.PutNotificationState(ctx, entry.AlertID, state); putErr != nil {
			log.Printf("notifications worker: put retry state %q: %v", entry.AlertID, putErr)
		}
	}
}

// gcLoop periodically cleans up old resolved alerts and delivery state.
func (w *Worker) gcLoop() {
	defer w.wg.Done()

	ticker := time.NewTicker(w.gcInterval)
	defer ticker.Stop()

	for {
		select {
		case <-w.ctx.Done():
			return
		case <-ticker.C:
			w.garbageCollect()
		}
	}
}

// garbageCollect removes resolved alerts older than a threshold.
func (w *Worker) garbageCollect() {
	ctx := w.ctx

	resolvedAlerts, err := w.alertStore.ListByStatus(ctx, alerts.AlertResolved)
	if err != nil {
		log.Printf("notifications worker: list resolved alerts: %v", err)
		return
	}

	cutoff := time.Now().Add(-24 * time.Hour).UnixNano()
	for _, alert := range resolvedAlerts {
		if alert.UpdatedAt < cutoff {
			if delErr := w.alertStore.Delete(ctx, alert.ID); delErr != nil {
				log.Printf("notifications worker: delete resolved alert %q: %v", alert.ID, delErr)
			}
			_ = w.notifStore.DeleteNotificationState(ctx, alert.ID)
		}
	}
}

// findRule returns the rule with the given name, or nil.
func (w *Worker) findRule(name string) *rules.Rule {
	rs := w.engine.Rules()
	for i := range rs {
		if rs[i].Name == name {
			return &rs[i]
		}
	}
	return nil
}

// backoffDuration computes exponential backoff: base * 2^(attempt-1).
func backoffDuration(base time.Duration, attempt int) int64 {
	if attempt <= 0 {
		return int64(base)
	}
	shift := min(attempt-1, 10)
	return int64(base) * (1 << shift)
}
