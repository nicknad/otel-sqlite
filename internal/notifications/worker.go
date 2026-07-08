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

const (
	defaultFlushInterval   = 200 * time.Millisecond
	defaultDeliveryWorkers = 4
)

// Worker receives events from the batcher, runs them through the rule engine,
// maintains alert state (window → counter → state → alert), and delivers
// alert notifications via pluggable notifiers with retry and DLQ support.
//
// Architecture:
//   - N process goroutines pull from a shared event channel, evaluate rules,
//     update alert state in-memory, and accumulate dirty alerts in per-shard maps.
//   - A flush goroutine periodically writes dirty alerts to bbolt in batch,
//     eliminating per-event bbolt write contention.
//   - A bounded delivery goroutine pool handles HTTP notifier calls asynchronously
//     so a slow webhook doesn't stall alert processing.
//   - Retry and GC goroutines for background maintenance.
type Worker struct {
	alertStore alerts.AlertStore
	notifStore Store
	engine     rules.RuleEngine
	notifiers  map[string]Notifier
	aMetrics   *metrics.Metrics

	eventQueue chan *events.Event

	retryInterval   time.Duration
	gcInterval      time.Duration
	flushInterval   time.Duration
	deliveryWorkers int

	alertIdleTTL            time.Duration
	dlqRetention            time.Duration
	bboltCompactionEnabled  bool
	bboltCompactionInterval time.Duration
	lastCompaction          time.Time

	// dirtyAlerts accumulates alert updates in memory.  The process
	// goroutine writes here; the flush goroutine drains to bbolt.
	dirtyAlerts map[string]*alerts.Alert
	dirtyMu     sync.Mutex

	// Delivery queue buffers state-change alerts for the delivery pool.
	deliveryQueue chan deliveryJob

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

	// AlertIdleTTL is how long Pending/Firing alerts can remain idle (no
	// matching events) before being garbage-collected. Prevents unbounded
	// growth from ephemeral resources that emit one error then disappear.
	// Defaults to 24h if <= 0.
	AlertIdleTTL time.Duration

	// DLQRetention is how long dead-letter queue entries are retained before
	// being purged. Defaults to 30 days if <= 0.
	DLQRetention time.Duration

	// BboltCompactionEnabled enables periodic compaction of the bbolt stores.
	// Defaults to false.
	BboltCompactionEnabled bool

	// BboltCompactionInterval is how often to compact the bbolt stores.
	// Defaults to 24h if enabled and <= 0.
	BboltCompactionInterval time.Duration

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
	alertIdleTTL := cfg.AlertIdleTTL
	if alertIdleTTL <= 0 {
		alertIdleTTL = 24 * time.Hour
	}
	dlqRetention := cfg.DLQRetention
	if dlqRetention <= 0 {
		dlqRetention = 30 * 24 * time.Hour
	}
	compactionInterval := cfg.BboltCompactionInterval
	if compactionInterval <= 0 {
		compactionInterval = 24 * time.Hour
	}

	return &Worker{
		alertStore:              cfg.AlertStore,
		notifStore:              cfg.NotifStore,
		engine:                  cfg.Engine,
		notifiers:               cfg.Notifiers,
		aMetrics:                cfg.Metrics,
		eventQueue:              make(chan *events.Event, qDepth),
		retryInterval:           retryInterval,
		gcInterval:              gcInterval,
		alertIdleTTL:            alertIdleTTL,
		dlqRetention:            dlqRetention,
		bboltCompactionEnabled:  cfg.BboltCompactionEnabled,
		bboltCompactionInterval: compactionInterval,
		flushInterval:           defaultFlushInterval,
		deliveryWorkers:         defaultDeliveryWorkers,
		dirtyAlerts:             make(map[string]*alerts.Alert),
		deliveryQueue:           make(chan deliveryJob, 256),
	}
}

// Send enqueues an event for processing (non-blocking if queue not full).
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

// Start launches all worker goroutines: process, flush, delivery pool, retry, GC.
func (w *Worker) Start(ctx context.Context) {
	w.ctx, w.cancel = context.WithCancelCause(ctx)
	w.quiescent = make(chan struct{})

	// 1 process + N delivery + 1 flush + 1 retry + 1 gc
	totalGoroutines := 1 + w.deliveryWorkers + 3
	w.wg.Add(totalGoroutines)

	go w.processLoop()

	for range w.deliveryWorkers {
		go w.deliveryLoop()
	}

	go w.flushLoop()
	go w.retryLoop()
	go w.gcLoop()

	log.Printf("notifications worker: started (queue_depth=%d flush=%s retry=%s gc=%s delivery_workers=%d)",
		cap(w.eventQueue), w.flushInterval, w.retryInterval, w.gcInterval, w.deliveryWorkers)
}

// Stop signals shutdown, drains the event queue, and flushes dirty alerts.
func (w *Worker) Stop() {
	if w.cancel != nil {
		w.cancel(errors.New("notifications worker stopped"))
	}

	// Drain remaining events from the queue.
	go func() {
		for {
			select {
			case event := <-w.eventQueue:
				w.processEvent(event)
			default:
				close(w.quiescent)
				return
			}
		}
	}()

	<-w.quiescent
	w.wg.Wait()

	// Final flush of any remaining dirty alerts.
	w.flushDirty()
	log.Println("notifications worker: stopped")
}

// ---------------------------------------------------------------------------
// Process goroutine
// ---------------------------------------------------------------------------

func (w *Worker) processLoop() {
	defer w.wg.Done()

	for {
		select {
		case <-w.ctx.Done():
			return
		case event := <-w.eventQueue:
			w.processEvent(event)
		}
	}
}

// processEvent evaluates an event against rules and updates alert state.
// Dirty alerts are accumulated in w.dirtyAlerts instead of written to bbolt.
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

	// Try dirty map first, then bbolt.
	w.dirtyMu.Lock()
	alert, ok := w.dirtyAlerts[alertID]
	w.dirtyMu.Unlock()
	if !ok {
		var err error
		alert, err = w.alertStore.Get(ctx, alertID)
		if err != nil {
			log.Printf("notifications worker: get alert %q: %v", alertID, err)
			return
		}
	}

	if alert == nil {
		alert = alerts.NewAlert(rule.Name, event.ResourceID, event.Severity, now)
	} else {
		alert.LastMatched = now
		alert.Severity = event.Severity
	}

	// Add event timestamp to the alert's window in-place (no copies).
	alert.WindowTimestamps = alerts.AddToSlice(alert.WindowTimestamps, now, rule.AlertWindow)
	count := len(alert.WindowTimestamps)

	// Evaluate state transition.
	transition := alerts.EvaluateAlert(alert, count, now, rule.AlertThreshold, rule.AlertResolveWindow)

	switch transition {
	case alerts.TransitionGarbage:
		if delErr := w.alertStore.Delete(ctx, alertID); delErr != nil {
			log.Printf("notifications worker: delete garbage alert %q: %v", alertID, delErr)
		}
		w.dirtyMu.Lock()
		delete(w.dirtyAlerts, alertID)
		w.dirtyMu.Unlock()
		return

	case alerts.TransitionNone:
		alert.Count = count
		alert.UpdatedAt = now
		w.dirtyMu.Lock()
		w.dirtyAlerts[alertID] = alert
		w.dirtyMu.Unlock()
		return
	}

	// State transition occurred: Firing or Resolved.
	alerts.ApplyTransition(alert, transition, count, now)
	w.dirtyMu.Lock()
	w.dirtyAlerts[alertID] = alert
	w.dirtyMu.Unlock()

	// Deliver the state-change notification asynchronously.
	w.tryDeliverAlert(ctx, alert, rule)
}

// ---------------------------------------------------------------------------
// Flush goroutine — async batched bbolt writes
// ---------------------------------------------------------------------------

func (w *Worker) flushLoop() {
	defer w.wg.Done()

	ticker := time.NewTicker(w.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-w.ctx.Done():
			return
		case <-ticker.C:
			w.flushDirty()
		}
	}
}

// flushDirty swaps the dirty map and writes all accumulated alerts to bbolt.
func (w *Worker) flushDirty() {
	ctx := w.ctx

	w.dirtyMu.Lock()
	batch := w.dirtyAlerts
	w.dirtyAlerts = make(map[string]*alerts.Alert, len(batch))
	w.dirtyMu.Unlock()

	total := 0
	for id, alert := range batch {
		if putErr := w.alertStore.Put(ctx, alert); putErr != nil {
			log.Printf("notifications worker: flush put %q: %v", id, putErr)
		}
		total++
	}
	if total > 0 && w.aMetrics != nil {
		w.aMetrics.UpdateNotifyQueueDepth(len(w.eventQueue))
	}
}

// ---------------------------------------------------------------------------
// Async delivery goroutine pool
// ---------------------------------------------------------------------------

// deliveryJob is a pending alert delivery.
type deliveryJob struct {
	alert *alerts.Alert
	rule  *rules.Rule
}

// tryDeliverAlert submits a delivery job to the async pool. Non-blocking:
// if the delivery queue is full, the alert will be picked up by the retry
// loop on its next scan.
func (w *Worker) tryDeliverAlert(ctx context.Context, alert *alerts.Alert, rule *rules.Rule) {
	select {
	case w.deliveryQueue <- deliveryJob{alert: alert, rule: rule}:
	default:
		// Delivery pool saturated — retry loop will handle it.
	}
}

func (w *Worker) deliveryLoop() {
	defer w.wg.Done()

	for {
		select {
		case <-w.ctx.Done():
			return
		case job := <-w.deliveryQueue:
			w.deliverAlert(w.ctx, job.alert, job.rule)
		}
	}
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
		return
	}

	w.deliverOnce(ctx, alert, notifier, rule, state, now)
}

// ---------------------------------------------------------------------------
// Retry loop
// ---------------------------------------------------------------------------

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

		alert, err := w.alertStore.Get(ctx, entry.AlertID)
		if err != nil {
			log.Printf("notifications worker: get retryable alert %q: %v", entry.AlertID, err)
			continue
		}
		if alert == nil {
			_ = w.notifStore.DeleteNotificationState(ctx, entry.AlertID)
			continue
		}

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
		if state != nil {
			state.LastAttempt = now
		}
		w.deliverOnce(ctx, alert, notifier, rule, state, now)
	}
}

// ---------------------------------------------------------------------------
// GC loop
// ---------------------------------------------------------------------------

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

func (w *Worker) garbageCollect() {
	ctx := w.ctx

	// 1. Evict resolved alerts older than 24h.
	resolvedAlerts, err := w.alertStore.ListByStatus(ctx, alerts.AlertResolved)
	if err != nil {
		log.Printf("notifications worker: list resolved alerts: %v", err)
	} else {
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

	// 2. Evict idle Pending/Firing alerts that haven't seen an event
	//    within the configured alert idle TTL.
	deletedIdle, err := w.alertStore.DeleteIdleAlerts(ctx, w.alertIdleTTL, w.notifStore)
	if err != nil {
		log.Printf("notifications worker: delete idle alerts: %v", err)
	} else if deletedIdle > 0 {
		log.Printf("notifications worker: gc evicted %d idle alerts", deletedIdle)
	}

	// 3. Purge aged DLQ entries.
	deletedDLQ, err := w.notifStore.PurgeDLQ(ctx, w.dlqRetention)
	if err != nil {
		log.Printf("notifications worker: purge dlq: %v", err)
	} else if deletedDLQ > 0 {
		log.Printf("notifications worker: gc purged %d DLQ entries", deletedDLQ)
	}

	// 4. Periodic bbolt compaction (reclaims freed pages to OS).
	if w.bboltCompactionEnabled {
		now := time.Now()
		if now.Sub(w.lastCompaction) >= w.bboltCompactionInterval {
			w.compactStores(ctx)
			w.lastCompaction = now
		}
	}
}

// compactStores compacts both the alert store and notification state store.
func (w *Worker) compactStores(ctx context.Context) {
	log.Println("notifications worker: starting bbolt compaction")

	if err := w.alertStore.Compact(ctx); err != nil {
		log.Printf("notifications worker: compact alert store: %v", err)
	}

	if err := w.notifStore.Compact(ctx); err != nil {
		log.Printf("notifications worker: compact notif store: %v", err)
	}

	log.Println("notifications worker: bbolt compaction complete")
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// deliverOnce calls notifier.Send and handles retry scheduling / DLQ.
// state may be nil; it is initialized on demand.
func (w *Worker) deliverOnce(
	ctx context.Context,
	alert *alerts.Alert,
	notifier Notifier,
	rule *rules.Rule,
	state *NotificationState,
	now int64,
) {
	err := notifier.Send(ctx, alert)
	if err != nil {
		log.Printf("notifications worker: send alert %q to %q failed: %v", alert.ID, rule.Destination, err)

		if w.aMetrics != nil {
			w.aMetrics.IncrementNotifyEventsFailed(rule.Destination)
		}
		if !IsRetryable(err) {
			w.handleNonRetryable(ctx, alert, err)
			return
		}
		if state == nil {
			state = &NotificationState{}
		}
		state.LastAttempt = now
		state.RetryCount++
		state.NextRetry = now + backoffDuration(rule.RetryBackoff, state.RetryCount)

		if state.RetryCount >= rule.MaxRetries {
			w.handleDeadLetter(ctx, alert, state, err)
			return
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
	}

	if putErr := w.notifStore.PutNotificationState(ctx, alert.ID, state); putErr != nil {
		log.Printf("notifications worker: put notification state %q: %v", alert.ID, putErr)
	}
}

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

func (w *Worker) findRule(name string) *rules.Rule {
	rs := w.engine.Rules()
	for i := range rs {
		if rs[i].Name == name {
			return &rs[i]
		}
	}
	return nil
}

func backoffDuration(base time.Duration, attempt int) int64 {
	if attempt <= 0 {
		return int64(base)
	}
	shift := min(attempt-1, 10)
	return int64(base) * (1 << shift)
}
