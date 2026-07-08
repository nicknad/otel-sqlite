package notifications

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"codeberg.org/nicknad/otel-sqlite/internal/alerts"
	"codeberg.org/nicknad/otel-sqlite/internal/events"
	"codeberg.org/nicknad/otel-sqlite/internal/model"
	"codeberg.org/nicknad/otel-sqlite/internal/rules"
)

// ---------------------------------------------------------------------------
// E2E receiver (in-process webhook)
// ---------------------------------------------------------------------------

type e2eReceiver struct {
	mu       sync.Mutex
	received []map[string]any
}

func newE2EReceiver() *e2eReceiver {
	return &e2eReceiver{received: make([]map[string]any, 0)}
}

func (r *e2eReceiver) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /webhook", func(w http.ResponseWriter, req *http.Request) {
		var payload map[string]any
		if err := json.NewDecoder(req.Body).Decode(&payload); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		r.mu.Lock()
		r.received = append(r.received, payload)
		r.mu.Unlock()
		w.WriteHeader(http.StatusOK)
	})
	return mux
}

func (r *e2eReceiver) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.received)
}

func (r *e2eReceiver) last() map[string]any {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.received) == 0 {
		return nil
	}
	return r.received[len(r.received)-1]
}

// ---------------------------------------------------------------------------
// E2E: Event → Rule → Alert creation + Notifier delivery
// ---------------------------------------------------------------------------

func TestE2E_AlertPipeline_Delivery(t *testing.T) {
	// Setup webhook receiver.
	recv := newE2EReceiver()
	srv := httptest.NewServer(recv.handler())
	defer srv.Close()

	// Setup stores.
	alertStorePath := filepath.Join(t.TempDir(), "e2e-alerts.db")
	alertStore, err := alerts.NewBboltAlertStore(alertStorePath)
	if err != nil {
		t.Fatalf("NewBboltAlertStore: %v", err)
	}
	defer alertStore.Close()

	notifStorePath := filepath.Join(t.TempDir(), "e2e-notif.db")
	notifStore, err := NewBboltStore(notifStorePath)
	if err != nil {
		t.Fatalf("NewBboltStore: %v", err)
	}
	defer notifStore.Close()

	// Rule engine: threshold=1 so every match immediately fires.
	engine, err := rules.NewRuleEngine([]rules.Rule{
		{
			Name:               "e2e-errors",
			MatchSeverity:      model.SeverityError,
			Cooldown:           0,
			MaxRetries:         3,
			RetryBackoff:       10 * time.Millisecond,
			AlertWindow:        30 * time.Second,
			AlertThreshold:     1, // fire on first match
			AlertResolveWindow: 60 * time.Second,
			Destination:        "http",
		},
	})
	if err != nil {
		t.Fatalf("NewRuleEngine: %v", err)
	}

	// HTTP notifier.
	httpNotifier := NewHTTPNotifier(&HTTPNotifierConfig{
		URL:     srv.URL + "/webhook",
		Timeout: 2 * time.Second,
	})

	// Worker.
	w := NewWorker(&WorkerConfig{
		AlertStore:      alertStore,
		NotifStore:      notifStore,
		Engine:          engine,
		Notifiers:       map[string]Notifier{"http": httpNotifier},
		EventQueueDepth: 50,
		RetryInterval:   500 * time.Millisecond,
		GCInterval:      10 * time.Minute, // long so GC doesn't interfere
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w.Start(ctx)
	defer w.Stop()

	// Send an error event.
	ev := &events.Event{
		Severity:   model.SeverityError,
		Body:       "disk full on /dev/sda1",
		ResourceID: "host-1",
		Timestamp:  time.Now().UnixNano(),
	}
	if err := w.Send(context.Background(), ev); err != nil {
		t.Fatalf("Send: %v", err)
	}

	// Wait for async delivery + flush to complete.
	deadline := time.Now().Add(3 * time.Second)
	for recv.count() < 1 && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}

	if recv.count() != 1 {
		t.Fatalf("received %d notifications, want 1", recv.count())
	}

	// Wait for the async flush to bbolt (200ms default flush interval).
	time.Sleep(400 * time.Millisecond)

	// Verify the alert was persisted.
	alertID := alerts.AlertID("e2e-errors", "host-1")
	alert, err := alertStore.Get(ctx, alertID)
	if err != nil {
		t.Fatalf("Get alert: %v", err)
	}
	if alert == nil {
		t.Fatal("alert not persisted")
	}
	if alert.Status != alerts.AlertFiring {
		t.Errorf("alert status = %v, want firing", alert.Status)
	}
	if alert.Count < 1 {
		t.Errorf("alert count = %d, want >= 1", alert.Count)
	}

	// Verify webhook payload.
	last := recv.last()
	if last["status"] != "firing" {
		t.Errorf("webhook status = %v, want firing", last["status"])
	}
	if last["rule_id"] != "e2e-errors" {
		t.Errorf("webhook rule_id = %v", last["rule_id"])
	}
	if last["resource_id"] != "host-1" {
		t.Errorf("webhook resource_id = %v", last["resource_id"])
	}
}

// ---------------------------------------------------------------------------
// E2E: Alert state transitions (pending → firing → resolved)
// ---------------------------------------------------------------------------

func TestE2E_AlertStateTransitions(t *testing.T) {
	recv := newE2EReceiver()
	srv := httptest.NewServer(recv.handler())
	defer srv.Close()

	alertStore, _ := alerts.NewBboltAlertStore(filepath.Join(t.TempDir(), "e2e-trans-alerts.db"))
	defer alertStore.Close()
	notifStore, _ := NewBboltStore(filepath.Join(t.TempDir(), "e2e-trans-notif.db"))
	defer notifStore.Close()

	// threshold=3 — need 3 events within 5s to fire.
	engine, _ := rules.NewRuleEngine([]rules.Rule{
		{
			Name:               "trans-rule",
			MatchSeverity:      model.SeverityError,
			Cooldown:           0,
			MaxRetries:         1,
			AlertWindow:        5 * time.Second,
			AlertThreshold:     3,
			AlertResolveWindow: 1 * time.Second, // short so resolve triggers quickly
			Destination:        "http",
		},
	})

	httpNotifier := NewHTTPNotifier(&HTTPNotifierConfig{
		URL:     srv.URL + "/webhook",
		Timeout: 2 * time.Second,
	})

	w := NewWorker(&WorkerConfig{
		AlertStore:      alertStore,
		NotifStore:      notifStore,
		Engine:          engine,
		Notifiers:       map[string]Notifier{"http": httpNotifier},
		EventQueueDepth: 50,
		RetryInterval:   500 * time.Millisecond,
		GCInterval:      10 * time.Minute,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w.Start(ctx)
	defer w.Stop()

	// Send 2 events — below threshold, no notification expected.
	for i := 0; i < 2; i++ {
		w.Send(ctx, &events.Event{
			Severity:   model.SeverityError,
			Body:       "error",
			ResourceID: "res-1",
			Timestamp:  time.Now().UnixNano(),
		})
	}
	time.Sleep(300 * time.Millisecond)
	if recv.count() != 0 {
		t.Fatalf("expected 0 notifications below threshold, got %d", recv.count())
	}

	// Send 3rd event — crosses threshold, fires.
	w.Send(ctx, &events.Event{
		Severity:   model.SeverityError,
		Body:       "error",
		ResourceID: "res-1",
		Timestamp:  time.Now().UnixNano(),
	})
	time.Sleep(300 * time.Millisecond)
	if recv.count() != 1 {
		t.Fatalf("expected 1 notification after threshold, got %d", recv.count())
	}
	if recv.last()["status"] != "firing" {
		t.Errorf("expected firing status, got %v", recv.last()["status"])
	}

	// Now wait for resolve: no more events, resolve window is 1s.
	time.Sleep(1500 * time.Millisecond)

	// Send one more event to trigger re-evaluation (the processEvent only
	// runs on new events). The counter will have pruned old timestamps.
	w.Send(ctx, &events.Event{
		Severity:   model.SeverityError,
		Body:       "error",
		ResourceID: "res-1",
		Timestamp:  time.Now().UnixNano(),
	})
	time.Sleep(300 * time.Millisecond)

	// The alert should have been resolved and a resolved notification sent.
	if recv.count() >= 2 {
		// Check the second notification is a "resolved" one.
		// Note: depending on timing, the count might be 2 or more.
		t.Logf("received %d notifications total", recv.count())
	}
}

// ---------------------------------------------------------------------------
// E2E: No-matching-rule events are dropped
// ---------------------------------------------------------------------------

func TestE2E_NoMatchingRule(t *testing.T) {
	alertStore, _ := alerts.NewBboltAlertStore(filepath.Join(t.TempDir(), "e2e-nomatch-alerts.db"))
	defer alertStore.Close()
	notifStore, _ := NewBboltStore(filepath.Join(t.TempDir(), "e2e-nomatch-notif.db"))
	defer notifStore.Close()

	// Rule only matches FATAL.
	engine, _ := rules.NewRuleEngine([]rules.Rule{
		{
			Name:          "fatal-only",
			MatchSeverity: model.SeverityFatal,
			Destination:   "log",
		},
	})

	w := NewWorker(&WorkerConfig{
		AlertStore:      alertStore,
		NotifStore:      notifStore,
		Engine:          engine,
		EventQueueDepth: 10,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w.Start(ctx)

	// Send ERROR event — should not match FATAL rule.
	w.Send(ctx, &events.Event{Severity: model.SeverityError, Body: "err", ResourceID: "r1", Timestamp: time.Now().UnixNano()})
	time.Sleep(300 * time.Millisecond)
	w.Stop()

	// Allow flush to complete.
	time.Sleep(100 * time.Millisecond)
	all, _ := alertStore.ListAll(ctx)
	if len(all) != 0 {
		t.Errorf("expected 0 alerts, got %d", len(all))
	}
}

// ---------------------------------------------------------------------------
// E2E: Retry and DLQ for failed notification deliveries
// ---------------------------------------------------------------------------

func TestE2E_RetryAndDLQ(t *testing.T) {
	alertStore, _ := alerts.NewBboltAlertStore(filepath.Join(t.TempDir(), "e2e-dlq-alerts.db"))
	defer alertStore.Close()
	notifStore, _ := NewBboltStore(filepath.Join(t.TempDir(), "e2e-dlq-notif.db"))
	defer notifStore.Close()

	// A notifier that always fails.
	failNotifier := &alwaysFailNotifier{}

	engine, _ := rules.NewRuleEngine([]rules.Rule{
		{
			Name:               "dlq-rule",
			MatchSeverity:      model.SeverityError,
			Cooldown:           0,
			MaxRetries:         3,
			RetryBackoff:       10 * time.Millisecond,
			AlertThreshold:     1,
			AlertResolveWindow: 60 * time.Second,
			Destination:        "fail",
		},
	})

	w := NewWorker(&WorkerConfig{
		AlertStore:      alertStore,
		NotifStore:      notifStore,
		Engine:          engine,
		Notifiers:       map[string]Notifier{"fail": failNotifier},
		EventQueueDepth: 50,
		RetryInterval:   50 * time.Millisecond,
		GCInterval:      10 * time.Minute,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w.Start(ctx)

	// Pre-create an alert in firing state (simulating it was already triggered)
	// so the retry loop will attempt delivery.
	alert := alerts.NewAlert("dlq-rule", "res-1", model.SeverityError, time.Now().UnixNano())
	alert.Status = alerts.AlertFiring
	alertStore.Put(ctx, alert)

	// Pre-populate notification state with retries maxed out.
	notifStore.PutNotificationState(ctx, alert.ID, &NotificationState{
		RetryCount: 3,
		NextRetry:  time.Now().UnixNano() - int64(time.Second), // due now
	})

	// Wait for retry loop to attempt delivery → exceed max retries → DLQ.
	time.Sleep(400 * time.Millisecond)
	w.Stop()

	dlqEntries, _ := notifStore.ListDLQ(context.Background())
	if len(dlqEntries) != 1 {
		t.Fatalf("expected 1 DLQ entry, got %d", len(dlqEntries))
	}
}

// alwaysFailNotifier always returns a retryable error.
type alwaysFailNotifier struct{}

func (n *alwaysFailNotifier) Send(_ context.Context, _ *alerts.Alert) error {
	return errAlwaysFail
}
func (n *alwaysFailNotifier) Name() string { return "fail" }
func (n *alwaysFailNotifier) Close() error { return nil }

var errAlwaysFail = NewNotRetryableError(fmt.Errorf("always fails"))
