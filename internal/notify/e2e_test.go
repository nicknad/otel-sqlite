package notify

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"codeberg.org/nicknad/otel-sqlite/internal/model"
)

// e2eReceiver is an in-process webhook receiver that records received
// payloads. It exposes the same API as cmd/webhook-receiver so the E2E
// test exercises the full pipeline: Worker → RuleEngine → HTTPNotifier → receiver.
type e2eReceiver struct {
	mu       sync.Mutex
	received []map[string]any
}

func newE2EReceiver() *e2eReceiver {
	return &e2eReceiver{
		received: make([]map[string]any, 0),
	}
}

// handler returns an http.Handler that mimics the webhook-receiver.
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
	mux.HandleFunc("GET /received", func(w http.ResponseWriter, req *http.Request) {
		r.mu.Lock()
		defer r.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(r.received) //nolint:errcheck
	})
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("POST /reset", func(w http.ResponseWriter, req *http.Request) {
		r.mu.Lock()
		r.received = r.received[:0]
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

// TestE2E_WorkerToWebhook validates the full notification pipeline:
// Worker → RuleEngine → HTTPNotifier → webhook receiver.
func TestE2E_WorkerToWebhook(t *testing.T) {
	// --- Setup: webhook receiver ---
	recv := newE2EReceiver()
	srv := httptest.NewServer(recv.handler())
	defer srv.Close()

	// --- Setup: bbolt store ---
	storePath := filepath.Join(t.TempDir(), "e2e-notify.db")
	store, err := NewBboltStore(storePath)
	if err != nil {
		t.Fatalf("NewBboltStore: %v", err)
	}
	defer store.Close()

	// --- Setup: rule engine ---
	engine, err := NewRuleEngine([]Rule{
		{
			Name:          "e2e-errors",
			MatchSeverity: model.SeverityError,
			Cooldown:      0, // no cooldown so every event fires
			MaxRetries:    3,
			RetryBackoff:  10 * time.Millisecond,
			Destination:   "http",
		},
	})
	if err != nil {
		t.Fatalf("NewRuleEngine: %v", err)
	}

	// --- Setup: HTTP notifier pointing at test server ---
	httpNotifier := NewHTTPNotifier(&HTTPNotifierConfig{
		URL:     srv.URL + "/webhook",
		Timeout: 2 * time.Second,
	})

	// --- Setup: worker ---
	w := NewWorker(&WorkerConfig{
		Store:           store,
		Engine:          engine,
		Notifiers:       map[string]Notifier{"http": httpNotifier},
		EventQueueDepth: 50,
		RetryInterval:   500 * time.Millisecond,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w.Start(ctx)
	defer w.Stop()

	// --- Send several events ---
	events := []*Event{
		{
			Severity: model.SeverityError, Body: "disk full on /dev/sda1", ResourceID: "host-1",
			Attributes: []model.Attribute{
				{Key: "host.name", Str: "host-1", Kind: model.ValueString},
			},
		},
		{
			Severity: model.SeverityError, Body: "connection refused to db", ResourceID: "host-2",
			Attributes: []model.Attribute{
				{Key: "service.name", Str: "auth-svc", Kind: model.ValueString},
			},
		},
		{
			Severity: model.SeverityFatal, Body: "kernel panic", ResourceID: "host-3",
			SeverityText: "FATAL",
		},
	}

	for _, ev := range events {
		if err := w.Send(context.Background(), ev); err != nil {
			t.Fatalf("Send: %v", err)
		}
	}

	// --- Wait for delivery ---
	deadline := time.Now().Add(3 * time.Second)
	for recv.count() < len(events) && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}

	// --- Assertions ---
	if got := recv.count(); got != len(events) {
		t.Fatalf("received %d notifications, want %d", got, len(events))
	}

	// Verify first event.
	last := recv.last()
	if last["body"] != "kernel panic" {
		t.Errorf("last body = %v, want %q", last["body"], "kernel panic")
	}
	if last["severity"] != "FATAL" {
		t.Errorf("last severity = %v, want %q", last["severity"], "FATAL")
	}
	if last["severity_text"] != "FATAL" {
		t.Errorf("last severity_text = %v, want %q", last["severity_text"], "FATAL")
	}
}

// TestE2E_CooldownSuppression validates that cooldown suppresses
// repeated notifications within the cooldown window.
func TestE2E_CooldownSuppression(t *testing.T) {
	recv := newE2EReceiver()
	srv := httptest.NewServer(recv.handler())
	defer srv.Close()

	storePath := filepath.Join(t.TempDir(), "e2e-cooldown.db")
	store, err := NewBboltStore(storePath)
	if err != nil {
		t.Fatalf("NewBboltStore: %v", err)
	}
	defer store.Close()

	engine, err := NewRuleEngine([]Rule{
		{
			Name:          "cooldown-rule",
			MatchSeverity: model.SeverityError,
			Cooldown:      2 * time.Second, // long cooldown
			MaxRetries:    1,
			Destination:   "http",
		},
	})
	if err != nil {
		t.Fatalf("NewRuleEngine: %v", err)
	}

	httpNotifier := NewHTTPNotifier(&HTTPNotifierConfig{
		URL:     srv.URL + "/webhook",
		Timeout: 2 * time.Second,
	})

	w := NewWorker(&WorkerConfig{
		Store:           store,
		Engine:          engine,
		Notifiers:       map[string]Notifier{"http": httpNotifier},
		EventQueueDepth: 50,
		RetryInterval:   500 * time.Millisecond,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w.Start(ctx)
	defer w.Stop()

	// Send the same event twice in quick succession.
	ev := &Event{Severity: model.SeverityError, Body: "repeated error", ResourceID: "res-1"}
	w.Send(context.Background(), ev)
	time.Sleep(50 * time.Millisecond)
	w.Send(context.Background(), ev)
	time.Sleep(50 * time.Millisecond)

	// Only the first should be delivered; second is suppressed by cooldown.
	if recv.count() != 1 {
		t.Errorf("expected 1 notification (cooldown suppression), got %d", recv.count())
	}
}

// TestE2E_DedupSuppression validates that identical events within the
// dedup window are suppressed.
func TestE2E_DedupSuppression(t *testing.T) {
	recv := newE2EReceiver()
	srv := httptest.NewServer(recv.handler())
	defer srv.Close()

	storePath := filepath.Join(t.TempDir(), "e2e-dedup.db")
	store, err := NewBboltStore(storePath)
	if err != nil {
		t.Fatalf("NewBboltStore: %v", err)
	}
	defer store.Close()

	engine, err := NewRuleEngine([]Rule{
		{
			Name:          "dedup-rule",
			MatchSeverity: model.SeverityError,
			Cooldown:      0,
			DedupWindow:   5 * time.Second,
			MaxRetries:    1,
			Destination:   "http",
		},
	})
	if err != nil {
		t.Fatalf("NewRuleEngine: %v", err)
	}

	httpNotifier := NewHTTPNotifier(&HTTPNotifierConfig{
		URL:     srv.URL + "/webhook",
		Timeout: 2 * time.Second,
	})

	w := NewWorker(&WorkerConfig{
		Store:           store,
		Engine:          engine,
		Notifiers:       map[string]Notifier{"http": httpNotifier},
		EventQueueDepth: 50,
		RetryInterval:   500 * time.Millisecond,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w.Start(ctx)
	defer w.Stop()

	// Send identical events.
	ev := &Event{Severity: model.SeverityError, Body: "duplicate", ResourceID: "res-1"}
	w.Send(context.Background(), ev)
	time.Sleep(50 * time.Millisecond)
	w.Send(context.Background(), ev) // same fingerprint → dedup'd
	time.Sleep(50 * time.Millisecond)

	// Different body → not dedup'd.
	ev2 := &Event{Severity: model.SeverityError, Body: "different", ResourceID: "res-1"}
	w.Send(context.Background(), ev2)
	time.Sleep(50 * time.Millisecond)

	if recv.count() != 2 {
		t.Errorf("expected 2 notifications (one dedup suppressed), got %d", recv.count())
	}
}

// TestE2E_RetryAndDLQ validates that a failing notifier triggers retries
// and eventually moves the event to the dead-letter queue.
func TestE2E_RetryAndDLQ(t *testing.T) {
	// A receiver that fails N times then succeeds.
	failingRecv := &failingReceiver{failCount: 3}
	srv := httptest.NewServer(failingRecv.handler())
	defer srv.Close()

	storePath := filepath.Join(t.TempDir(), "e2e-retry.db")
	store, err := NewBboltStore(storePath)
	if err != nil {
		t.Fatalf("NewBboltStore: %v", err)
	}
	defer store.Close()

	engine, err := NewRuleEngine([]Rule{
		{
			Name:          "retry-rule",
			MatchSeverity: model.SeverityError,
			Cooldown:      0,
			MaxRetries:    3,
			RetryBackoff:  10 * time.Millisecond,
			Destination:   "http",
		},
	})
	if err != nil {
		t.Fatalf("NewRuleEngine: %v", err)
	}

	httpNotifier := NewHTTPNotifier(&HTTPNotifierConfig{
		URL:     srv.URL + "/webhook",
		Timeout: 2 * time.Second,
	})

	w := NewWorker(&WorkerConfig{
		Store:           store,
		Engine:          engine,
		Notifiers:       map[string]Notifier{"http": httpNotifier},
		EventQueueDepth: 50,
		RetryInterval:   50 * time.Millisecond,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w.Start(ctx)

	// Pre-populate: already 2 failed retries, next retry due now.
	store.PutState(context.Background(), "retry-rule:res-1:abc", &NotificationState{
		RetryCount:  2,
		NextRetry:   time.Now().UnixNano() - int64(time.Second),
		EventDigest: "abc",
	})

	// Wait for retry loop to attempt delivery (which will fail again).
	time.Sleep(300 * time.Millisecond)
	w.Stop()

	// Should be dead-lettered after the 3rd consecutive failure.
	dlqEntries, err := store.ListDLQ(context.Background())
	if err != nil {
		t.Fatalf("ListDLQ: %v", err)
	}
	if len(dlqEntries) != 1 {
		t.Fatalf("expected 1 DLQ entry, got %d", len(dlqEntries))
	}
	if dlqEntries[0].FailReason == "" {
		t.Error("DLQ entry should have a fail reason")
	}

	// Clean up temp file.
	os.Remove(storePath)
}

// failingReceiver returns 500 for the first N requests, then 200.
type failingReceiver struct {
	mu        sync.Mutex
	failCount int
	attempts  int
}

func (f *failingReceiver) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /webhook", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.attempts++
		attempt := f.attempts
		shouldFail := attempt <= f.failCount
		f.mu.Unlock()
		if shouldFail {
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	return mux
}
