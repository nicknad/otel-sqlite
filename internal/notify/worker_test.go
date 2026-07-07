package notify

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// mockStore is an in-memory Store implementation for testing.
type mockStore struct {
	mu     sync.Mutex
	states map[string]*NotificationState
	dlq    []*DLQEntry
}

func newMockStore() *mockStore {
	return &mockStore{
		states: make(map[string]*NotificationState),
	}
}

func (m *mockStore) GetState(_ context.Context, key string) (*NotificationState, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.states[key]
	if !ok {
		return nil, ErrNotFound
	}
	// Return a copy.
	cpy := *s
	return &cpy, nil
}

func (m *mockStore) PutState(_ context.Context, key string, state *NotificationState) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cpy := *state
	m.states[key] = &cpy
	return nil
}

func (m *mockStore) DeleteState(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.states, key)
	return nil
}

func (m *mockStore) EnqueueDLQ(_ context.Context, event *Event, reason string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.dlq = append(m.dlq, &DLQEntry{
		ID:         "dlq-1",
		Event:      event,
		FailReason: reason,
		FailedAt:   time.Now().UnixNano(),
	})
	return nil
}

func (m *mockStore) ListDLQ(_ context.Context) ([]*DLQEntry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.dlq, nil
}

func (m *mockStore) AckDLQ(_ context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	var filtered []*DLQEntry
	for _, e := range m.dlq {
		if e.ID != id {
			filtered = append(filtered, e)
		}
	}
	m.dlq = filtered
	return nil
}

func (m *mockStore) ScanRetryable(_ context.Context) ([]RetryableEntry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now().UnixNano()
	var entries []RetryableEntry
	for k, s := range m.states {
		if s.RetryCount > 0 && s.NextRetry > 0 && s.NextRetry <= now {
			entries = append(entries, RetryableEntry{Key: k, RetryCount: s.RetryCount})
		}
	}
	return entries, nil
}

func (m *mockStore) Close() error { return nil }

// mockRuleEngine always matches with a fixed rule.
type mockRuleEngine struct {
	rule Rule
	key  string
}

func (m *mockRuleEngine) Evaluate(_ context.Context, event *Event) (*Rule, string, bool) {
	return &m.rule, m.key, true
}

func (m *mockRuleEngine) Rules() []Rule {
	return []Rule{m.rule}
}

// mockNotifier records sent events.
type mockNotifier struct {
	mu     sync.Mutex
	sent   []*Event
	failOn int // fail on the Nth send (0 = never fail)
	count  int
}

func (m *mockNotifier) Send(_ context.Context, event *Event) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.count++
	m.sent = append(m.sent, event)
	if m.failOn > 0 && m.count == m.failOn {
		return errors.New("mock send failure")
	}
	return nil
}

func (m *mockNotifier) Name() string   { return "mock" }
func (m *mockNotifier) Close() error   { return nil }
func (m *mockNotifier) Sent() []*Event { m.mu.Lock(); defer m.mu.Unlock(); return m.sent }

func TestWorker_SendAndProcess(t *testing.T) {
	store := newMockStore()
	notifier := &mockNotifier{}
	engine := &mockRuleEngine{
		rule: Rule{
			Name:        "test-rule",
			Cooldown:    0,
			MaxRetries:  3,
			Destination: "mock",
		},
		key: "test-rule:res1:fp1",
	}

	w := NewWorker(&WorkerConfig{
		Store:           store,
		Engine:          engine,
		Notifiers:       map[string]Notifier{"mock": notifier},
		EventQueueDepth: 10,
		RetryInterval:   10 * time.Second,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w.Start(ctx)

	event := &Event{Severity: 17, Body: "test error", ResourceID: "res1"}
	err := w.Send(context.Background(), event)
	if err != nil {
		t.Fatalf("Send: %v", err)
	}

	// Wait a bit for processing.
	time.Sleep(100 * time.Millisecond)
	w.Stop()

	sent := notifier.Sent()
	if len(sent) != 1 {
		t.Fatalf("expected 1 sent event, got %d", len(sent))
	}
	if sent[0].Body != "test error" {
		t.Errorf("body = %q, want %q", sent[0].Body, "test error")
	}
}

func TestWorker_Cooldown(t *testing.T) {
	store := newMockStore()
	notifier := &mockNotifier{}
	engine := &mockRuleEngine{
		rule: Rule{
			Name:         "test-rule",
			Cooldown:     5 * time.Minute,
			MaxRetries:   3,
			RetryBackoff: 1 * time.Second,
			Destination:  "mock",
		},
		key: "test-rule:res1:fp1",
	}

	w := NewWorker(&WorkerConfig{
		Store:           store,
		Engine:          engine,
		Notifiers:       map[string]Notifier{"mock": notifier},
		EventQueueDepth: 10,
		RetryInterval:   500 * time.Millisecond,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w.Start(ctx)

	// First event — should be delivered.
	event := &Event{Severity: 17, Body: "error 1", ResourceID: "res1"}
	w.Send(context.Background(), event)
	time.Sleep(100 * time.Millisecond)

	// Second event — should be suppressed by cooldown.
	w.Send(context.Background(), event)
	time.Sleep(100 * time.Millisecond)

	w.Stop()

	sent := notifier.Sent()
	if len(sent) != 1 {
		t.Errorf("expected 1 sent event (cooldown suppressed), got %d", len(sent))
	}
}

func TestWorker_NoMatchingRule(t *testing.T) {
	// An engine that never matches.
	noMatch := &noMatchEngine{}
	store := newMockStore()
	notifier := &mockNotifier{}

	w := NewWorker(&WorkerConfig{
		Store:           store,
		Engine:          noMatch,
		Notifiers:       map[string]Notifier{"mock": notifier},
		EventQueueDepth: 10,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w.Start(ctx)

	event := &Event{Severity: 17, Body: "test"}
	w.Send(context.Background(), event)
	time.Sleep(50 * time.Millisecond)
	w.Stop()

	if len(notifier.Sent()) != 0 {
		t.Error("expected no events to be delivered")
	}
}

func TestWorker_QueueFull(t *testing.T) {
	store := newMockStore()
	w := NewWorker(&WorkerConfig{
		Store:           store,
		Engine:          &noMatchEngine{},
		EventQueueDepth: 1,
	})

	ctx := context.Background()
	// Fill the queue.
	w.Send(ctx, &Event{Body: "ev1"})
	// Next send should fail.
	err := w.Send(ctx, &Event{Body: "ev2"})
	if !errors.Is(err, ErrQueueFull) {
		t.Errorf("expected ErrQueueFull, got %v", err)
	}
}

func TestWorker_RetryDLQ(t *testing.T) {
	store := newMockStore()
	// Notifier always fails.
	notifier := &mockNotifier{failOn: 1}
	engine := &mockRuleEngine{
		rule: Rule{
			Name:         "test-rule",
			MaxRetries:   3,
			RetryBackoff: 10 * time.Millisecond,
			Destination:  "mock",
		},
		key: "test-rule:res1:fp1",
	}

	// Pre-populate state: already 2 failed attempts, retry due now.
	store.PutState(context.Background(), "test-rule:res1:fp1", &NotificationState{
		RetryCount: 2,
		NextRetry:  time.Now().UnixNano() - int64(time.Second),
	})

	w := NewWorker(&WorkerConfig{
		Store:           store,
		Engine:          engine,
		Notifiers:       map[string]Notifier{"mock": notifier},
		EventQueueDepth: 10,
		RetryInterval:   50 * time.Millisecond,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w.Start(ctx)

	// Wait for retry loop to run and fail.
	time.Sleep(200 * time.Millisecond)
	w.Stop()

	// Should be dead-lettered.
	dlqEntries, _ := store.ListDLQ(context.Background())
	if len(dlqEntries) != 1 {
		t.Fatalf("expected 1 DLQ entry, got %d", len(dlqEntries))
	}
	if dlqEntries[0].FailReason != "mock send failure" {
		t.Errorf("fail reason = %q", dlqEntries[0].FailReason)
	}
}

// noMatchEngine always returns "no match".
type noMatchEngine struct{}

func (n *noMatchEngine) Evaluate(_ context.Context, _ *Event) (*Rule, string, bool) {
	return nil, "", false
}
func (n *noMatchEngine) Rules() []Rule { return nil }

// ---------------------------------------------------------------------------
// Regression tests for fixes
// ---------------------------------------------------------------------------

func TestWorker_StoredEventOnRetry(t *testing.T) {
	// The retry loop must send the original StoredEvent body, not the key string.
	store := newMockStore()
	notifier := &mockNotifier{failOn: 1} // always fails on first attempt
	engine := &mockRuleEngine{
		rule: Rule{
			Name:         "stored-rule",
			MaxRetries:   3,
			RetryBackoff: 10 * time.Millisecond,
			Destination:  "mock",
		},
		key: "stored-rule:res1:fp1",
	}

	originalEvent := &Event{
		Severity:   17,
		Body:       "real error message",
		ResourceID: "res1",
	}

	// Pre-populate state with StoredEvent and retry due now.
	store.PutState(context.Background(), "stored-rule:res1:fp1", &NotificationState{
		RetryCount:  1,
		NextRetry:   time.Now().UnixNano() - int64(time.Second),
		StoredEvent: originalEvent,
	})

	w := NewWorker(&WorkerConfig{
		Store:           store,
		Engine:          engine,
		Notifiers:       map[string]Notifier{"mock": notifier},
		EventQueueDepth: 10,
		RetryInterval:   50 * time.Millisecond,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w.Start(ctx)

	time.Sleep(200 * time.Millisecond)
	w.Stop()

	// The notifier should have received the original event body, not the key.
	sent := notifier.Sent()
	if len(sent) == 0 {
		t.Fatal("expected at least 1 send attempt")
	}
	lastEvent := sent[len(sent)-1]
	if lastEvent.Body != "real error message" {
		t.Errorf("retry body = %q, want %q (should use StoredEvent, not key string)",
			lastEvent.Body, "real error message")
	}
	if lastEvent.ResourceID != "res1" {
		t.Errorf("retry resource_id = %q, want %q", lastEvent.ResourceID, "res1")
	}
}

func TestWorker_NonRetryableGoesStraightToDLQ(t *testing.T) {
	// 4xx errors should go directly to DLQ without retrying.
	store := newMockStore()
	notifier := &nonRetryableNotifier{}
	engine := &mockRuleEngine{
		rule: Rule{
			Name:         "nr-rule",
			MaxRetries:   3,
			RetryBackoff: 10 * time.Millisecond,
			Destination:  "mock",
			Cooldown:     0,
		},
		key: "nr-rule:res1:fp1",
	}

	w := NewWorker(&WorkerConfig{
		Store:           store,
		Engine:          engine,
		Notifiers:       map[string]Notifier{"mock": notifier},
		EventQueueDepth: 10,
		RetryInterval:   500 * time.Millisecond,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w.Start(ctx)

	event := &Event{Severity: 17, Body: "unauthorized", ResourceID: "res1"}
	w.Send(context.Background(), event)
	time.Sleep(200 * time.Millisecond)
	w.Stop()

	// Should be in DLQ immediately (no retries).
	dlq, _ := store.ListDLQ(context.Background())
	if len(dlq) != 1 {
		t.Fatalf("expected 1 DLQ entry, got %d", len(dlq))
	}
	// State should be deleted (not pending retry).
	_, err := store.GetState(context.Background(), "nr-rule:res1:fp1")
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("state should be deleted after non-retryable, got err=%v", err)
	}
}

func TestWorker_QueueDrainOnShutdown(t *testing.T) {
	// Events already buffered in the channel must be processed during Stop().
	store := newMockStore()
	notifier := &mockNotifier{}
	engine := &mockRuleEngine{
		rule: Rule{
			Name:        "drain-rule",
			Cooldown:    0,
			MaxRetries:  1,
			Destination: "mock",
		},
		key: "drain-rule:res1:fp1",
	}

	w := NewWorker(&WorkerConfig{
		Store:           store,
		Engine:          engine,
		Notifiers:       map[string]Notifier{"mock": notifier},
		EventQueueDepth: 100,
		RetryInterval:   10 * time.Second,
	})

	ctx, cancel := context.WithCancel(context.Background())
	w.Start(ctx)

	// Enqueue several events, then immediately stop.
	for i := 0; i < 5; i++ {
		w.Send(context.Background(), &Event{Severity: 17, Body: "msg", ResourceID: "res1"})
	}
	cancel()
	w.Stop() // should drain and process all 5

	sent := notifier.Sent()
	if len(sent) != 5 {
		t.Errorf("expected 5 events drained and delivered, got %d", len(sent))
	}
}

func TestWorker_CheckCooldown(t *testing.T) {
	w := &Worker{}

	// Past cooldown.
	if !w.checkCooldown(&NotificationState{CooldownUntil: 0}, 100) {
		t.Error("cooldownUntil=0 should pass")
	}
	if !w.checkCooldown(&NotificationState{CooldownUntil: 50}, 100) {
		t.Error("cooldownUntil=50, now=100 should pass")
	}

	// Active cooldown.
	if w.checkCooldown(&NotificationState{CooldownUntil: 200}, 100) {
		t.Error("cooldownUntil=200, now=100 should block")
	}
}

func TestWorker_CheckDedup(t *testing.T) {
	w := &Worker{}
	rule := &Rule{DedupWindow: 1 * time.Second}

	// No previous event.
	event := &Event{Body: "error"}
	state := &NotificationState{LastAttempt: 0, EventDigest: ""}
	now := int64(time.Second)
	if !w.checkDedup(state, rule, event, now) {
		t.Error("first event should pass dedup")
	}
	if state.EventDigest == "" {
		t.Error("EventDigest should be set after check")
	}

	// Same fingerprint within window → blocked.
	event2 := &Event{Body: "error"} // same body → same fingerprint
	state2 := &NotificationState{
		LastAttempt: now,
		EventDigest: event.Fingerprint(),
	}
	now2 := now + int64(500*time.Millisecond) // within 1s window
	if w.checkDedup(state2, rule, event2, now2) {
		t.Error("duplicate within window should be blocked")
	}

	// Same fingerprint outside window → passes.
	now3 := now + int64(2*time.Second) // outside 1s window
	if !w.checkDedup(state2, rule, event2, now3) {
		t.Error("duplicate outside window should pass")
	}

	// No dedup window → always passes.
	ruleNoDedup := &Rule{DedupWindow: 0}
	state3 := &NotificationState{LastAttempt: now, EventDigest: event.Fingerprint()}
	if !w.checkDedup(state3, ruleNoDedup, event, now+1) {
		t.Error("dedup window=0 should always pass")
	}
}

func TestWorker_CheckRateLimit(t *testing.T) {
	w := &Worker{}
	rule := &Rule{RateLimit: 3, RateWindow: 1 * time.Second}

	// Below limit.
	state := &NotificationState{
		ErrorRateBucket: []int64{100, 200},
	}
	if !w.checkRateLimit(state, rule, 300) {
		t.Error("2 events in window should pass rate limit of 3")
	}

	// At limit.
	state2 := &NotificationState{
		ErrorRateBucket: []int64{100, 200, 300},
	}
	if w.checkRateLimit(state2, rule, 400) {
		t.Error("3 events at limit of 3 should block")
	}

	// Expired bucket entries don't count.
	base := time.Now().UnixNano()
	state4 := &NotificationState{
		ErrorRateBucket: []int64{base - int64(2*time.Second)}, // 2s ago, outside 1s window
	}
	if !w.checkRateLimit(state4, rule, base) {
		t.Error("expired bucket entries should not count against limit")
	}
	// The prune should have removed the old entry.
	if len(state4.ErrorRateBucket) != 1 || state4.ErrorRateBucket[0] != base {
		t.Error("bucket should contain only current timestamp after prune")
	}

	// No rate limit → always passes.
	ruleNoLimit := &Rule{RateLimit: 0}
	if !w.checkRateLimit(&NotificationState{}, ruleNoLimit, 100) {
		t.Error("rate limit=0 should always pass")
	}
}

// nonRetryableNotifier always returns a non-retryable error.
type nonRetryableNotifier struct{}

func (n *nonRetryableNotifier) Send(_ context.Context, _ *Event) error {
	return NewNotRetryableError(errors.New("401 unauthorized"))
}
func (n *nonRetryableNotifier) Name() string { return "mock" }
func (n *nonRetryableNotifier) Close() error { return nil }
