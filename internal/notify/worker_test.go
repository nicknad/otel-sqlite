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
