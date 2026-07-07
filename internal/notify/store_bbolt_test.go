package notify

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestBboltStore_GetPutDelete(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	store, err := NewBboltStore(path)
	if err != nil {
		t.Fatalf("NewBboltStore: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	key := "test:res1:abc123"

	// Get non-existent.
	_, err = store.GetState(ctx, key)
	if err != ErrNotFound {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}

	// Put.
	state := &NotificationState{
		RetryCount:    2,
		LastAttempt:   time.Now().UnixNano(),
		CooldownUntil: time.Now().Add(time.Hour).UnixNano(),
		EventDigest:   "abc123",
	}
	err = store.PutState(ctx, key, state)
	if err != nil {
		t.Fatalf("PutState: %v", err)
	}

	// Get.
	got, err := store.GetState(ctx, key)
	if err != nil {
		t.Fatalf("GetState: %v", err)
	}
	if got.RetryCount != 2 {
		t.Errorf("RetryCount = %d, want 2", got.RetryCount)
	}
	if got.EventDigest != "abc123" {
		t.Errorf("EventDigest = %q, want %q", got.EventDigest, "abc123")
	}
	if got.UpdatedAt == 0 {
		t.Error("UpdatedAt should be set")
	}

	// Update.
	state.RetryCount = 3
	err = store.PutState(ctx, key, state)
	if err != nil {
		t.Fatalf("PutState (update): %v", err)
	}
	got, err = store.GetState(ctx, key)
	if err != nil {
		t.Fatalf("GetState after update: %v", err)
	}
	if got.RetryCount != 3 {
		t.Errorf("RetryCount = %d, want 3", got.RetryCount)
	}

	// Delete.
	err = store.DeleteState(ctx, key)
	if err != nil {
		t.Fatalf("DeleteState: %v", err)
	}
	_, err = store.GetState(ctx, key)
	if err != ErrNotFound {
		t.Errorf("expected ErrNotFound after delete, got %v", err)
	}
}

func TestBboltStore_DLQ(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	store, err := NewBboltStore(path)
	if err != nil {
		t.Fatalf("NewBboltStore: %v", err)
	}
	defer store.Close()

	ctx := context.Background()

	event := &Event{
		Severity: 17, // ERROR
		Body:     "test error",
	}

	// Enqueue.
	err = store.EnqueueDLQ(ctx, event, "connection refused")
	if err != nil {
		t.Fatalf("EnqueueDLQ: %v", err)
	}

	// List.
	entries, err := store.ListDLQ(ctx)
	if err != nil {
		t.Fatalf("ListDLQ: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 DLQ entry, got %d", len(entries))
	}
	if entries[0].Event.Body != "test error" {
		t.Errorf("event body = %q, want %q", entries[0].Event.Body, "test error")
	}
	if entries[0].FailReason != "connection refused" {
		t.Errorf("fail reason = %q, want %q", entries[0].FailReason, "connection refused")
	}

	// Ack.
	err = store.AckDLQ(ctx, entries[0].ID)
	if err != nil {
		t.Fatalf("AckDLQ: %v", err)
	}

	entries, err = store.ListDLQ(ctx)
	if err != nil {
		t.Fatalf("ListDLQ after ack: %v", err)
	}
	if len(entries) != 0 {
		t.Errorf("expected 0 entries after ack, got %d", len(entries))
	}
}

func TestBboltStore_ScanRetryable(t *testing.T) {
	path := tempDBPath(t)
	defer os.Remove(path)

	store, err := NewBboltStore(path)
	if err != nil {
		t.Fatalf("NewBboltStore: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	now := time.Now().UnixNano()

	// State due for retry.
	state1 := &NotificationState{
		RetryCount: 1,
		NextRetry:  now - int64(time.Second), // past due
	}
	store.PutState(ctx, "rule:res1:fp1", state1)

	// State not due yet.
	state2 := &NotificationState{
		RetryCount: 2,
		NextRetry:  now + int64(time.Hour), // future
	}
	store.PutState(ctx, "rule:res2:fp2", state2)

	// State with no retries.
	state3 := &NotificationState{
		RetryCount: 0,
		NextRetry:  now - int64(time.Hour),
	}
	store.PutState(ctx, "rule:res3:fp3", state3)

	entries, err := store.ScanRetryable(ctx)
	if err != nil {
		t.Fatalf("ScanRetryable: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 retryable entry, got %d", len(entries))
	}
	if entries[0].Key != "rule:res1:fp1" {
		t.Errorf("key = %q, want %q", entries[0].Key, "rule:res1:fp1")
	}
	if entries[0].RetryCount != 1 {
		t.Errorf("retry count = %d, want 1", entries[0].RetryCount)
	}
}

func tempDBPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "test.db")
}
