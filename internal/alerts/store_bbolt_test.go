package alerts

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"codeberg.org/nicknad/otel-sqlite/internal/model"
)

func TestBboltAlertStore_GetPutDelete(t *testing.T) {
	path := filepath.Join(t.TempDir(), "alerts-test.db")
	store, err := NewBboltAlertStore(path)
	if err != nil {
		t.Fatalf("NewBboltAlertStore: %v", err)
	}
	defer store.Close()

	ctx := context.Background()

	// Get non-existent.
	alert, err := store.Get(ctx, "rule:res-1")
	if err != nil {
		t.Fatalf("Get non-existent: %v", err)
	}
	if alert != nil {
		t.Error("expected nil for non-existent alert")
	}

	// Put.
	now := time.Now().UnixNano()
	a := NewAlert("test-rule", "res-1", model.SeverityError, now)
	a.Status = AlertFiring
	a.WindowTimestamps = []int64{now, now + 1}
	err = store.Put(ctx, a)
	if err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Get.
	got, err := store.Get(ctx, a.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got == nil {
		t.Fatal("expected alert, got nil")
	}
	if got.Status != AlertFiring {
		t.Errorf("status = %v, want firing", got.Status)
	}
	if len(got.WindowTimestamps) != 2 {
		t.Errorf("window timestamps len = %d, want 2", len(got.WindowTimestamps))
	}

	// Update.
	a.Status = AlertResolved
	err = store.Put(ctx, a)
	if err != nil {
		t.Fatalf("Put update: %v", err)
	}

	got, err = store.Get(ctx, a.ID)
	if err != nil {
		t.Fatalf("Get after update: %v", err)
	}
	if got.Status != AlertResolved {
		t.Errorf("status = %v, want resolved", got.Status)
	}

	// Delete.
	err = store.Delete(ctx, a.ID)
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}

	got, err = store.Get(ctx, a.ID)
	if err != nil {
		t.Fatalf("Get after delete: %v", err)
	}
	if got != nil {
		t.Error("expected nil after delete")
	}
}

func TestBboltAlertStore_ListByStatus(t *testing.T) {
	path := filepath.Join(t.TempDir(), "alerts-list.db")
	store, err := NewBboltAlertStore(path)
	if err != nil {
		t.Fatalf("NewBboltAlertStore: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	now := time.Now().UnixNano()

	// Create alerts with different statuses.
	a1 := NewAlert("rule-a", "res-1", model.SeverityError, now)
	a1.Status = AlertFiring
	store.Put(ctx, a1)

	a2 := NewAlert("rule-b", "res-2", model.SeverityError, now)
	a2.Status = AlertFiring
	store.Put(ctx, a2)

	a3 := NewAlert("rule-c", "res-3", model.SeverityError, now)
	a3.Status = AlertResolved
	store.Put(ctx, a3)

	firing, err := store.ListByStatus(ctx, AlertFiring)
	if err != nil {
		t.Fatalf("ListByStatus firing: %v", err)
	}
	if len(firing) != 2 {
		t.Errorf("firing count = %d, want 2", len(firing))
	}

	resolved, err := store.ListByStatus(ctx, AlertResolved)
	if err != nil {
		t.Fatalf("ListByStatus resolved: %v", err)
	}
	if len(resolved) != 1 {
		t.Errorf("resolved count = %d, want 1", len(resolved))
	}
}

func TestBboltAlertStore_ListAll(t *testing.T) {
	path := filepath.Join(t.TempDir(), "alerts-all.db")
	store, err := NewBboltAlertStore(path)
	if err != nil {
		t.Fatalf("NewBboltAlertStore: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	now := time.Now().UnixNano()

	store.Put(ctx, NewAlert("r1", "res-1", model.SeverityError, now))
	store.Put(ctx, NewAlert("r2", "res-2", model.SeverityError, now))

	all, err := store.ListAll(ctx)
	if err != nil {
		t.Fatalf("ListAll: %v", err)
	}
	if len(all) != 2 {
		t.Errorf("all count = %d, want 2", len(all))
	}

	// Clean up.
	os.Remove(path)
}

// mockNotifStore implements AlertNotifStore for testing.
type mockNotifStore struct {
	deleted []string
}

func (m *mockNotifStore) DeleteNotificationState(_ context.Context, alertID string) error {
	m.deleted = append(m.deleted, alertID)
	return nil
}

func TestBboltAlertStore_DeleteIdleAlerts_EvictsStalePending(t *testing.T) {
	path := filepath.Join(t.TempDir(), "alerts-idle.db")
	store, err := NewBboltAlertStore(path)
	if err != nil {
		t.Fatalf("NewBboltAlertStore: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	now := time.Now()

	// Create a stale Pending alert (idle for 2 hours).
	stale := NewAlert("rule-a", "res-stale", model.SeverityError, now.Add(-2*time.Hour).UnixNano())
	stale.Status = AlertPending
	stale.LastMatched = now.Add(-2 * time.Hour).UnixNano()
	stale.UpdatedAt = stale.LastMatched
	if err := store.Put(ctx, stale); err != nil {
		t.Fatalf("Put stale: %v", err)
	}

	// Create a recent Pending alert (idle for 10 minutes).
	recent := NewAlert("rule-b", "res-recent", model.SeverityError, now.Add(-10*time.Minute).UnixNano())
	recent.Status = AlertPending
	recent.LastMatched = now.Add(-10 * time.Minute).UnixNano()
	recent.UpdatedAt = recent.LastMatched
	if err := store.Put(ctx, recent); err != nil {
		t.Fatalf("Put recent: %v", err)
	}

	// Create a stale Firing alert.
	staleFiring := NewAlert("rule-c", "res-fire", model.SeverityError, now.Add(-3*time.Hour).UnixNano())
	staleFiring.Status = AlertFiring
	staleFiring.LastMatched = now.Add(-3 * time.Hour).UnixNano()
	staleFiring.UpdatedAt = staleFiring.LastMatched
	if err := store.Put(ctx, staleFiring); err != nil {
		t.Fatalf("Put staleFiring: %v", err)
	}

	// Create a Resolved alert — should NOT be evicted by DeleteIdleAlerts.
	resolved := NewAlert("rule-d", "res-resolved", model.SeverityError, now.Add(-5*time.Hour).UnixNano())
	resolved.Status = AlertResolved
	resolved.LastMatched = now.Add(-5 * time.Hour).UnixNano()
	resolved.UpdatedAt = resolved.LastMatched
	if err := store.Put(ctx, resolved); err != nil {
		t.Fatalf("Put resolved: %v", err)
	}

	notifStore := &mockNotifStore{}

	// Evict with 1-hour TTL. Should delete stale Pending and stale Firing,
	// but keep recent Pending and Resolved.
	deleted, err := store.DeleteIdleAlerts(ctx, 1*time.Hour, notifStore)
	if err != nil {
		t.Fatalf("DeleteIdleAlerts: %v", err)
	}
	if deleted != 2 {
		t.Errorf("deleted = %d, want 2 (stale pending + stale firing)", deleted)
	}

	// Verify stale alerts are gone.
	a, _ := store.Get(ctx, stale.ID)
	if a != nil {
		t.Error("stale Pending alert should be deleted")
	}
	a, _ = store.Get(ctx, staleFiring.ID)
	if a != nil {
		t.Error("stale Firing alert should be deleted")
	}

	// Verify recent Pending still exists.
	a, _ = store.Get(ctx, recent.ID)
	if a == nil {
		t.Error("recent Pending alert should NOT be deleted")
	}

	// Verify Resolved still exists.
	a, _ = store.Get(ctx, resolved.ID)
	if a == nil {
		t.Error("Resolved alert should NOT be deleted by DeleteIdleAlerts")
	}

	// Check notification state cleanup.
	if len(notifStore.deleted) != 2 {
		t.Errorf("notif state deletions = %d, want 2", len(notifStore.deleted))
	}
}

func TestBboltAlertStore_DeleteIdleAlerts_KeepsActive(t *testing.T) {
	path := filepath.Join(t.TempDir(), "alerts-active.db")
	store, err := NewBboltAlertStore(path)
	if err != nil {
		t.Fatalf("NewBboltAlertStore: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	now := time.Now()

	// Create an active Firing alert (matched 5 seconds ago).
	active := NewAlert("rule-x", "res-active", model.SeverityError, now.Add(-5*time.Second).UnixNano())
	active.Status = AlertFiring
	active.LastMatched = now.Add(-5 * time.Second).UnixNano()
	active.UpdatedAt = active.LastMatched
	if err := store.Put(ctx, active); err != nil {
		t.Fatalf("Put: %v", err)
	}

	notifStore := &mockNotifStore{}

	// Evict with 1-minute TTL. The active alert (5s old) should survive.
	deleted, err := store.DeleteIdleAlerts(ctx, 1*time.Minute, notifStore)
	if err != nil {
		t.Fatalf("DeleteIdleAlerts: %v", err)
	}
	if deleted != 0 {
		t.Errorf("deleted = %d, want 0 (active alert should survive)", deleted)
	}

	a, _ := store.Get(ctx, active.ID)
	if a == nil {
		t.Error("active Firing alert should NOT be deleted")
	}
}
