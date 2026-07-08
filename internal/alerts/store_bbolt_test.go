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
