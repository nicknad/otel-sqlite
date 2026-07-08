package alerts

import (
	"testing"
	"time"
)

func TestCounter_Hit(t *testing.T) {
	c := NewCounter(1*time.Second, 3)
	base := time.Now().UnixNano()

	// Below threshold.
	count, firing := c.Hit(base)
	if firing {
		t.Error("should not fire below threshold")
	}
	if count != 1 {
		t.Errorf("count = %d, want 1", count)
	}

	_, firing = c.Hit(base + int64(100*time.Millisecond))
	if firing {
		t.Error("should not fire at count=2 below threshold=3")
	}

	// Hit threshold.
	count, firing = c.Hit(base + int64(200*time.Millisecond))
	if !firing {
		t.Error("should fire at threshold")
	}
	if count != 3 {
		t.Errorf("count = %d, want 3", count)
	}
}

func TestCounter_Prune(t *testing.T) {
	c := NewCounter(500*time.Millisecond, 5)
	base := time.Now().UnixNano()

	c.Hit(base)
	c.Hit(base + int64(100*time.Millisecond))

	if c.Count() != 2 {
		t.Fatalf("pre-prune count = %d, want 2", c.Count())
	}

	// Prune at a time outside window.
	c.Prune(base + int64(1*time.Second))
	if c.Count() != 0 {
		t.Errorf("post-prune count = %d, want 0", c.Count())
	}
}

func TestCounter_Threshold(t *testing.T) {
	c := NewCounter(1*time.Second, 10)
	if c.Threshold() != 10 {
		t.Errorf("threshold = %d, want 10", c.Threshold())
	}
}

func TestCounter_SnapshotRoundtrip(t *testing.T) {
	c := NewCounter(5*time.Second, 3)
	base := time.Now().UnixNano()
	c.Hit(base)
	c.Hit(base + int64(1*time.Second))

	snap := c.Snapshot()
	if len(snap) != 2 {
		t.Fatalf("snapshot len = %d, want 2", len(snap))
	}

	c2 := NewCounter(5*time.Second, 3)
	c2.LoadFrom(snap)
	if c2.Count() != 2 {
		t.Errorf("loaded count = %d, want 2", c2.Count())
	}
}
