package alerts

import (
	"testing"
	"time"
)

func TestWindow_Add(t *testing.T) {
	base := time.Now().UnixNano()
	w := NewWindow(1 * time.Second)

	// Add within window.
	n := w.Add(base)
	if n != 1 {
		t.Fatalf("count = %d, want 1", n)
	}

	// Add another within window.
	n = w.Add(base + int64(500*time.Millisecond))
	if n != 2 {
		t.Fatalf("count = %d, want 2", n)
	}

	// Add outside window — should prune old ones.
	n = w.Add(base + int64(2*time.Second))
	if n != 1 {
		t.Fatalf("count = %d, want 1 (old pruned), got %d", n, n)
	}
}

func TestWindow_Prune(t *testing.T) {
	base := time.Now().UnixNano()
	w := NewWindow(1 * time.Second)

	w.Add(base)
	w.Add(base + int64(500*time.Millisecond))
	w.Add(base + int64(2*time.Second)) // only this one should remain

	w.Prune(base + int64(2*time.Second))
	if w.Count() != 1 {
		t.Errorf("Count after prune = %d, want 1", w.Count())
	}
}

func TestWindow_Last(t *testing.T) {
	w := NewWindow(1 * time.Second)
	if w.Last() != 0 {
		t.Error("empty window Last should be 0")
	}

	ts := time.Now().UnixNano()
	w.Add(ts)
	if w.Last() != ts {
		t.Errorf("Last = %d, want %d", w.Last(), ts)
	}
}

func TestWindow_SnapshotRoundtrip(t *testing.T) {
	base := time.Now().UnixNano()
	w := NewWindow(5 * time.Second)
	w.Add(base)
	w.Add(base + int64(1*time.Second))
	w.Add(base + int64(2*time.Second))

	snap := w.Snapshot()
	if len(snap) != 3 {
		t.Fatalf("snapshot len = %d, want 3", len(snap))
	}

	w2 := NewWindow(5 * time.Second)
	w2.LoadFrom(snap)
	if w2.Count() != 3 {
		t.Errorf("loaded count = %d, want 3", w2.Count())
	}
	if w2.Last() != base+int64(2*time.Second) {
		t.Errorf("loaded Last = %d, want %d", w2.Last(), base+int64(2*time.Second))
	}
}
