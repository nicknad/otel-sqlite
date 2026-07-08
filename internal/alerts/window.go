package alerts

import (
	"sort"
	"time"
)

// Window tracks event timestamps within a sliding time window.
// It supports efficient insertion and pruning of expired entries.
type Window struct {
	size       time.Duration
	timestamps []int64 // sorted chronologically
}

// NewWindow creates a new Window with the given sliding duration.
func NewWindow(size time.Duration) *Window {
	return &Window{
		size:       size,
		timestamps: make([]int64, 0),
	}
}

// Size returns the configured window duration.
func (w *Window) Size() time.Duration {
	return w.size
}

// Add inserts a timestamp and prunes entries outside the window.
// Returns the number of timestamps remaining in the window.
func (w *Window) Add(ts int64) int {
	cutoff := ts - int64(w.size)

	idx := sort.Search(len(w.timestamps), func(i int) bool {
		return w.timestamps[i] >= cutoff
	})
	w.timestamps = append(w.timestamps[idx:], ts)
	return len(w.timestamps)
}

// Count returns the number of timestamps currently within the window.
func (w *Window) Count() int {
	return len(w.timestamps)
}

// Prune removes timestamps older than (now - size).
func (w *Window) Prune(now int64) {
	cutoff := now - int64(w.size)
	idx := sort.Search(len(w.timestamps), func(i int) bool {
		return w.timestamps[i] >= cutoff
	})
	if idx > 0 {
		w.timestamps = w.timestamps[idx:]
	}
}

// Last returns the most recent timestamp in the window, or 0 if empty.
func (w *Window) Last() int64 {
	if len(w.timestamps) == 0 {
		return 0
	}
	return w.timestamps[len(w.timestamps)-1]
}

// Snapshot returns a copy of the timestamps for serialization.
func (w *Window) Snapshot() []int64 {
	cpy := make([]int64, len(w.timestamps))
	copy(cpy, w.timestamps)
	return cpy
}

// LoadFrom restores the window from a snapshot of timestamps.
func (w *Window) LoadFrom(timestamps []int64) {
	w.timestamps = make([]int64, len(timestamps))
	copy(w.timestamps, timestamps)
}

// AddToSlice prunes expired entries from a timestamp slice and appends ts.
// It operates in-place on the slice header — the caller's variable must be
// reassigned from the return value.  This is used by the notification worker
// to update alert.WindowTimestamps without allocating a separate Window.
func AddToSlice(timestamps []int64, ts int64, windowSize time.Duration) []int64 {
	cutoff := ts - int64(windowSize)
	idx := sort.Search(len(timestamps), func(i int) bool {
		return timestamps[i] >= cutoff
	})
	return append(timestamps[idx:], ts)
}
