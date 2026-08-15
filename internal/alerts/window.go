package alerts

import (
	"sort"
	"time"
)

// AddToSlice prunes expired entries from a timestamp slice and appends ts.
// It operates in-place on the slice header — the caller's variable must be
// reassigned from the return value. It is used by the notification worker
// to update alert.WindowTimestamps without allocating a separate Window.
func AddToSlice(timestamps []int64, ts int64, windowSize time.Duration) []int64 {
	cutoff := ts - int64(windowSize)
	idx := sort.Search(len(timestamps), func(i int) bool {
		return timestamps[i] >= cutoff
	})
	return append(timestamps[idx:], ts)
}
