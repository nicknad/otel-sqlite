package alerts

import "time"

// Counter accumulates event hits within a sliding window and checks
// whether a configurable threshold has been exceeded.
type Counter struct {
	window    *Window
	threshold int
}

// NewCounter creates a Counter with the given window size and threshold.
func NewCounter(windowSize time.Duration, threshold int) *Counter {
	return &Counter{
		window:    NewWindow(windowSize),
		threshold: threshold,
	}
}

// Hit records an event timestamp and returns true if the count within the
// window now meets or exceeds the threshold.
func (c *Counter) Hit(ts int64) (count int, firing bool) {
	count = c.window.Add(ts)
	firing = count >= c.threshold
	return
}

// Count returns the current number of events in the window (no mutation).
func (c *Counter) Count() int {
	return c.window.Count()
}

// Prune removes timestamps older than (now - window size) without recording
// a new event. Used to re-evaluate state when no new events arrive.
func (c *Counter) Prune(now int64) {
	c.window.Prune(now)
}

// Threshold returns the configured threshold.
func (c *Counter) Threshold() int {
	return c.threshold
}

// Window returns the underlying window.
func (c *Counter) Window() *Window {
	return c.window
}

// LoadFrom restores the counter's window from a snapshot.
func (c *Counter) LoadFrom(timestamps []int64) {
	c.window.LoadFrom(timestamps)
}

// Snapshot returns a copy of the window timestamps for serialization.
func (c *Counter) Snapshot() []int64 {
	return c.window.Snapshot()
}
