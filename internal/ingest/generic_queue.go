package ingest

import "context"

// boundedQueue is a channel-based bounded FIFO queue for a single item type.
// It is the shared core for the log and metric ingress queues: both signals
// need identical backpressure semantics (blocking Send on full queue,
// context-aware cancel, Len/Cap for depth gauges, and a raw channel for
// select-based consumers).
type boundedQueue[T any] struct {
	ch chan T
}

// newBoundedQueue creates a bounded queue with the given capacity.
func newBoundedQueue[T any](capacity int) *boundedQueue[T] {
	return &boundedQueue[T]{ch: make(chan T, capacity)}
}

// Send adds an item to the queue, blocking until space is available or the
// context is canceled.
func (q *boundedQueue[T]) Send(ctx context.Context, item T) error {
	select {
	case q.ch <- item:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Receive removes and returns an item from the queue, blocking until one is
// available or the context is canceled.
func (q *boundedQueue[T]) Receive(ctx context.Context) (T, error) {
	var zero T
	select {
	case item, ok := <-q.ch:
		if !ok {
			return zero, ErrQueueClosed
		}
		return item, nil
	case <-ctx.Done():
		return zero, ctx.Err()
	}
}

// Close closes the queue. After Close, Send returns an error and Receive
// drains remaining items then returns ErrQueueClosed.
func (q *boundedQueue[T]) Close() {
	close(q.ch)
}

// Len returns the current number of items in the queue.
func (q *boundedQueue[T]) Len() int {
	return len(q.ch)
}

// Cap returns the capacity of the queue.
func (q *boundedQueue[T]) Cap() int {
	return cap(q.ch)
}

// Chan returns the underlying channel for use in select statements.
func (q *boundedQueue[T]) Chan() <-chan T {
	return q.ch
}
