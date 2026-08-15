// Package ingest provides bounded queue abstractions for the ingestion pipeline.
// All queues are bounded to prevent memory exhaustion and provide backpressure.
package ingest

import (
	"context"

	"codeberg.org/nicknad/otel-sqlite/internal/model"
)

// IngressQueue is a bounded queue for receiving log batches.
// It provides backpressure by blocking when full.
//
// Each Send enqueues an entire LogBatch in a single channel operation,
// eliminating per-record channel overhead in the gRPC handler.
type IngressQueue interface {
	// Send adds a log batch to the queue.
	// Blocks until space is available or context is canceled.
	Send(ctx context.Context, batch *model.LogBatch) error

	// Receive removes and returns a log batch from the queue.
	// Blocks until a batch is available or context is canceled.
	Receive(ctx context.Context) (*model.LogBatch, error)

	// Close closes the queue.
	// After Close is called, Send will return an error.
	Close()

	// Len returns the current number of items in the queue.
	Len() int

	// Cap returns the capacity of the queue.
	Cap() int

	// Chan returns the underlying channel for use in select statements.
	// This allows consumers to respond to both incoming batches and other
	// events (e.g., flush ticks, context cancellation) in a single select.
	Chan() <-chan *model.LogBatch
}

// NewIngressQueue creates a new bounded ingress queue with the given capacity.
// Capacity is measured in batches, not individual records.
func NewIngressQueue(capacity int) IngressQueue {
	return &boundedIngressQueue{
		q: newBoundedQueue[*model.LogBatch](capacity),
	}
}

// boundedIngressQueue is a typed wrapper around the shared generic queue core.
type boundedIngressQueue struct {
	q *boundedQueue[*model.LogBatch]
}

func (q *boundedIngressQueue) Send(ctx context.Context, batch *model.LogBatch) error {
	return q.q.Send(ctx, batch)
}

func (q *boundedIngressQueue) Receive(ctx context.Context) (*model.LogBatch, error) {
	return q.q.Receive(ctx)
}

func (q *boundedIngressQueue) Close() {
	q.q.Close()
}

func (q *boundedIngressQueue) Len() int {
	return q.q.Len()
}

func (q *boundedIngressQueue) Cap() int {
	return q.q.Cap()
}

func (q *boundedIngressQueue) Chan() <-chan *model.LogBatch {
	return q.q.Chan()
}

// ErrQueueClosed is returned when trying to receive from a closed queue.
var ErrQueueClosed = &QueueError{Message: "queue closed"}

// QueueError represents an error that occurred with a queue.
type QueueError struct {
	Message string
}

func (e *QueueError) Error() string {
	return e.Message
}
