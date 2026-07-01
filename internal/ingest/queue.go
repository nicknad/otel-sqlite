// Package ingest provides bounded queue abstractions for the ingestion pipeline.
// All queues are bounded to prevent memory exhaustion and provide backpressure.
package ingest

import (
	"context"

	"github.com/nnadolski/otel-sqlite/internal/model"
)

// IngressQueue is a bounded queue for receiving individual log records.
// It provides backpressure by blocking when full.
type IngressQueue interface {
	// Send adds a log record to the queue.
	// Blocks until space is available or context is canceled.
	Send(ctx context.Context, record *model.LogRecord) error

	// Receive removes and returns a log record from the queue.
	// Blocks until a record is available or context is canceled.
	Receive(ctx context.Context) (*model.LogRecord, error)

	// Close closes the queue.
	// After Close is called, Send will return an error.
	Close()

	// Len returns the current number of items in the queue.
	Len() int

	// Cap returns the capacity of the queue.
	Cap() int

	// Chan returns the underlying channel for use in select statements.
	// This allows consumers to respond to both incoming records and other
	// events (e.g., flush ticks, context cancellation) in a single select.
	Chan() <-chan *model.LogRecord
}

// NewIngressQueue creates a new bounded ingress queue with the given capacity.
func NewIngressQueue(capacity int) IngressQueue {
	return &boundedIngressQueue{
		ch: make(chan *model.LogRecord, capacity),
	}
}

// boundedIngressQueue is a channel-based implementation of IngressQueue.
type boundedIngressQueue struct {
	ch chan *model.LogRecord
}

func (q *boundedIngressQueue) Send(ctx context.Context, record *model.LogRecord) error {
	select {
	case q.ch <- record:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (q *boundedIngressQueue) Receive(ctx context.Context) (*model.LogRecord, error) {
	select {
	case record, ok := <-q.ch:
		if !ok {
			return nil, ErrQueueClosed
		}
		return record, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (q *boundedIngressQueue) Close() {
	close(q.ch)
}

func (q *boundedIngressQueue) Len() int {
	return len(q.ch)
}

func (q *boundedIngressQueue) Cap() int {
	return cap(q.ch)
}

func (q *boundedIngressQueue) Chan() <-chan *model.LogRecord {
	return q.ch
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
