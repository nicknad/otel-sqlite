package ingest

import (
	"context"

	"codeberg.org/nicknad/otel-sqlite/internal/model"
)

// MetricIngressQueue is a bounded queue for receiving metric batches.
// Semantics are identical to IngressQueue (backpressure by blocking when
// full); only the payload type differs.
type MetricIngressQueue interface {
	// Send adds a metric batch to the queue.
	// Blocks until space is available or context is canceled.
	Send(ctx context.Context, batch *model.MetricBatch) error

	// Receive removes and returns a metric batch from the queue.
	// Blocks until a batch is available or context is canceled.
	Receive(ctx context.Context) (*model.MetricBatch, error)

	// Close closes the queue.
	// After Close is called, Send will return an error.
	Close()

	// Len returns the current number of items in the queue.
	Len() int

	// Cap returns the capacity of the queue.
	Cap() int

	// Chan returns the underlying channel for use in select statements.
	Chan() <-chan *model.MetricBatch
}

// NewMetricIngressQueue creates a new bounded metric ingress queue with the
// given capacity. Capacity is measured in batches, not data points.
func NewMetricIngressQueue(capacity int) MetricIngressQueue {
	return &boundedMetricIngressQueue{
		q: newBoundedQueue[*model.MetricBatch](capacity),
	}
}

// boundedMetricIngressQueue is a typed wrapper around the shared generic
// queue core.
type boundedMetricIngressQueue struct {
	q *boundedQueue[*model.MetricBatch]
}

func (q *boundedMetricIngressQueue) Send(ctx context.Context, batch *model.MetricBatch) error {
	return q.q.Send(ctx, batch)
}

func (q *boundedMetricIngressQueue) Receive(ctx context.Context) (*model.MetricBatch, error) {
	return q.q.Receive(ctx)
}

func (q *boundedMetricIngressQueue) Close() {
	q.q.Close()
}

func (q *boundedMetricIngressQueue) Len() int {
	return q.q.Len()
}

func (q *boundedMetricIngressQueue) Cap() int {
	return q.q.Cap()
}

func (q *boundedMetricIngressQueue) Chan() <-chan *model.MetricBatch {
	return q.q.Chan()
}
