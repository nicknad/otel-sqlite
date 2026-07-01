package storage

import (
	"context"
)

// ErrQueueClosed is returned when trying to receive from a closed queue.
var ErrQueueClosed = &QueueError{Message: "command queue closed"}

// QueueError represents an error that occurred with a queue.
type QueueError struct {
	Message string
}

func (e *QueueError) Error() string {
	return e.Message
}

// CommandQueue is a bounded, FIFO, single-consumer queue for Command objects.
//
// Properties:
//   - Bounded: Send blocks when the queue is full (provides backpressure).
//   - FIFO: Commands are dequeued in the order they were enqueued.
//   - Single consumer: Only one goroutine should call Receive.
//
// The queue is safe for one concurrent sender and one concurrent receiver.
type CommandQueue interface {
	// Send adds a command to the queue.
	// Blocks until space is available or context is canceled.
	Send(ctx context.Context, cmd Command) error

	// Receive removes and returns a command from the queue.
	// Blocks until a command is available or context is canceled.
	Receive(ctx context.Context) (Command, error)

	// Close closes the queue.
	// After Close, Send returns an error; Receive drains remaining items
	// then returns ErrQueueClosed.
	Close()

	// Len returns the current number of items in the queue.
	Len() int

	// Cap returns the capacity of the queue.
	Cap() int

	// Chan returns the underlying channel for use in select statements.
	// This allows consumers to respond to both incoming commands and other
	// events (e.g., flush ticks, context cancellation) in a single select.
	Chan() <-chan Command
}

// NewCommandQueue creates a new bounded command queue with the given capacity.
func NewCommandQueue(capacity int) CommandQueue {
	return &boundedCommandQueue{
		ch: make(chan Command, capacity),
	}
}

// boundedCommandQueue is a channel-based implementation of CommandQueue.
type boundedCommandQueue struct {
	ch chan Command
}

func (q *boundedCommandQueue) Send(ctx context.Context, cmd Command) error {
	select {
	case q.ch <- cmd:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (q *boundedCommandQueue) Receive(ctx context.Context) (Command, error) {
	select {
	case cmd, ok := <-q.ch:
		if !ok {
			return nil, ErrQueueClosed
		}
		return cmd, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (q *boundedCommandQueue) Close() {
	close(q.ch)
}

func (q *boundedCommandQueue) Len() int {
	return len(q.ch)
}

func (q *boundedCommandQueue) Cap() int {
	return cap(q.ch)
}

func (q *boundedCommandQueue) Chan() <-chan Command {
	return q.ch
}

// Ensure *boundedCommandQueue satisfies CommandQueue at compile time.
var _ CommandQueue = (*boundedCommandQueue)(nil)
