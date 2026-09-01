// Package notifications provides notifier delivery, retry logic, and
// dead-letter queue management. It receives alerts from the alerting
// pipeline and delivers them via pluggable notifiers.
package notifications

import "errors"

// IsRetryable checks whether an error from a Notifier is retryable.
// Non-retryable errors are returned when the destination indicates the
// request should not be retried (e.g., 4xx HTTP responses).
func IsRetryable(err error) bool {
	var nr *notRetryableError
	return !errors.As(err, &nr)
}

// notRetryableError wraps an error to mark it as non-retryable.
type notRetryableError struct{ err error }

func (e *notRetryableError) Error() string { return e.err.Error() }
func (e *notRetryableError) Unwrap() error { return e.err }

// NewNotRetryableError wraps an error to mark it as non-retryable.
func NewNotRetryableError(err error) error {
	return &notRetryableError{err: err}
}

// Sentinel errors.
var (
	ErrQueueFull = errors.New("notifications: event queue full")
	ErrNotFound  = errors.New("notifications: state not found")
)
