package mika

import "fmt"

// RetryableError wraps an error to indicate that the failure is eligible for retry.
// When a record is failed with a RetryableError, MiKa will attempt to publish
// the record to the retry topic (if configured and retries remain).
// Non-retryable errors skip the retry topic and go directly to DLQ or return an error.
type RetryableError struct {
	Err error
}

func NewRetryableError(err error) *RetryableError {
	return &RetryableError{Err: err}
}

func (e *RetryableError) Error() string {
	return fmt.Sprintf("retryable: %s", e.Err.Error())
}

func (e *RetryableError) Unwrap() error {
	return e.Err
}
