package model

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
)

const (
	RETRY_BACKOFF_NONE        = "none"
	RETRY_BACKOFF_LINEAR      = "linear"
	RETRY_BACKOFF_EXPONENTIAL = "exponential"
)

// OptionsOnError represents the options to handle errors during job execution.
// It includes timeout, maximum retries, retry delay, and retry backoff strategy.
// It is used to define how the system should behave when a job fails.
//
// Parameters:
// - Timeout is the maximum time in seconds to wait for a job to complete before considering it failed.
// - MaxRetries is the maximum number of retries to attempt if a job fails.
// - RetryDelay is the delay in seconds before retrying a failed job.
// - RetryBackoff is the strategy to use for retrying failed jobs. It can be one of the following:
//   - none: no retry, the job will fail immediately.
//   - linear: retry with a fixed delay.
//   - exponential: retry with an exponentially increasing delay.
//     If RetryBackoff is not provided, it defaults to "none".
type OnError struct {
	Timeout      float64 `json:"timeout"`
	MaxRetries   int     `json:"max_retries"`
	RetryDelay   float64 `json:"retry_delay"`
	RetryBackoff string  `json:"retry_backoff"`
}

// IsValid checks if the OnError options are valid.
// Timeout, MaxRetries, and RetryDelay must be non-negative.
// RetryBackoff must be one of the predefined strategies: none, linear, or exponential.
// It returns an error if any of the conditions are not met.
func (c *OnError) IsValid() error {
	if c.Timeout < 0 {
		return errors.New("timeout cannot be negative")
	}
	if c.MaxRetries < 0 {
		return errors.New("max retries cannot be negative")
	}
	if c.RetryDelay < 0 {
		return errors.New("retry delay cannot be negative")
	}
	if c.RetryBackoff != RETRY_BACKOFF_NONE &&
		c.RetryBackoff != RETRY_BACKOFF_LINEAR &&
		c.RetryBackoff != RETRY_BACKOFF_EXPONENTIAL {
		return errors.New("retry backoff must be one of: none, linear, exponential")
	}
	return nil
}

func (c OnError) Value() (driver.Value, error) {
	return c.Marshal()
}

func (c *OnError) Scan(value interface{}) error {
	return c.Unmarshal(value)
}

func (r OnError) Marshal() ([]byte, error) {
	return json.Marshal(r)
}

func (r *OnError) Unmarshal(value interface{}) error {
	if o, ok := value.(OnError); ok {
		*r = o
	} else {
		b, ok := value.([]byte)
		if !ok {
			return errors.New("type assertion to []byte failed")
		}
		return json.Unmarshal(b, r)
	}
	return nil
}
