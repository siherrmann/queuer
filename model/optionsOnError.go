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

type OnError struct {
	Timeout      float64 `json:"timeout"`
	MaxRetries   int     `json:"max_retries"`
	RetryDelay   float64 `json:"retry_delay"`
	RetryBackoff string  `json:"retry_backoff"`
}

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
