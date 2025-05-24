package model

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
)

const (
	RETRY_BACKOFF_LINEAR      = "linear"
	RETRY_BACKOFF_EXPONENTIAL = "exponential"
)

type Options struct {
	Timeout      float64 `json:"timeout"`
	MaxRetries   int     `json:"max_retries"`
	RetryDelay   float64 `json:"retry_delay"`
	RetryBackoff string  `json:"retry_backoff"`
}

func (c Options) Value() (driver.Value, error) {
	return c.Marshal()
}

func (c *Options) Scan(value interface{}) error {
	return c.Unmarshal(value)
}

func (r Options) Marshal() ([]byte, error) {
	return json.Marshal(r)
}

func (r *Options) Unmarshal(value interface{}) error {
	if o, ok := value.(Options); ok {
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
