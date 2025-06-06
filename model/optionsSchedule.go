package model

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"time"
)

type Schedule struct {
	Start        time.Time                                         `json:"start"`
	Interval     time.Duration                                     `json:"interval"`
	MaxCount     int                                               `json:"max_count"`
	NextInterval func(start time.Time, currentCount int) time.Time `json:"-"`
}

func (c *Schedule) IsValid() error {
	if c.Start.IsZero() {
		return errors.New("start time cannot be zero")
	}
	if c.Interval <= 0 && c.NextInterval == nil && c.MaxCount > 1 {
		return errors.New("if maxCount is greater than 1 interval must be greater than zero or nextInterval must be provided")
	}
	return nil
}

func (c Schedule) Value() (driver.Value, error) {
	return c.Marshal()
}

func (c *Schedule) Scan(value interface{}) error {
	return c.Unmarshal(value)
}

func (r Schedule) Marshal() ([]byte, error) {
	return json.Marshal(r)
}

func (r *Schedule) Unmarshal(value interface{}) error {
	if o, ok := value.(Schedule); ok {
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
