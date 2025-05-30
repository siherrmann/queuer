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

func NewSchedule(start time.Time, interval time.Duration, maxCount int, nextInterval func(start time.Time, currentCount int) time.Time) (*Schedule, error) {
	if start.IsZero() {
		return nil, errors.New("start time cannot be zero")
	}
	if interval <= 0 && nextInterval == nil && maxCount > 1 {
		return nil, errors.New("if maxCount is greater than 1 interval must be greater than zero or nextInterval must be provided")
	}

	return &Schedule{
		Start:        start,
		Interval:     interval,
		MaxCount:     maxCount,
		NextInterval: nextInterval,
	}, nil
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
