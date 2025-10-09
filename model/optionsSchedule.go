package model

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"time"

	"github.com/siherrmann/queuer/helper"
)

// NextIntervalFunc defines a function type that calculates the next interval
// based on the start time and the current count of executions.
type NextIntervalFunc func(start time.Time, currentCount int) time.Time

// Schedule represents the scheduling options for a job.
// It includes the start time, maximum count of executions, interval between executions,
// and an optional next interval function to calculate the next execution time.
// It is used to define how often a job should be executed.
//
// Parameters:
// - Start is the time when the job should start executing.
// - If MaxCount is 0, the job will run indefinitely.
// - If MaxCount greater 0, the job will run MaxCount times.
// - Interval is the duration between executions.
// - If MaxCount is equal or greater than 0, Interval must be greater than zero or NextInterval must be provided.
// - If MaxCount is 1, NextInterval can be provided to specify the next execution time.
type Schedule struct {
	Start        time.Time     `json:"start"`
	MaxCount     int           `json:"max_count"`
	Interval     time.Duration `json:"interval"`
	NextInterval string        `json:"next_interval,omitempty"`
}

// IsValid checks if the Schedule options are valid.
// Start time must not be zero, MaxCount must be non-negative,
// Interval must be greater than zero if MaxCount is greater than 1,
// or NextInterval must be provided.
func (c *Schedule) IsValid() error {
	if c.Start.IsZero() {
		return helper.NewError("zero Start check", errors.New("start time cannot be zero"))
	}
	if c.MaxCount < 0 {
		return helper.NewError("negative MaxCount check", errors.New("maxCount must be greater than or equal to 0"))
	}
	if c.MaxCount > 1 && c.Interval <= time.Duration(0) && c.NextInterval == "" {
		return helper.NewError("invalid interval check", errors.New("interval must be greater than zero or nextInterval must be provided"))
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
			return helper.NewError("byte assertion", errors.New("type assertion to []byte failed"))
		}
		return json.Unmarshal(b, r)
	}
	return nil
}
