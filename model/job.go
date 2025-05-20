package model

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
)

type Parameters map[string]interface{}

func (c Parameters) Value() (driver.Value, error) {
	return c.Marshal()
}

func (c *Parameters) Scan(value interface{}) error {
	return c.Unmarshal(value)
}

func (r Parameters) Marshal() ([]byte, error) {
	return json.Marshal(r)
}

func (r *Parameters) Unmarshal(value interface{}) error {
	if s, ok := value.(Parameters); ok {
		*r = Parameters(s)
	} else {
		b, ok := value.([]byte)
		if !ok {
			return errors.New("type assertion to []byte failed")
		}
		return json.Unmarshal(b, r)
	}
	return nil
}

type Job struct {
	ID         int        `json:"id"`
	RID        uuid.UUID  `json:"rid"`
	WorkerID   string     `json:"worker_id"`
	WorkerRID  uuid.UUID  `json:"worker_rid"`
	Parameters Parameters `json:"parameters"`
	Status     string     `json:"status"`
	CreatedAt  time.Time  `json:"created_at"`
	UpdatedAt  time.Time  `json:"updated_at"`
}
