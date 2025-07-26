package model

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
)

type Master struct {
	ID        int            `json:"id"`
	Settings  MasterSettings `json:"settings"`
	WorkerID  int            `json:"worker_id"`
	WorkerRID uuid.UUID      `json:"worker_rid"`
	CreatedAt time.Time      `json:"created_at"`
	UpdatedAt time.Time      `json:"updated_at"`
}

type MasterSettings struct {
	// MasterLockTimeout is the duration after which a master lock
	// is considered stale and a new worker can get master.
	MasterLockTimeout time.Duration `json:"master_lock_timeout"`
	// MasterPollInterval is the interval at which the master worker
	// updates the master entry to stay master.
	MasterPollInterval time.Duration `json:"master_poll_interval"`
	// RetentionArchive is the duration for which archived data is retained.
	RetentionArchive time.Duration `json:"retention_archive"`
}

func (c MasterSettings) Value() (driver.Value, error) {
	return c.Marshal()
}

func (c *MasterSettings) Scan(value interface{}) error {
	return c.Unmarshal(value)
}

func (r MasterSettings) Marshal() ([]byte, error) {
	return json.Marshal(r)
}

func (r *MasterSettings) Unmarshal(value interface{}) error {
	if o, ok := value.(MasterSettings); ok {
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
