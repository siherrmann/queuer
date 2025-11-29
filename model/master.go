package model

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/siherrmann/queuer/helper"
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
	// JobDeleteThreshold is the duration for which archived data is retained.
	JobDeleteThreshold time.Duration `json:"retention_archive"`
	// WorkerStaleThreshold is the duration after which a worker
	// is considered stale if it hasn't updated its heartbeat
	// and gets updated to status STOPPED.
	WorkerStaleThreshold time.Duration `json:"worker_stale_threshold"`
	// WorkerDeleteThreshold is the duration after which a stale worker
	// is deleted from the database.
	WorkerDeleteThreshold time.Duration `json:"worker_delete_threshold"`
	// JobStaleThreshold is the duration after which a job
	// is considered stale if it hasn't been updated
	// and gets updated to status CANCELED.
	JobStaleThreshold time.Duration `json:"job_stale_threshold"`
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
			return helper.NewError("byte assertion", errors.New("type assertion to []byte failed"))
		}
		return json.Unmarshal(b, r)
	}
	return nil
}

func (r *MasterSettings) SetDefault() {
	if r.MasterLockTimeout == 0 {
		r.MasterLockTimeout = 5 * time.Minute
	}
	if r.MasterPollInterval == 0 {
		r.MasterPollInterval = 1 * time.Minute
	}
	if r.WorkerStaleThreshold == 0 {
		r.WorkerStaleThreshold = 5 * time.Minute
	}
	if r.WorkerDeleteThreshold == 0 {
		r.WorkerDeleteThreshold = 24 * time.Hour
	}
	if r.JobStaleThreshold == 0 {
		r.JobStaleThreshold = 1 * time.Hour
	}
	if r.JobDeleteThreshold == 0 {
		r.JobDeleteThreshold = 7 * 24 * time.Hour
	}
}
