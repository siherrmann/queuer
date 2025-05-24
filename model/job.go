package model

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	JobStatusQueued    = "QUEUED"
	JobStatusRunning   = "RUNNING"
	JobStatusFailed    = "FAILED"
	JobStatusSucceeded = "SUCCEEDED"
	JobStatusCancelled = "CANCELLED"
)

type Parameters []interface{}

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

func (r *Parameters) ToReflectValues() []reflect.Value {
	reflectValues := []reflect.Value{}
	for _, p := range *r {
		reflectValues = append(reflectValues, reflect.ValueOf(p))
	}
	return reflectValues
}

type Job struct {
	ID         int        `json:"id"`
	RID        uuid.UUID  `json:"rid"`
	WorkerID   int        `json:"worker_id"`
	WorkerRID  uuid.UUID  `json:"worker_rid"`
	Options    Options    `json:"options"`
	TaskName   string     `json:"task_name"`
	Parameters Parameters `json:"parameters"`
	Status     string     `json:"status"`
	Attempts   int        `json:"attempts"`
	Results    Parameters `json:"result"`
	CreatedAt  time.Time  `json:"created_at"`
	UpdatedAt  time.Time  `json:"updated_at"`
}

type JobFromNotification struct {
	ID         int        `json:"id"`
	RID        uuid.UUID  `json:"rid"`
	WorkerID   int        `json:"worker_id"`
	WorkerRID  uuid.UUID  `json:"worker_rid"`
	TaskName   string     `json:"task_name"`
	Parameters Parameters `json:"parameters"`
	Status     string     `json:"status"`
	Attempts   int        `json:"attempts"`
	Result     Parameters `json:"result"`
	CreatedAt  DBTime     `json:"created_at"`
	UpdatedAt  DBTime     `json:"updated_at"`
}

func (jn *JobFromNotification) ToJob() *Job {
	return &Job{
		ID:         jn.ID,
		RID:        jn.RID,
		WorkerID:   jn.WorkerID,
		WorkerRID:  jn.WorkerRID,
		TaskName:   jn.TaskName,
		Parameters: jn.Parameters,
		Status:     jn.Status,
		Attempts:   jn.Attempts,
		CreatedAt:  jn.CreatedAt.Time,
		UpdatedAt:  jn.UpdatedAt.Time,
	}
}

type DBTime struct {
	time.Time
}

const dbTimeLayoutWithoutZeroes = "2006-01-02T15:04:05."
const dbTimeLayout = "2006-01-02T15:04:05.000000"

func (ct *DBTime) UnmarshalJSON(b []byte) error {
	s := strings.Trim(string(b), "\"")
	if s == "null" {
		ct.Time = time.Time{}
		return nil
	}

	tSplit := strings.Split(s, ".")
	if len(tSplit) != 2 {
		return fmt.Errorf("invalid time format: %s", s)
	}

	var err error
	ct.Time, err = time.Parse(dbTimeLayoutWithoutZeroes+strings.Repeat("0", len(tSplit[1])), s)
	if err != nil {
		return fmt.Errorf("error parsing time: %s, error: %w", s, err)
	}

	return nil
}

func (ct *DBTime) MarshalJSON() ([]byte, error) {
	if ct.Time.IsZero() {
		return []byte("null"), nil
	}
	return []byte(fmt.Sprintf("\"%s\"", ct.Time.Format(dbTimeLayout))), nil
}

var nilTime = (time.Time{}).UnixNano()

func (ct *DBTime) IsSet() bool {
	return !ct.IsZero()
}
