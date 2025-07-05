package model

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"queuer/helper"
	"reflect"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	// Job statuses before processing
	JobStatusQueued    = "QUEUED"
	JobStatusScheduled = "SCHEDULED"
	// Running status is used when the job is being processed by a worker.
	JobStatusRunning = "RUNNING"
	// Job statuses after processing
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
	if r == nil {
		return []reflect.Value{}
	}

	reflectValues := []reflect.Value{}
	for _, p := range *r {
		reflectValues = append(reflectValues, reflect.ValueOf(p))
	}

	return reflectValues
}

func (r *Parameters) ToInterfaceSlice() []interface{} {
	if r == nil {
		return []interface{}{}
	}

	interfaceSlice := make([]interface{}, len(*r))
	copy(interfaceSlice, *r)

	return interfaceSlice
}

type Job struct {
	ID          int        `json:"id"`
	RID         uuid.UUID  `json:"rid"`
	WorkerID    int        `json:"worker_id"`
	WorkerRID   uuid.UUID  `json:"worker_rid"`
	Options     *Options   `json:"options"`
	TaskName    string     `json:"task_name"`
	Parameters  Parameters `json:"parameters"`
	Status      string     `json:"status"`
	ScheduledAt *time.Time `json:"scheduled_at"`
	StartedAt   *time.Time `json:"started_at"`
	Attempts    int        `json:"attempts"`
	Results     Parameters `json:"result"`
	Error       string     `json:"error"`
	CreatedAt   time.Time  `json:"created_at"`
	UpdatedAt   time.Time  `json:"updated_at"`
}

func NewJob(task interface{}, options *Options, parameters ...interface{}) (*Job, error) {
	taskName, err := helper.GetTaskNameFromInterface(task)
	if err != nil {
		return nil, fmt.Errorf("error getting task name: %v", err)
	}

	if len(taskName) == 0 || len(taskName) > 100 {
		return nil, fmt.Errorf("taskName must have a length between 1 and 100")
	}

	if options != nil && options.OnError != nil {
		err := options.OnError.IsValid()
		if err != nil {
			return nil, fmt.Errorf("invalid OnError options: %v", err)
		}
	}
	if options != nil && options.Schedule != nil {
		err := options.Schedule.IsValid()
		if err != nil {
			return nil, fmt.Errorf("invalid Schedule options: %v", err)
		}
	}

	status := JobStatusQueued
	var scheduledAt time.Time
	if options != nil && options.Schedule != nil {
		status = JobStatusScheduled
		scheduledAt = options.Schedule.Start.UTC()
	}

	return &Job{
		TaskName:    taskName,
		Status:      status,
		ScheduledAt: &scheduledAt,
		Options:     options,
		Parameters:  parameters,
	}, nil
}

type JobFromNotification struct {
	ID          int        `json:"id"`
	RID         uuid.UUID  `json:"rid"`
	WorkerID    int        `json:"worker_id"`
	WorkerRID   uuid.UUID  `json:"worker_rid"`
	Options     *Options   `json:"options"`
	TaskName    string     `json:"task_name"`
	Parameters  Parameters `json:"parameters"`
	Status      string     `json:"status"`
	ScheduledAt DBTime     `json:"scheduled_at"`
	StartedAt   DBTime     `json:"started_at"`
	Attempts    int        `json:"attempts"`
	Results     Parameters `json:"result"`
	Error       string     `json:"error"`
	CreatedAt   DBTime     `json:"created_at"`
	UpdatedAt   DBTime     `json:"updated_at"`
}

func (jn *JobFromNotification) ToJob() *Job {
	return &Job{
		ID:          jn.ID,
		RID:         jn.RID,
		WorkerID:    jn.WorkerID,
		WorkerRID:   jn.WorkerRID,
		Options:     jn.Options,
		TaskName:    jn.TaskName,
		Parameters:  jn.Parameters,
		Status:      jn.Status,
		ScheduledAt: &jn.ScheduledAt.Time,
		StartedAt:   &jn.StartedAt.Time,
		Attempts:    jn.Attempts,
		Results:     jn.Results,
		Error:       jn.Error,
		CreatedAt:   jn.CreatedAt.Time,
		UpdatedAt:   jn.UpdatedAt.Time,
	}
}

type DBTime struct {
	time.Time
}

const dbTimeLayoutWithoutZeroes = "2006-01-02T15:04:05."
const dbTimeLayout = "2006-01-02T15:04:05.000000"

func (ct DBTime) MarshalJSON() ([]byte, error) {
	if ct.Time.IsZero() {
		return []byte("null"), nil
	}
	return []byte(fmt.Sprintf("\"%s\"", ct.Time.Format(dbTimeLayout))), nil
}

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

func (ct *DBTime) IsSet() bool {
	return !ct.IsZero()
}
