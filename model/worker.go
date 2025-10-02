package model

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

const (
	WorkerStatusReady   = "READY"
	WorkerStatusRunning = "RUNNING"
	WorkerStatusFailed  = "FAILED"
	WorkerStatusStopped = "STOPPED"
)

// Worker represents a worker that can execute tasks.
// It includes the worker's ID, name, options for error handling, maximum concurrency,
// available tasks, and status.
//
// ID, RID, Status, CreatedAt, and UpdatedAt are set automatically on creation.
//
// Parameters:
//   - Name is the name of the worker, which should be unique and descriptive.
//   - Options is an optional field that can be used to specify error handling options.
//     If the Job has options set, the Job options are used as primary options.
//   - MaxConcurrency is the maximum number of tasks that can be executed concurrently by the worker.
//   - AvailableTasks is a list of task names that the worker can execute.
//   - AvailableNextIntervalFuncs is a list of next interval functions that the worker can use for
type Worker struct {
	ID                         int       `json:"id"`
	RID                        uuid.UUID `json:"rid"`
	Name                       string    `json:"name"`
	Options                    *OnError  `json:"options,omitempty"`
	MaxConcurrency             int       `json:"max_concurrency"`
	AvailableTasks             []string  `json:"available_tasks,omitempty"`
	AvailableNextIntervalFuncs []string  `json:"available_next_interval,omitempty"`
	Status                     string    `json:"status"`
	CreatedAt                  time.Time `json:"created_at"`
	UpdatedAt                  time.Time `json:"updated_at"`
}

// NewWorker creates a new Worker with the specified name and maximum concurrency.
// It validates the name and maximum concurrency, and initializes the worker status to running.
// It returns a pointer to the new Worker instance or an error if something is invalid.
func NewWorker(name string, maxConcurrency int) (*Worker, error) {
	if len(name) == 0 || len(name) > 100 {
		return nil, fmt.Errorf("name must have a length between 1 and 100")
	}

	if maxConcurrency < 1 || maxConcurrency > 100 {
		return nil, fmt.Errorf("maxConcurrency must be between 1 and 100")
	}

	return &Worker{
		Name:           name,
		MaxConcurrency: maxConcurrency,
		Status:         WorkerStatusReady,
	}, nil
}

// NewWorkerWithOptions creates a new Worker with the specified name, maximum concurrency, and error handling options.
// It validates the name, maximum concurrency, and options, and initializes the worker status to running.
// It returns a pointer to the new Worker instance or an error if something is invalid.
func NewWorkerWithOptions(name string, maxConcurrency int, options *OnError) (*Worker, error) {
	if len(name) == 0 || len(name) > 100 {
		return nil, fmt.Errorf("name must have a length between 1 and 100")
	}

	if maxConcurrency < 1 || maxConcurrency > 1000 {
		return nil, fmt.Errorf("maxConcurrency must be between 1 and 100")
	}

	err := options.IsValid()
	if err != nil {
		return nil, fmt.Errorf("invalid options: %w", err)
	}

	return &Worker{
		Name:           name,
		Options:        options,
		MaxConcurrency: maxConcurrency,
		Status:         WorkerStatusReady,
	}, nil
}
