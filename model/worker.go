package model

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

const (
	WorkerStatusRunning = "RUNNING"
	WorkerStatusFailed  = "FAILED"
)

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
		Status:         WorkerStatusRunning,
	}, nil
}

func NewWorkerWithOptions(name string, maxConcurrency int, options *OnError) (*Worker, error) {
	err := options.IsValid()
	if err != nil {
		return nil, fmt.Errorf("invalid options: %w", err)
	}

	return &Worker{
		Name:           name,
		Options:        options,
		MaxConcurrency: maxConcurrency,
		Status:         WorkerStatusRunning,
	}, nil
}
