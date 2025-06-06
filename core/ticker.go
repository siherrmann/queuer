package core

import (
	"context"
	"fmt"
	"queuer/helper"
	"queuer/model"
	"reflect"
	"time"
)

// Ticker represents a recurring task runner.
type Ticker struct {
	interval   time.Duration
	task       interface{}
	parameters model.Parameters
}

// NewTicker creates and returns a new Ticker instance.
func NewTicker(interval time.Duration, task interface{}, parameters ...interface{}) (*Ticker, error) {
	if interval <= 0 {
		return nil, fmt.Errorf("ticker interval must be greater than zero")
	}

	err := helper.CheckValidTaskWithParameters(task, parameters...)
	if err != nil {
		return nil, fmt.Errorf("error checking task: %s", reflect.TypeOf(task).Kind())
	}

	return &Ticker{
		interval:   interval,
		task:       task,
		parameters: model.Parameters(parameters),
	}, nil
}

// Go starts the Ticker. It runs the task at the specified interval
// until the provided context is cancelled.
func (t *Ticker) Go(ctx context.Context) error {
	runner, err := NewRunner(nil, t.task, t.parameters...)
	if err != nil {
		return fmt.Errorf("error creating runner: %v", err)
	}

	go runner.Run(ctx)

	ticker := time.NewTicker(t.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("context cancelled: %w", ctx.Err())
		case <-ticker.C:
			go runner.Run(ctx)
		case err := <-runner.ErrorChannel:
			if err != nil {
				return fmt.Errorf("error running task: %w", err)
			}
		}
	}
}
