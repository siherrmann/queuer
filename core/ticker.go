package core

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/siherrmann/queuer/helper"
)

// Ticker represents a recurring task runner.
type Ticker struct {
	interval time.Duration
	runner   *Runner
}

// NewTicker creates and returns a new Ticker instance.
// It initializes the ticker with a specified interval and a task to run.
// The task must be valid and compatible with the provided parameters.
// It returns a pointer to the new Ticker instance or an error if the interval, task or parameters are invalid.
func NewTicker(interval time.Duration, task interface{}, parameters ...interface{}) (*Ticker, error) {
	if interval <= 0 {
		return nil, fmt.Errorf("ticker interval must be greater than zero")
	}

	err := helper.CheckValidTaskWithParameters(task, parameters...)
	if err != nil {
		return nil, helper.NewError("checking task with parameters", err)
	}

	runner, err := NewRunner(nil, task, parameters...)
	if err != nil {
		return nil, helper.NewError("creating runner", err)
	}

	return &Ticker{
		interval: interval,
		runner:   runner,
	}, nil
}

// Go starts the Ticker. It runs the task at the specified interval
// until the provided context is cancelled.
// It uses a ticker to trigger the task execution at the specified interval.
// If the context is done, it will stop the ticker and return.
// The task is run in a separate goroutine to avoid blocking the ticker.
// If the task returns an error, it will log the error.
// The ticker will continue to run until the context is cancelled or an error occurs.
func (t *Ticker) Go(ctx context.Context) {
	go t.runner.Run(ctx)

	ticker := time.NewTicker(t.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			go t.runner.Run(ctx)
		case err := <-t.runner.ErrorChannel:
			if err != nil {
				// TODO also implement error channel?
				log.Printf("Error running task: %v", err)
				return
			}
		}
	}
}
