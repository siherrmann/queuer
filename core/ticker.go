package core

import (
	"context"
	"fmt"
	"log"
	"queuer/helper"
	"reflect"
	"time"
)

// Ticker represents a recurring task runner.
type Ticker struct {
	interval time.Duration
	runner   *Runner
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

	runner, err := NewRunner(nil, task, parameters...)
	if err != nil {
		return nil, fmt.Errorf("error creating runner: %v", err)
	}

	return &Ticker{
		interval: interval,
		runner:   runner,
	}, nil
}

// Go starts the Ticker. It runs the task at the specified interval
// until the provided context is cancelled.
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
