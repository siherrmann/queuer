package core

import (
	"context"
	"fmt"
	"log"
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
func NewTicker(task interface{}, interval time.Duration, parameters ...interface{}) (*Ticker, error) {
	if interval <= 0 {
		return nil, fmt.Errorf("ticker interval must be greater than zero")
	}

	if helper.IsValidTaskWithParameters(task, parameters...) {
		return nil, fmt.Errorf("task must be a function, got %s", reflect.TypeOf(task).Kind())
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
	errorChan := make(chan error, 1)
	go CancelableGo(
		ctx,
		func() {
			reflect.ValueOf(t.task).Call(t.parameters.ToReflectValues())
		},
		errorChan,
	)

	ticker := time.NewTicker(t.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("context cancelled: %w", ctx.Err())
		case <-ticker.C:
			log.Printf("Ticker ticked. Running task...")
			go CancelableGo(
				ctx,
				func() {
					reflect.ValueOf(t.task).Call(t.parameters.ToReflectValues())
				},
				errorChan,
			)
		case err := <-errorChan:
			if err != nil {
				return fmt.Errorf("error running task: %w", err)
			}
		}
	}
}
