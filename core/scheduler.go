package core

import (
	"context"
	"log"
	"reflect"
	"time"

	"github.com/siherrmann/queuer/helper"
	"github.com/siherrmann/queuer/model"
)

type Scheduler struct {
	Task       interface{}
	Parameters model.Parameters
	StartTime  *time.Time
}

// NewScheduler creates a new Scheduler instance for the specified task and parameters.
// It checks if the task and parameters are valid and returns a pointer to the new Scheduler instance.
// It returns an error if the task or parameters are invalid.
func NewScheduler(startTime *time.Time, task interface{}, parameters ...interface{}) (*Scheduler, error) {
	err := helper.CheckValidTaskWithParameters(task, parameters...)
	if err != nil {
		return nil, helper.NewError("checking task with parameters", err)
	}

	return &Scheduler{
		Task:       task,
		Parameters: model.Parameters(parameters),
		StartTime:  startTime,
	}, nil
}

// Go starts the scheduler to run the task at the specified start time.
// It creates a new Runner instance and runs the task after the specified duration.
// If the start time is nil, it runs the task immediately.
// It uses a context to manage cancellation and timeout.
// If the context is done, it will cancel the task and return an error.
// The context's timeout is set based on the OnError options if provided, otherwise it uses a cancelable context.
func (s *Scheduler) Go(ctx context.Context) {
	var duration time.Duration
	if s.StartTime != nil {
		duration = time.Until(*s.StartTime)
	}

	runner, err := NewRunner(
		nil,
		func() {
			time.AfterFunc(duration, func() {
				reflect.ValueOf(s.Task).Call(s.Parameters.ToReflectValues())
			})
		},
	)
	if err != nil {
		log.Printf("Error creating runner: %v", err)
		return
	}

	go runner.Run(ctx)
}
