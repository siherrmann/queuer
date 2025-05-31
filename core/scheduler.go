package core

import (
	"context"
	"fmt"
	"queuer/helper"
	"queuer/model"
	"reflect"
	"time"
)

type Scheduler struct {
	Task       interface{}
	Parameters model.Parameters
	StartTime  *time.Time
}

func NewScheduler(task interface{}, startTime *time.Time, parameters ...interface{}) (*Scheduler, error) {
	if !helper.IsValidTaskWithParameters(task, parameters...) {
		return nil, fmt.Errorf("task must be a function and prameters must match the function signature")
	}

	return &Scheduler{
		Task:       task,
		Parameters: model.Parameters(parameters),
		StartTime:  startTime,
	}, nil
}

func (s *Scheduler) Go(ctx context.Context) {
	var duration time.Duration
	if s.StartTime != nil {
		duration = s.StartTime.Sub(time.Now())
	}

	errorChan := make(chan error, 1)
	go CancelableGo(
		ctx,
		func() {
			time.AfterFunc(duration, func() {
				reflect.ValueOf(s.Task).Call(s.Parameters.ToReflectValues())
			})
		},
		errorChan,
	)
}
