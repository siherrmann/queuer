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

type Scheduler struct {
	Task       interface{}
	Parameters model.Parameters
	StartTime  *time.Time
}

func NewScheduler(task interface{}, startTime *time.Time, parameters ...interface{}) (*Scheduler, error) {
	err := helper.CheckValidTaskWithParameters(task, parameters...)
	if err != nil {
		return nil, fmt.Errorf("error checking task: %s", reflect.TypeOf(task).Kind())
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
