package core

import (
	"fmt"
	"reflect"
	"time"
)

type Scheduler struct {
	Task  interface{}
	After time.Duration
}

func NewScheduler(task interface{}, startTime *time.Time) (*Scheduler, error) {
	if reflect.ValueOf(task).Kind() != reflect.Func {
		return nil, fmt.Errorf("task must be a function, got %s", reflect.TypeOf(task).Kind())
	}

	var duration time.Duration
	now := time.Now()
	if startTime != nil {
		duration = startTime.Sub(now)
	}

	return &Scheduler{
		After: duration,
		Task:  task,
	}, nil
}

func (s *Scheduler) Go() {
	go func() {
		timer := time.AfterFunc(s.After, func() {
			reflect.ValueOf(s.Task).Call([]reflect.Value{})
		})
		defer timer.Stop()
	}()
}
