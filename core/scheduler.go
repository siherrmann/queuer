package core

import (
	"fmt"
	"log"
	"queuer/model"
	"reflect"
	"time"
)

type Scheduler struct {
	Task      interface{}
	StartTime *time.Time
}

func NewScheduler(task interface{}, startTime *time.Time) (*Scheduler, error) {
	if reflect.ValueOf(task).Kind() != reflect.Func {
		return nil, fmt.Errorf("task must be a function, got %s", reflect.TypeOf(task).Kind())
	}

	return &Scheduler{
		StartTime: startTime,
		Task:      task,
	}, nil
}

func (s *Scheduler) Go(parameters model.Parameters) {
	var duration time.Duration
	if s.StartTime != nil {
		duration = s.StartTime.Sub(time.Now())
	}

	go func() {
		log.Printf("Scheduler will run task after %v", duration)
		time.AfterFunc(duration, func() {
			log.Printf("Running task %s with parameters: %v", reflect.TypeOf(s.Task).Name(), parameters)
			reflect.ValueOf(s.Task).Call(parameters.ToReflectValues())
		})
	}()
}
