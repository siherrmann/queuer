package core

import (
	"context"
	"fmt"
	"math"
	"queuer/model"
	"reflect"
	"time"
)

type Runner struct {
	cancel context.CancelFunc
	task   *model.Task
	job    *model.Job
	// Result channel to return results
	resultsChannel chan []interface{}
	errorChannel   chan error
}

func NewRunner(task *model.Task, job *model.Job) (*Runner, error) {
	if len(job.Parameters) != len(task.InputParameters) {
		return nil, fmt.Errorf("task %s requires %d parameters, got %d", job.TaskName, len(task.InputParameters), len(job.Parameters))
	}

	for i, param := range job.Parameters {
		// Convert json float to int if the parameter is int
		if task.InputParameters[i].Kind() == reflect.Int && reflect.TypeOf(param).Kind() == reflect.Float64 {
			job.Parameters[i] = int(param.(float64))
		} else if task.InputParameters[i].Kind() != reflect.TypeOf(param).Kind() {
			return nil, fmt.Errorf("parameter %d of task %s must be of type %s, got %s", i, job.TaskName, task.InputParameters[i].Kind(), reflect.TypeOf(param).Kind())
		}
	}

	return &Runner{
		task:           task,
		job:            job,
		resultsChannel: make(chan []interface{}, 1),
		errorChannel:   make(chan error, 1),
	}, nil
}

func (r *Runner) Run(ctx context.Context) ([]interface{}, error) {
	var ctxRunner context.Context
	if r.job.Options != nil && r.job.Options.OnError.Timeout > 0 {
		ctxRunner, r.cancel = context.WithTimeout(
			ctx,
			time.Duration(math.Round(r.job.Options.OnError.Timeout*1000))*time.Millisecond,
		)
	} else {
		ctxRunner, r.cancel = context.WithCancel(ctx)
	}
	defer r.Cancel()

	panicChan := make(chan interface{}, 1)

	go func() {
		defer func() {
			if p := recover(); p != nil {
				panicChan <- p
			}
		}()

		// Run the task function with the parameters
		taskFunc := reflect.ValueOf(r.task.Task)
		results := taskFunc.Call(r.job.Parameters.ToReflectValues())
		resultValues := []interface{}{}
		for _, result := range results {
			resultValues = append(resultValues, result.Interface())
		}

		var err error
		var ok bool
		if err, ok = resultValues[len(resultValues)-1].(error); len(resultValues) > 0 && (ok || (len(r.task.OutputParameters) > 0 && r.task.OutputParameters[1].String() == "error" && resultValues[len(resultValues)-1] == nil)) {
			resultValues = resultValues[:len(resultValues)-1]
		}

		if err != nil {
			r.errorChannel <- fmt.Errorf("task %s failed with error: %v", r.job.TaskName, err)
		} else {
			r.resultsChannel <- resultValues
		}
	}()

	for {
		select {
		case err := <-r.errorChannel:
			r.Cancel()
			return nil, fmt.Errorf("error running task %s: %v", r.job.TaskName, err)
		case p := <-panicChan:
			r.Cancel()
			return nil, fmt.Errorf("task %s panicked: %v", r.job.TaskName, p)
		case results := <-r.resultsChannel:
			r.Cancel()
			return results, nil
		case <-ctxRunner.Done():
			return nil, fmt.Errorf("error running task %s: %v", r.job.TaskName, ctxRunner.Err())
		}
	}
}

func (r *Runner) Cancel(onCancel ...func()) {
	if len(onCancel) > 0 {
		for _, cancelFunc := range onCancel {
			if cancelFunc != nil {
				cancelFunc()
			}
		}
	}

	// Cancel the context if it exists
	if r.cancel != nil {
		r.cancel()
	}
}
