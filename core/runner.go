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
	ctx    context.Context
	cancel context.CancelFunc
	task   *model.Task
	job    *model.Job
}

func NewRunner(task *model.Task, job *model.Job) (*Runner, error) {
	var ctx context.Context
	var cancel context.CancelFunc
	if job.Options.Timeout > 0 {
		ctx, cancel = context.WithTimeout(
			context.Background(),
			time.Duration(math.Round(job.Options.Timeout*1000))*time.Millisecond,
		)
		defer cancel()
	} else {
		ctx, cancel = context.WithCancel(context.Background())
		defer cancel()
	}

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
		ctx:    ctx,
		cancel: cancel,
	}, nil
}

func (r *Runner) Run() ([]interface{}, error) {
	// Run the task function with the parameters
	taskFunc := reflect.ValueOf(r.task.Task)
	results := taskFunc.Call(r.job.Parameters.ToReflectValues())
	resultValues := []interface{}{}
	for _, result := range results {
		resultValues = append(resultValues, result.Interface())
	}

	var err error
	var ok bool
	if err, ok = resultValues[len(resultValues)-1].(error); ok && len(resultValues) > 0 {
		resultValues = resultValues[:len(resultValues)-1]
	}

	if err != nil {
		return []interface{}{}, fmt.Errorf("task %s failed with error: %v", r.job.TaskName, err)
	} else {
		return resultValues, nil
	}
}
