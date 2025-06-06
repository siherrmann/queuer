package core

import (
	"context"
	"fmt"
	"math"
	"queuer/helper"
	"queuer/model"
	"reflect"
	"time"
)

type Runner struct {
	cancel     context.CancelFunc
	Options    *model.Options
	Task       interface{}
	Parameters model.Parameters
	// Result channel to return results
	ResultsChannel chan []interface{}
	ErrorChannel   chan error
}

func NewRunner(options *model.Options, task interface{}, parameters ...interface{}) (*Runner, error) {
	taskInputParameters, err := helper.GetInputParametersFromTask(task)
	if err != nil {
		return nil, fmt.Errorf("error getting input parameters of task: %v", err)
	} else if len(taskInputParameters) != len(parameters) {
		return nil, fmt.Errorf("task expects %d parameters, got %d", len(taskInputParameters), len(parameters))
	}

	for i, param := range parameters {
		// Convert json float to int if the parameter is int
		if taskInputParameters[i].Kind() == reflect.Int && reflect.TypeOf(param).Kind() == reflect.Float64 {
			parameters[i] = int(param.(float64))
		} else if taskInputParameters[i].Kind() != reflect.TypeOf(param).Kind() {
			return nil, fmt.Errorf("parameter %d of task must be of type %s, got %s", i, taskInputParameters[i].Kind(), reflect.TypeOf(param).Kind())
		}
	}

	err = helper.CheckValidTaskWithParameters(task, parameters...)
	if err != nil {
		return nil, fmt.Errorf("error checking task: %v", err)
	}

	return &Runner{
		Task:           task,
		Parameters:     model.Parameters(parameters),
		Options:        options,
		ResultsChannel: make(chan []interface{}, 1),
		ErrorChannel:   make(chan error, 1),
	}, nil
}

func NewRunnerFromJob(task *model.Task, job *model.Job) (*Runner, error) {
	if task == nil || job == nil {
		return nil, fmt.Errorf("task and job cannot be nil")
	}

	parameters := make([]interface{}, len(job.Parameters))
	copy(parameters, job.Parameters)

	runner, err := NewRunner(job.Options, task.Task, job.Parameters...)
	if err != nil {
		return nil, fmt.Errorf("error creating runner from job: %v", err)
	}

	return runner, nil
}

// Run executes the task with the provided parameters.
// It will return results on ResultsChannel and errors on ErrorChannel.
// If the task panics, it will send the panic value to ErrorChannel.
// The main intended use of this function is to run the task in a separate goroutine
func (r *Runner) Run(ctx context.Context) {
	if r.Options != nil && r.Options.OnError != nil && r.Options.OnError.Timeout > 0 {
		ctx, r.cancel = context.WithTimeout(
			ctx,
			time.Duration(math.Round(r.Options.OnError.Timeout*1000))*time.Millisecond,
		)
	} else {
		ctx, r.cancel = context.WithCancel(ctx)
	}

	resultsChannel := make(chan []interface{}, 1)
	errorChannel := make(chan error, 1)
	panicChan := make(chan interface{})

	go func() {
		defer func() {
			if p := recover(); p != nil {
				panicChan <- p
			}
		}()

		taskFunc := reflect.ValueOf(r.Task)
		results := taskFunc.Call(r.Parameters.ToReflectValues())
		resultValues := []interface{}{}
		for _, result := range results {
			resultValues = append(resultValues, result.Interface())
		}

		outputParameters, err := helper.GetOutputParametersFromTask(r.Task)
		if err != nil {
			errorChannel <- fmt.Errorf("error getting output parameters of task: %v", err)
			return
		}

		var ok bool
		if len(resultValues) > 0 {
			if err, ok = resultValues[len(resultValues)-1].(error); ok || (outputParameters[1].String() == "error" && resultValues[len(resultValues)-1] == nil) {
				resultValues = resultValues[:len(resultValues)-1]
			}
		}

		if err != nil {
			errorChannel <- fmt.Errorf("runner failed with error: %v", err)
		} else {
			resultsChannel <- resultValues
		}
	}()

	for {
		select {
		case p := <-panicChan:
			r.ErrorChannel <- fmt.Errorf("panic running task: %v", p)
			return
		case err := <-errorChannel:
			r.ErrorChannel <- fmt.Errorf("error running task: %v", err)
			return
		case results := <-resultsChannel:
			r.ResultsChannel <- results
			return
		case <-ctx.Done():
			r.ErrorChannel <- fmt.Errorf("error running task: %v", ctx.Err())
			return
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
