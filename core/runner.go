package core

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"sync"
	"time"

	"github.com/siherrmann/queuer/helper"
	"github.com/siherrmann/queuer/model"
	vh "github.com/siherrmann/validator/helper"
)

type Runner struct {
	cancel     context.CancelFunc
	cancelMu   sync.RWMutex
	Options    *model.Options
	Task       interface{}
	Parameters model.Parameters
	// Result channel to return results
	ResultsChannel chan []interface{}
	ErrorChannel   chan error
}

// NewRunner creates a new Runner instance for the specified task and parameters.
// It checks if the task and parameters are valid and returns a pointer to the new Runner instance.
// It returns an error if the task or parameters are invalid.
func NewRunner(options *model.Options, task interface{}, parameters ...interface{}) (*Runner, error) {
	taskInputParameters, err := helper.GetInputParametersFromTask(task)
	if err != nil {
		return nil, helper.NewError("getting task input parameters", err)
	} else if len(taskInputParameters) != len(parameters) {
		return nil, fmt.Errorf("task expects %d parameters, got %d", len(taskInputParameters), len(parameters))
	}

	for i, param := range parameters {
		paramConverted, err := vh.AnyToType(param, taskInputParameters[i])
		if err != nil {
			return nil, fmt.Errorf("error converting parameter %d to type %s: %v", i, taskInputParameters[i].Kind(), err)
		}
		parameters[i] = paramConverted
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

// NewRunnerFromJob creates a new Runner instance from a job.
// It initializes the runner with the job's task and parameters.
// It returns a pointer to the new Runner instance or an error if the job is invalid.
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
// The main intended use of this function is to run the task in a separate goroutine with panic recovery.
// It uses a context to manage cancellation and timeout.
// If the context is done, it will cancel the task and return an error.
// The context's timeout is set based on the OnError options if provided, otherwise it uses a cancelable context.
func (r *Runner) Run(ctx context.Context) {
	var cancel context.CancelFunc
	if r.Options != nil && r.Options.OnError != nil && r.Options.OnError.Timeout > 0 {
		ctx, cancel = context.WithTimeout(
			ctx,
			time.Duration(math.Round(r.Options.OnError.Timeout*1000))*time.Millisecond,
		)
	} else {
		ctx, cancel = context.WithCancel(ctx)
	}

	// Store the cancel function safely
	r.cancelMu.Lock()
	r.cancel = cancel
	r.cancelMu.Unlock()

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
			errorChannel <- helper.NewError("getting task output parameters", err)
			return
		}

		var ok bool
		if len(resultValues) > 0 {
			if err, ok = resultValues[len(resultValues)-1].(error); ok || (outputParameters[1].String() == "error" && resultValues[len(resultValues)-1] == nil) {
				resultValues = resultValues[:len(resultValues)-1]
			}
		}

		if err != nil {
			errorChannel <- err
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
	r.cancelMu.RLock()
	cancel := r.cancel
	r.cancelMu.RUnlock()

	if cancel != nil {
		cancel()
	}
}
