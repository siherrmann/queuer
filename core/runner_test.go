package core

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/siherrmann/queuer/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Mock function to simulate a task that might succeed or fail
type funcRunner struct {
	ctx          context.Context // Context for manual cancellation
	panic        bool            // If true, it will panic on call
	workDuration time.Duration   // How long each call should simulate work
	returnErr    error           // The error it returns when failing
}

func newFuncRunner(ctx context.Context, panic bool, workDuration time.Duration, errToReturn error) *funcRunner {
	return &funcRunner{
		ctx:          ctx,
		panic:        panic,
		workDuration: workDuration,
		returnErr:    errToReturn,
	}
}

func (m *funcRunner) Call(param1 int, param2 string) (int, error) {
	if m.panic {
		panic("FuncRunner panicked on call")
	}

	// Simulate work duration
	if m.workDuration > 0 {
		select {
		case <-time.After(m.workDuration):
			// Work completed
		case <-m.ctx.Done(): // Check for context cancellation
			return 0, fmt.Errorf("task %s canceled: %w", "mockTask", m.ctx.Err())
		}
	}

	// Error case if string conversion fails
	param2Int, err := strconv.Atoi(param2)
	if err != nil {
		return 0, m.returnErr
	}

	// Simulate successful return values (e.g., sum of two ints)
	return param1 + param2Int, nil
}

func (m *funcRunner) CallWithoutReturn(param1 int, param2 string) {
	if m.panic {
		panic("FuncRunner panicked on call")
	}

	// Simulate work duration
	if m.workDuration > 0 {
		select {
		case <-time.After(m.workDuration):
			// Work completed
		case <-m.ctx.Done(): // Check for context cancellation
			return
		}
	}

	// Error case if string conversion fails
	_, err := strconv.Atoi(param2)
	if err != nil {
		return
	}
}

// TestNewRunner tests the constructor's validation and parameter handling
func TestNewRunner(t *testing.T) {
	mockFn := newFuncRunner(context.Background(), false, 100*time.Millisecond, nil).Call
	mockTask, err := model.NewTask(mockFn)
	require.NoError(t, err, "Failed to create mock task")

	tests := []struct {
		name    string
		job     *model.Job
		task    *model.Task
		wantErr bool
	}{
		{
			name: "Valid Parameters",
			job: &model.Job{
				TaskName: mockTask.Name,
				Parameters: []interface{}{
					10,
					"20",
				},
			},
			task:    mockTask,
			wantErr: false,
		},
		{
			name: "Valid Parameter Type",
			job: &model.Job{
				TaskName: mockTask.Name,
				Parameters: []interface{}{
					10.0,     // Should convert to int
					"string", // Is vaild string
				},
			},
			task:    mockTask,
			wantErr: false,
		},
		{
			name: "Mismatched Parameter Count",
			job: &model.Job{
				TaskName: mockTask.Name,
				Parameters: []interface{}{
					10,
				},
			},
			task:    mockTask,
			wantErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runner, err := NewRunnerFromJob(test.task, test.job)
			if test.wantErr {
				assert.Error(t, err, "NewRunner should return an error")
			} else {
				assert.NoError(t, err, "NewRunner should not return an error")
				assert.NotNil(t, runner)
			}
		})
	}
}

// TestRunSuccess tests a task that completes successfully within timeout
func TestRunSuccess(t *testing.T) {
	mockFn := newFuncRunner(context.Background(), false, 100*time.Millisecond, nil).Call // Succeeds immediately, short work
	mockTask, err := model.NewTask(mockFn)
	require.NoError(t, err, "Failed to create mock task")

	job := &model.Job{
		TaskName: mockTask.Name,
		Parameters: []interface{}{
			5,
			"10",
		},
		Options: &model.Options{
			OnError: &model.OnError{
				Timeout: 5.0,
			},
		},
	}

	runner, err := NewRunnerFromJob(mockTask, job)
	require.NoError(t, err)

	go runner.Run(context.Background())

outerLoop:
	for {
		select {
		case err := <-runner.ErrorChannel:
			assert.NoError(t, err, "Runner should not return an error")
			break outerLoop
		case results := <-runner.ResultsChannel:
			assert.NoError(t, err)
			assert.NotNil(t, results)
			assert.Len(t, results, 1)
			assert.Equal(t, 15, results[0])
			break outerLoop
		}
	}
}

// TestRunFailure tests a task that fails within timeout
func TestRunFailure(t *testing.T) {
	expectedErr := fmt.Errorf("invalid int string")
	mockFn := newFuncRunner(context.Background(), false, 100*time.Millisecond, expectedErr).Call // Fails once
	mockTask, err := model.NewTask(mockFn)
	require.NoError(t, err, "Failed to create mock task")

	job := &model.Job{
		TaskName: mockTask.Name,
		Parameters: []interface{}{
			1,
			"string",
		},
		Options: &model.Options{
			OnError: &model.OnError{
				Timeout: 5.0,
			},
		},
	}

	runner, err := NewRunnerFromJob(mockTask, job)
	require.NoError(t, err)

	go runner.Run(context.Background())

outerLoop:
	for {
		select {
		case err := <-runner.ErrorChannel:
			assert.Error(t, err)
			assert.Contains(t, err.Error(), expectedErr.Error())
			break outerLoop
		case results := <-runner.ResultsChannel:
			assert.Nil(t, results)
			break outerLoop
		}
	}
}

// TestRunTimeout tests a task that exceeds its timeout
func TestRunTimeout(t *testing.T) {
	mockFn := newFuncRunner(context.Background(), false, 3*time.Second, nil).Call // Task takes 3 seconds
	mockTask, err := model.NewTask(mockFn)
	require.NoError(t, err, "Failed to create mock task")

	job := &model.Job{
		TaskName: mockTask.Name,
		Parameters: []interface{}{
			1,
			"2",
		},
		Options: &model.Options{
			OnError: &model.OnError{
				Timeout: 0.5,
			},
		},
	}

	runner, err := NewRunnerFromJob(mockTask, job)
	require.NoError(t, err)

	start := time.Now()
	go runner.Run(context.Background())

outerLoop:
	for {
		select {
		case err := <-runner.ErrorChannel:
			duration := time.Since(start)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "context deadline exceeded")
			assert.GreaterOrEqual(t, duration, 500*time.Millisecond)
			assert.Less(t, duration, 3*time.Second) // Should stop before task completes
			break outerLoop
		case results := <-runner.ResultsChannel:
			assert.Nil(t, results)
			break outerLoop
		}
	}
}

// TestRunParentContextCancel tests if Run respects parent context cancellation
func TestRunParentContextCancel(t *testing.T) {
	mockFn := newFuncRunner(context.Background(), false, 5*time.Second, nil).Call // Task takes 5 seconds
	mockTask, err := model.NewTask(mockFn)
	require.NoError(t, err, "Failed to create mock task")

	job := &model.Job{
		TaskName: mockTask.Name,
		Parameters: []interface{}{
			1,
			"2",
		},
		Options: &model.Options{
			OnError: &model.OnError{
				Timeout: 10.0,
			},
		},
	}

	runner, err := NewRunnerFromJob(mockTask, job)
	require.NoError(t, err)

	parentCtx, parentCancel := context.WithCancel(context.Background())
	defer parentCancel()

	go func() {
		time.Sleep(1 * time.Second) // Cancel parent after 1 second
		parentCancel()
	}()

	start := time.Now()
	go runner.Run(parentCtx) // Pass the cancellable parent context

outerLoop:
	for {
		select {
		case err := <-runner.ErrorChannel:
			duration := time.Since(start)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "context canceled")
			assert.GreaterOrEqual(t, duration, 1*time.Second)
			assert.Less(t, duration, 5*time.Second) // Should stop before task completes
			break outerLoop
		case results := <-runner.ResultsChannel:
			assert.Nil(t, results)
			break outerLoop
		}
	}
}

// TestRunTaskPanic tests a task that panics
func TestRunTaskPanic(t *testing.T) {
	mockFn := newFuncRunner(context.Background(), true, 5*time.Second, nil).Call // Panics on 1st call
	mockTask, err := model.NewTask(mockFn)
	require.NoError(t, err, "Failed to create mock task")

	job := &model.Job{
		TaskName: mockTask.Name,
		Parameters: []interface{}{
			5,
			"5",
		},
		Options: &model.Options{
			OnError: &model.OnError{
				Timeout: 5.0,
			},
		},
	}

	runner, err := NewRunnerFromJob(mockTask, job)
	require.NoError(t, err)

	go runner.Run(context.Background())

outerLoop:
	for {
		select {
		case err := <-runner.ErrorChannel:
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "panicked")
			break outerLoop
		case results := <-runner.ResultsChannel:
			assert.Nil(t, results)
			break outerLoop
		}
	}
}

// TestCancelMethodWithOnCancelFunc tests the onCancel callback
func TestCancelMethodWithOnCancelFunc(t *testing.T) {
	mockFn := newFuncRunner(context.Background(), false, 5*time.Second, nil).Call
	mockTask, err := model.NewTask(mockFn)
	require.NoError(t, err, "Failed to create mock task")

	job := &model.Job{
		TaskName: mockTask.Name,
		Parameters: []interface{}{
			1,
			"1",
		},
	}

	runner, err := NewRunnerFromJob(mockTask, job)
	require.NoError(t, err)

	ready := make(chan struct{})
	go func() {
		close(ready)
		runner.Run(context.Background())
	}()

	<-ready
	called := make(chan bool, 1)
	go func() {
		time.Sleep(500 * time.Millisecond)
		runner.Cancel(func() {
			called <- true
		})
	}()

outerLoop:
	for {
		select {
		case err := <-runner.ErrorChannel:
			assert.Error(t, err, "Runner should return an error on cancel")
			assert.Equal(t, <-called, true, "onCancelFunc should have been called")
			assert.Contains(t, err.Error(), "canceled", "Error should indicate cancellation")
			break outerLoop
		case results := <-runner.ResultsChannel:
			assert.Nil(t, results, "Results should be nil on cancel")
			break outerLoop
		}
	}
}

// TestCancelMethodWithoutReturnValues tests the onCancel callback
func TestCancelMethodWithoutReturnValues(t *testing.T) {
	mockFn := newFuncRunner(context.Background(), false, 5*time.Second, nil).CallWithoutReturn
	mockTask, err := model.NewTask(mockFn)
	require.NoError(t, err, "Failed to create mock task")

	job := &model.Job{
		TaskName: mockTask.Name,
		Parameters: []interface{}{
			1,
			"1",
		},
	}

	runner, err := NewRunnerFromJob(mockTask, job)
	require.NoError(t, err)

	go runner.Run(context.Background())

outerLoop:
	for {
		select {
		case err := <-runner.ErrorChannel:
			assert.NoError(t, err)
			break outerLoop
		case results := <-runner.ResultsChannel:
			assert.Empty(t, results)
			break outerLoop
		}
	}
}
