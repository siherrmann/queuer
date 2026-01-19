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

// Test structs for struct parameter tests
type TestConfig struct {
	Name  string
	Value int
}

type TestData struct {
	ID      int
	Message string
	Active  bool
}

// Nested struct that contains both a struct and pointer to struct
type NestedStruct struct {
	Config   TestConfig
	DataPtr  *TestData
	Metadata string
}

// Task function that accepts a struct and pointer to struct
func taskWithStructParams(config TestConfig, data *TestData) (string, error) {
	if data == nil {
		return "", fmt.Errorf("data cannot be nil")
	}
	return fmt.Sprintf("%s-%d: %s (ID: %d, Active: %t)", config.Name, config.Value, data.Message, data.ID, data.Active), nil
}

// Task function that accepts a nested struct with both struct and pointer fields
func taskWithNestedStruct(nested NestedStruct) (string, error) {
	if nested.DataPtr == nil {
		return "", fmt.Errorf("nested.DataPtr cannot be nil")
	}
	return fmt.Sprintf("%s: %s-%d | %s (ID: %d, Active: %t)", nested.Metadata, nested.Config.Name, nested.Config.Value, nested.DataPtr.Message, nested.DataPtr.ID, nested.DataPtr.Active), nil
}

// TestNewRunnerWithStructParameters tests the runner with struct and pointer-to-struct parameters
func TestNewRunnerWithStructParameters(t *testing.T) {
	tests := []struct {
		name       string
		param1     interface{}
		param2     interface{}
		wantErr    bool
		wantResult string
	}{
		{
			name: "Valid Struct and Pointer",
			param1: TestConfig{
				Name:  "config",
				Value: 42,
			},
			param2: &TestData{
				ID:      1,
				Message: "test message",
				Active:  true,
			},
			wantErr:    false,
			wantResult: "config-42: test message (ID: 1, Active: true)",
		},
		{
			name: "Map to Struct and Map to Pointer Conversion",
			param1: map[string]interface{}{
				"Name":  "fromMap",
				"Value": 99,
			},
			param2: map[string]interface{}{
				"ID":      2,
				"Message": "converted from map",
				"Active":  false,
			},
			wantErr:    false,
			wantResult: "fromMap-99: converted from map (ID: 2, Active: false)",
		},
		{
			name: "Struct Value and Map to Pointer",
			param1: TestConfig{
				Name:  "direct",
				Value: 10,
			},
			param2: map[string]interface{}{
				"ID":      3,
				"Message": "mixed params",
				"Active":  true,
			},
			wantErr:    false,
			wantResult: "direct-10: mixed params (ID: 3, Active: true)",
		},
		{
			name: "Map to Struct with Type Conversion",
			param1: map[string]interface{}{
				"Name":  "typeConv",
				"Value": 25.0, // float64 should convert to int
			},
			param2: map[string]interface{}{
				"ID":      4.0, // float64 should convert to int
				"Message": "type conversion test",
				"Active":  true,
			},
			wantErr:    false,
			wantResult: "typeConv-25: type conversion test (ID: 4, Active: true)",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mockTask, err := model.NewTask(taskWithStructParams)
			require.NoError(t, err, "Failed to create mock task")

			job := &model.Job{
				TaskName: mockTask.Name,
				Parameters: []interface{}{
					test.param1,
					test.param2,
				},
				Options: &model.Options{
					OnError: &model.OnError{
						Timeout: 5.0,
					},
				},
			}

			runner, err := NewRunnerFromJob(mockTask, job)
			if test.wantErr {
				assert.Error(t, err, "NewRunnerFromJob should return an error")
				return
			}

			require.NoError(t, err, "NewRunnerFromJob should not return an error")
			assert.NotNil(t, runner)

			go runner.Run(context.Background())

		outerLoop:
			for {
				select {
				case err := <-runner.ErrorChannel:
					assert.NoError(t, err, "Runner should not return an error")
					break outerLoop
				case results := <-runner.ResultsChannel:
					assert.NotNil(t, results)
					assert.Len(t, results, 1)
					assert.Equal(t, test.wantResult, results[0])
					break outerLoop
				case <-time.After(2 * time.Second):
					t.Fatal("Test timed out")
				}
			}
		})
	}
}

// TestNewRunnerWithNestedStruct tests the runner with nested structs containing both struct and pointer fields
func TestNewRunnerWithNestedStruct(t *testing.T) {
	tests := []struct {
		name       string
		param      interface{}
		wantErr    bool
		wantResult string
	}{
		{
			name: "Direct Nested Struct with Struct and Pointer",
			param: NestedStruct{
				Config: TestConfig{
					Name:  "nested-config",
					Value: 100,
				},
				DataPtr: &TestData{
					ID:      10,
					Message: "nested data",
					Active:  true,
				},
				Metadata: "meta-direct",
			},
			wantErr:    false,
			wantResult: "meta-direct: nested-config-100 | nested data (ID: 10, Active: true)",
		},
		{
			name: "Map to Nested Struct Conversion",
			param: map[string]interface{}{
				"Config": map[string]interface{}{
					"Name":  "map-config",
					"Value": 200,
				},
				"DataPtr": map[string]interface{}{
					"ID":      20,
					"Message": "map data",
					"Active":  false,
				},
				"Metadata": "meta-from-map",
			},
			wantErr:    false,
			wantResult: "meta-from-map: map-config-200 | map data (ID: 20, Active: false)",
		},
		{
			name: "Map with Type Conversions in Nested Struct",
			param: map[string]interface{}{
				"Config": map[string]interface{}{
					"Name":  "type-conv",
					"Value": 300.5, // float64 to int
				},
				"DataPtr": map[string]interface{}{
					"ID":      30.0, // float64 to int
					"Message": "type conversion",
					"Active":  true,
				},
				"Metadata": "meta-type-conv",
			},
			wantErr:    false,
			wantResult: "meta-type-conv: type-conv-300 | type conversion (ID: 30, Active: true)",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mockTask, err := model.NewTask(taskWithNestedStruct)
			require.NoError(t, err, "Failed to create mock task")

			job := &model.Job{
				TaskName: mockTask.Name,
				Parameters: []interface{}{
					test.param,
				},
				Options: &model.Options{
					OnError: &model.OnError{
						Timeout: 5.0,
					},
				},
			}

			runner, err := NewRunnerFromJob(mockTask, job)
			if test.wantErr {
				assert.Error(t, err, "NewRunnerFromJob should return an error")
				return
			}

			require.NoError(t, err, "NewRunnerFromJob should not return an error")
			assert.NotNil(t, runner)

			go runner.Run(context.Background())

		outerLoop:
			for {
				select {
				case err := <-runner.ErrorChannel:
					assert.NoError(t, err, "Runner should not return an error")
					break outerLoop
				case results := <-runner.ResultsChannel:
					assert.NotNil(t, results)
					assert.Len(t, results, 1)
					assert.Equal(t, test.wantResult, results[0])
					break outerLoop
				case <-time.After(2 * time.Second):
					t.Fatal("Test timed out")
				}
			}
		})
	}
}
