package model

import (
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TaskMock is a mock function that simulates a task.
// It takes an integer duration and a string parameter, simulates some work,
// and returns an integer result and an error if the string cannot be converted to an integer.
func TaskMock(duration int, param2 string) (int, error) {
	// Simulate some work
	time.Sleep(time.Duration(duration) * time.Second)

	// Example for some error handling
	param2Int, err := strconv.Atoi(param2)
	if err != nil {
		return 0, err
	}

	return duration + param2Int, nil
}

func TaskMockWithNameLonger100_283032343638404244464850525456586062646668707274767880828486889092949698100() string {
	return "Uff"
}

func TestNewJob(t *testing.T) {
	tests := []struct {
		name       string
		task       interface{}
		options    *Options
		parameters []interface{}
		wantErr    bool
	}{
		{
			name:       "Valid nil options",
			task:       TaskMock,
			options:    nil,
			parameters: []interface{}{1, "2"},
			wantErr:    false,
		},
		{
			name: "Valid options",
			task: TaskMock,
			options: &Options{
				OnError: &OnError{
					MaxRetries:   3,
					RetryDelay:   1,
					RetryBackoff: RETRY_BACKOFF_NONE,
				},
				Schedule: &Schedule{
					Start:    time.Now().Add(1 * time.Minute),
					Interval: 2 * time.Minute,
					MaxCount: 5,
				},
			},
			parameters: []interface{}{1, "2"},
			wantErr:    false,
		},
		{
			name: "Valid OnError options",
			task: TaskMock,
			options: &Options{
				OnError: &OnError{
					MaxRetries:   3,
					RetryDelay:   1,
					RetryBackoff: RETRY_BACKOFF_NONE,
				},
			},
			parameters: []interface{}{1, "2"},
			wantErr:    false,
		},
		{
			name: "Invalid OnError options",
			task: TaskMock,
			options: &Options{
				OnError: &OnError{
					MaxRetries:   -3,
					RetryDelay:   1,
					RetryBackoff: RETRY_BACKOFF_NONE,
				},
			},
			parameters: []interface{}{1, "2"},
			wantErr:    true,
		},
		{
			name: "Valid Schedule options",
			task: TaskMock,
			options: &Options{
				Schedule: &Schedule{
					Start:    time.Now().Add(1 * time.Minute),
					Interval: 2 * time.Minute,
					MaxCount: 5,
				},
			},
			parameters: []interface{}{1, "2"},
			wantErr:    false,
		},
		{
			name: "Invalid Schedule options",
			task: TaskMock,
			options: &Options{
				Schedule: &Schedule{
					Start:    time.Now().Add(1 * time.Minute),
					Interval: -2 * time.Minute,
					MaxCount: 5,
				},
			},
			parameters: []interface{}{1, "2"},
			wantErr:    true,
		},
		{
			name:       "Invalid task with long name",
			task:       TaskMockWithNameLonger100_283032343638404244464850525456586062646668707274767880828486889092949698100,
			options:    nil,
			parameters: []interface{}{},
			wantErr:    true, // Expecting an error due to task name length
		},
		{
			name:       "Invalid task type",
			task:       123, // Invalid task type
			options:    nil,
			parameters: []interface{}{1, "2"},
			wantErr:    true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			job, err := NewJob(test.task, test.options, test.parameters...)
			if test.wantErr {
				assert.Error(t, err, "NewJob should return an error for invalid options")
				assert.Nil(t, job, "Job should be nil for invalid options")
			} else {
				assert.NoError(t, err, "NewJob should not return an error for valid options")
				require.NotNil(t, job, "Job should not be nil for valid options")
				assert.Equal(t, test.options, job.Options, "Job options should match the provided options")
				assert.Equal(t, test.parameters, job.Parameters.ToInterfaceSlice(), "Job parameters should match the provided parameters")
			}
		})
	}
}
