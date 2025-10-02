package model

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewWorker(t *testing.T) {
	tests := []struct {
		name           string
		workerName     string
		maxConcurrency int
		wantErr        bool
	}{
		{
			name:           "Valid worker",
			workerName:     "ExampleWorker",
			maxConcurrency: 5,
			wantErr:        false,
		},
		{
			name:           "Invalid worker name (empty)",
			workerName:     "",
			maxConcurrency: 5,
			wantErr:        true,
		},
		{
			name:           "Invalid worker name (too long)",
			workerName:     "WorkerMockWithNameLonger100_3032343638404244464850525456586062646668707274767880828486889092949698100",
			maxConcurrency: 5,
			wantErr:        true,
		},
		{
			name:           "Invalid max concurrency (too low)",
			workerName:     "ExampleWorker",
			maxConcurrency: 0,
			wantErr:        true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			w, err := NewWorker(test.workerName, test.maxConcurrency)
			if test.wantErr {
				assert.Error(t, err, "NewWorker should return an error for invalid options")
				assert.Nil(t, w, "Worker should be nil for invalid options")
			} else {
				assert.NoError(t, err, "NewWorker should not return an error for valid options")
				require.NotNil(t, w, "Worker should not be nil for valid options")
				assert.Equal(t, test.workerName, w.Name, "Worker name should match the provided name")
				assert.Equal(t, test.maxConcurrency, w.MaxConcurrency, "Worker max concurrency should match the provided value")
				assert.Equal(t, WorkerStatusReady, w.Status, "Worker status should be READY by default")
			}
		})
	}
}

func TestNewWorkerWithOptions(t *testing.T) {
	tests := []struct {
		name           string
		workerName     string
		maxConcurrency int
		options        *OnError
		wantErr        bool
	}{
		{
			name:           "Valid worker with options",
			workerName:     "ExampleWorker",
			maxConcurrency: 5,
			options: &OnError{
				Timeout:      1,
				MaxRetries:   3,
				RetryDelay:   2,
				RetryBackoff: RETRY_BACKOFF_LINEAR,
			},
			wantErr: false,
		},
		{
			name:           "Invalid worker name (empty)",
			workerName:     "",
			maxConcurrency: 5,
			options: &OnError{
				Timeout:      1,
				MaxRetries:   3,
				RetryDelay:   2,
				RetryBackoff: RETRY_BACKOFF_LINEAR,
			},
			wantErr: true,
		},
		{
			name:           "Invalid max concurrency (too high)",
			workerName:     "ExampleWorker",
			maxConcurrency: 1001,
			options: &OnError{
				Timeout:      1,
				MaxRetries:   3,
				RetryDelay:   2,
				RetryBackoff: RETRY_BACKOFF_LINEAR,
			},
			wantErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			w, err := NewWorkerWithOptions(test.workerName, test.maxConcurrency, test.options)
			if test.wantErr {
				assert.Error(t, err, "NewWorkerWithOptions should return an error for invalid options")
				assert.Nil(t, w, "Worker should be nil for invalid options")
			} else {
				assert.NoError(t, err, "NewWorkerWithOptions should not return an error for valid options")
				require.NotNil(t, w, "Worker should not be nil for valid options")
				assert.Equal(t, test.workerName, w.Name, "Worker name should match the provided name")
				assert.Equal(t, test.maxConcurrency, w.MaxConcurrency, "Worker max concurrency should match the provided value")
				assert.Equal(t, WorkerStatusReady, w.Status, "Worker status should be READY by default")
				assert.Equal(t, test.options, w.Options, "Worker options should match the provided options")
			}
		})
	}
}
