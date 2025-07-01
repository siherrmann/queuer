package model

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIsValid(t *testing.T) {
	tests := []struct {
		name    string
		options *Options
		wantErr bool
	}{
		{
			name:    "Valid nil options",
			options: nil,
			wantErr: false,
		},
		{
			name: "Valid options",
			options: &Options{
				OnError: &OnError{
					MaxRetries:   3,
					RetryDelay:   1,
					RetryBackoff: RETRY_BACKOFF_LINEAR,
				},
				Schedule: &Schedule{
					Start:    time.Now().Add(1 * time.Minute),
					Interval: 1 * time.Minute,
					MaxCount: 3,
				},
			},
			wantErr: false,
		},
		{
			name: "Valid OnError options",
			options: &Options{
				OnError: &OnError{
					Timeout:      1.0,
					MaxRetries:   3,
					RetryDelay:   1,
					RetryBackoff: RETRY_BACKOFF_LINEAR,
				},
			},
			wantErr: false,
		},
		{
			name: "Valid Schedule options",
			options: &Options{
				Schedule: &Schedule{
					Start:    time.Now().Add(1 * time.Minute),
					Interval: 1 * time.Minute,
					MaxCount: 3,
				},
			},
			wantErr: false,
		},
		{
			name: "Invalid options with negative Timeout",
			options: &Options{
				OnError: &OnError{
					Timeout:      -1.0,
					MaxRetries:   3,
					RetryDelay:   1,
					RetryBackoff: RETRY_BACKOFF_LINEAR,
				},
			},
			wantErr: true,
		},
		{
			name: "Invalid options with negative MaxRetries",
			options: &Options{
				OnError: &OnError{
					Timeout:      1.0,
					MaxRetries:   -3,
					RetryDelay:   1,
					RetryBackoff: RETRY_BACKOFF_LINEAR,
				},
			},
			wantErr: true,
		},
		{
			name: "Invalid options with negative RetryDelay",
			options: &Options{
				OnError: &OnError{
					Timeout:      1.0,
					MaxRetries:   3,
					RetryDelay:   -1,
					RetryBackoff: RETRY_BACKOFF_LINEAR,
				},
			},
			wantErr: true,
		},
		{
			name: "Invalid options with non existent RetryBackoff",
			options: &Options{
				OnError: &OnError{
					Timeout:      1.0,
					MaxRetries:   3,
					RetryDelay:   1,
					RetryBackoff: "non_existent_backoff", // invalid value
				},
			},
			wantErr: true,
		},
		{
			name: "Invalid options with zero Start time in Schedule",
			options: &Options{
				Schedule: &Schedule{
					Start:    time.Time{},
					Interval: 1 * time.Minute,
					MaxCount: 3,
				},
			},
			wantErr: true,
		},
		{
			name: "Invalid options with zero Interval in Schedule",
			options: &Options{
				Schedule: &Schedule{
					Start:    time.Now().Add(1 * time.Minute),
					Interval: 0,
					MaxCount: 3,
				},
			},
			wantErr: true,
		},
		{
			name: "Invalid options with negative MaxCount in Schedule",
			options: &Options{
				Schedule: &Schedule{
					Start:    time.Now().Add(1 * time.Minute),
					Interval: 1 * time.Minute,
					MaxCount: -1,
				},
			},
			wantErr: true,
		},
		{
			name: "Invalid options with zero Interval and no NextInterval in Schedule",
			options: &Options{
				Schedule: &Schedule{
					Start:        time.Now().Add(1 * time.Minute),
					Interval:     0,
					MaxCount:     3,
					NextInterval: "",
				},
			},
			wantErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.options.IsValid()
			if test.wantErr {
				assert.Error(t, err, "Options.IsValid should return an error for invalid options")
			} else {
				assert.NoError(t, err, "Options.IsValid should not return an error for valid options")
			}
		})
	}
}

func TestMarshalAndUnmarshalJSON(t *testing.T) {
	options := &Options{
		OnError: &OnError{
			Timeout:      1.0,
			MaxRetries:   3,
			RetryDelay:   1,
			RetryBackoff: RETRY_BACKOFF_LINEAR,
		},
		Schedule: &Schedule{
			Start:        time.Now().Add(1 * time.Minute),
			Interval:     1 * time.Minute,
			MaxCount:     3,
			NextInterval: "NextIntervalFunction",
		},
	}

	// Options specific tests
	optionsMarshalled, err := options.Value()
	require.NoError(t, err, "Options.Value should not return an error")
	assert.NotEmpty(t, optionsMarshalled, "Options.Value should return non-empty JSON data")

	var unmarshalledOptions Options
	err = unmarshalledOptions.Scan(optionsMarshalled)
	assert.NoError(t, err, "Options.Scan should not return an error")
	require.NotNil(t, unmarshalledOptions.OnError, "Unmarshalled OnError should not be nil")
	require.NotNil(t, unmarshalledOptions.Schedule, "Unmarshalled Schedule should not be nil")

	assert.Equal(t, options.OnError.MaxRetries, unmarshalledOptions.OnError.MaxRetries, "Unmarshalled MaxRetries should match original")
	assert.Equal(t, options.OnError.RetryDelay, unmarshalledOptions.OnError.RetryDelay, "Unmarshalled RetryDelay should match original")
	assert.Equal(t, options.OnError.Timeout, unmarshalledOptions.OnError.Timeout, "Unmarshalled Timeout should match original")
	assert.Equal(t, options.OnError.RetryBackoff, unmarshalledOptions.OnError.RetryBackoff, "Unmarshalled RetryBackoff should match original")
	assert.Equal(t, options.Schedule.Start.Unix(), unmarshalledOptions.Schedule.Start.Unix(), "Unmarshalled Start should match original")
	assert.Equal(t, options.Schedule.Interval, unmarshalledOptions.Schedule.Interval, "Unmarshalled Interval should match original")
	assert.Equal(t, options.Schedule.MaxCount, unmarshalledOptions.Schedule.MaxCount, "Unmarshalled MaxCount should match original")
	assert.Equal(t, options.Schedule.NextInterval, unmarshalledOptions.Schedule.NextInterval, "Unmarshalled NextInterval should match original")

	// OnError specific tests
	onErrorMarshalled, err := options.OnError.Value()
	require.NoError(t, err, "OnError.Value should not return an error")
	require.NotEmpty(t, onErrorMarshalled, "OnError.Value should return non-empty JSON data")

	var unmarshalledOnError OnError
	err = unmarshalledOnError.Scan(onErrorMarshalled)
	assert.NoError(t, err, "OnError.Scan should not return an error")
	assert.Equal(t, options.OnError.MaxRetries, unmarshalledOnError.MaxRetries, "Unmarshalled OnError MaxRetries should match original")
	assert.Equal(t, options.OnError.RetryDelay, unmarshalledOnError.RetryDelay, "Unmarshalled OnError RetryDelay should match original")
	assert.Equal(t, options.OnError.Timeout, unmarshalledOnError.Timeout, "Unmarshalled OnError Timeout should match original")
	assert.Equal(t, options.OnError.RetryBackoff, unmarshalledOnError.RetryBackoff, "Unmarshalled OnError RetryBackoff should match original")

	// Schedule specific tests
	scheduleMarshalled, err := options.Schedule.Value()
	require.NoError(t, err, "Schedule.Value should not return an error")
	require.NotEmpty(t, scheduleMarshalled, "Schedule.Value should return non-empty JSON data")

	var unmarshalledSchedule Schedule
	err = unmarshalledSchedule.Scan(scheduleMarshalled)
	assert.NoError(t, err, "Schedule.Scan should not return an error")
	assert.Equal(t, options.Schedule.Start.Unix(), unmarshalledSchedule.Start.Unix(), "Unmarshalled Schedule Start should match original")
	assert.Equal(t, options.Schedule.Interval, unmarshalledSchedule.Interval, "Unmarshalled Schedule Interval should match original")
	assert.Equal(t, options.Schedule.MaxCount, unmarshalledSchedule.MaxCount, "Unmarshalled Schedule MaxCount should match original")
	assert.Equal(t, options.Schedule.NextInterval, unmarshalledSchedule.NextInterval, "Unmarshalled Schedule NextInterval should match original")
}
