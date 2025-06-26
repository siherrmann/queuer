package model

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
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
