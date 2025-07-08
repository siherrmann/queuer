package queuer

import (
	"testing"

	"github.com/siherrmann/queuer/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewQueuer(t *testing.T) {
	tests := []struct {
		name           string
		maxConcurrency int
		options        []*model.OnError
		dbEnvs         map[string]string
		expectError    bool
	}{
		{
			name:           "Valid queuer",
			maxConcurrency: 100,
			options:        nil,
			dbEnvs: map[string]string{
				"QUEUER_DB_HOST":     "localhost",
				"QUEUER_DB_PORT":     dbPort,
				"QUEUER_DB_DATABASE": "database",
				"QUEUER_DB_USERNAME": "user",
				"QUEUER_DB_PASSWORD": "password",
				"QUEUER_DB_SCHEMA":   "public",
			},
			expectError: false,
		},
		{
			name:           "Valid queuer with options",
			maxConcurrency: 100,
			options: []*model.OnError{
				{
					Timeout:      10.0,
					MaxRetries:   3,
					RetryDelay:   1.0,
					RetryBackoff: model.RETRY_BACKOFF_LINEAR,
				},
			},
			dbEnvs: map[string]string{
				"QUEUER_DB_HOST":     "localhost",
				"QUEUER_DB_PORT":     dbPort,
				"QUEUER_DB_DATABASE": "database",
				"QUEUER_DB_USERNAME": "user",
				"QUEUER_DB_PASSWORD": "password",
				"QUEUER_DB_SCHEMA":   "public",
			},
			expectError: false,
		},
		{
			name:           "Invalid max concurrency",
			maxConcurrency: -1,
			options:        nil,
			dbEnvs: map[string]string{
				"QUEUER_DB_HOST":     "localhost",
				"QUEUER_DB_PORT":     dbPort,
				"QUEUER_DB_DATABASE": "database",
				"QUEUER_DB_USERNAME": "user",
				"QUEUER_DB_PASSWORD": "password",
				"QUEUER_DB_SCHEMA":   "public",
			},
			expectError: true,
		},
		{
			name:           "Invalid options",
			maxConcurrency: 100,
			options: []*model.OnError{
				{
					Timeout:      -10.0, // Invalid timeout value
					MaxRetries:   3,
					RetryDelay:   1.0,
					RetryBackoff: model.RETRY_BACKOFF_LINEAR,
				},
			},
			dbEnvs: map[string]string{
				"QUEUER_DB_HOST":     "localhost",
				"QUEUER_DB_PORT":     dbPort,
				"QUEUER_DB_DATABASE": "database",
				"QUEUER_DB_USERNAME": "user",
				"QUEUER_DB_PASSWORD": "password",
				"QUEUER_DB_SCHEMA":   "public",
			},
			expectError: true,
		},
		{
			name:           "Missing DB environment variable",
			maxConcurrency: 100,
			options:        nil,
			dbEnvs: map[string]string{
				"QUEUER_DB_HOST": "localhost",
				"QUEUER_DB_PORT": dbPort,
				// "QUEUER_DB_DATABASE": "database", // Intentionally missing
				"QUEUER_DB_USERNAME": "user",
				"QUEUER_DB_PASSWORD": "password",
				"QUEUER_DB_SCHEMA":   "public",
			},
			expectError: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			for key, value := range test.dbEnvs {
				t.Setenv(key, value)
			}

			if test.expectError {
				defer func() {
					if r := recover(); r == nil {
						t.Errorf("Expected panic for %s, but did not get one", test.name)
					}
				}()
			}

			queuer := NewQueuer(test.name, test.maxConcurrency, test.options...)
			if !test.expectError {
				require.NotNil(t, queuer, "Expected Queuer to be created successfully")
				assert.Equal(t, test.name, queuer.worker.Name, "Expected Queuer name to match")
				assert.Equal(t, test.maxConcurrency, queuer.worker.MaxConcurrency, "Expected Queuer max concurrency to match")
			}
		})
	}
}

func TestStop(t *testing.T) {
	envs := map[string]string{
		"QUEUER_DB_HOST":     "localhost",
		"QUEUER_DB_PORT":     dbPort,
		"QUEUER_DB_DATABASE": "database",
		"QUEUER_DB_USERNAME": "user",
		"QUEUER_DB_PASSWORD": "password",
		"QUEUER_DB_SCHEMA":   "public",
	}
	for key, value := range envs {
		t.Setenv(key, value)
	}

	queuer := NewQueuer("test", 10)
	require.NotNil(t, queuer, "Expected Queuer to be created successfully")

	err := queuer.Stop()
	assert.NoError(t, err, "Expected Stop to complete without error")
}
