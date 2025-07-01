package helper

import (
	"context"
	"log"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"

	_ "github.com/lib/pq"
)

var dbPort string

func TestMain(m *testing.M) {
	var teardown func(ctx context.Context, opts ...testcontainers.TerminateOption) error
	var err error
	teardown, dbPort, err = MustStartPostgresContainer()
	if err != nil {
		log.Fatalf("error starting postgres container: %v", err)
	}

	m.Run()

	if teardown != nil && teardown(context.Background()) != nil {
		log.Fatalf("error tearing down postgres container: %v", err)
	}
}

func TestNewDatabaseConfig(t *testing.T) {
	tests := []struct {
		name        string
		envs        map[string]string
		expectError bool
	}{
		{
			name: "Valid environment variables",
			envs: map[string]string{
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
			name: "Missing environment variable",
			envs: map[string]string{
				"QUEUER_DB_HOST":     "localhost",
				"QUEUER_DB_PORT":     dbPort,
				"QUEUER_DB_DATABASE": "database",
				"QUEUER_DB_USERNAME": "user",
				// "QUEUER_DB_PASSWORD": "password", // Intentionally missing
				"QUEUER_DB_SCHEMA": "public",
			},
			expectError: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			for key, value := range test.envs {
				t.Setenv(key, value)
			}

			dbConfig, err := NewDatabaseConfiguration()
			if test.expectError {
				assert.NotNil(t, err, "expected an error for %s, but got none", test.name)
				assert.Nil(t, dbConfig, "expected NewTestDatabaseConfig to return nil on error")
			} else {
				assert.NotNil(t, dbConfig, "expected NewTestDatabaseConfig to return a non-nil instance")
				assert.Equal(t, test.envs["QUEUER_DB_HOST"], dbConfig.Host, "expected host to be 'localhost', got %s", dbConfig.Host)
				assert.Equal(t, test.envs["QUEUER_DB_PORT"], dbConfig.Port, "expected port to be %s, got %s", dbPort, dbConfig.Port)
				assert.Equal(t, test.envs["QUEUER_DB_DATABASE"], dbConfig.Database, "expected database to be 'queuer_test', got %s", dbConfig.Database)
				assert.Equal(t, test.envs["QUEUER_DB_USERNAME"], dbConfig.Username, "expected username to be 'postgres', got %s", dbConfig.Username)
				assert.Equal(t, test.envs["QUEUER_DB_PASSWORD"], dbConfig.Password, "expected password to be empty, got %s", dbConfig.Password)
				assert.Equal(t, test.envs["QUEUER_DB_SCHEMA"], dbConfig.Schema, "expected schema to be 'public', got %s", dbConfig.Schema)
			}

		})
	}
}

func TestNew(t *testing.T) {
	dbConfig := NewTestDatabaseConfig(dbPort)
	database := NewTestDatabase(dbConfig)

	assert.NotNil(t, database, "expected NewDatabase to return a non-nil instance")
}

func TestHealth(t *testing.T) {
	dbConfig := NewTestDatabaseConfig(dbPort)
	database := NewTestDatabase(dbConfig)

	stats := database.Health()

	assert.Equal(t, stats["status"], "up", "expected status to be 'up', got %s", stats["status"])
	assert.NotContains(t, stats, "error", "expected error to not be present in health check")
	assert.Contains(t, stats, "message", "expected message to be present in health check")
	assert.Equal(t, stats["message"], "It's healthy", "expected message to be 'It's healthy', got %s", stats["message"])
}

func TestClose(t *testing.T) {
	dbConfig := NewTestDatabaseConfig(dbPort)
	database := NewTestDatabase(dbConfig)

	assert.NotNil(t, database, "expected NewDatabase to return a non-nil instance")
}
