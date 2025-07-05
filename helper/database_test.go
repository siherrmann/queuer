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

func TestCheckTableExistance(t *testing.T) {
	dbConfig := NewTestDatabaseConfig(dbPort)
	database := NewTestDatabase(dbConfig)

	_, err := database.Instance.Exec(
		`CREATE TABLE IF NOT EXISTS test_table_existance (
			id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
			name TEXT NOT NULL
		);`,
	)
	if err != nil {
		log.Panicf("error creating job table: %#v", err)
	}

	exists, err := database.CheckTableExistance("test_table_existance")
	assert.NoError(t, err, "expected no error when checking table existence")
	assert.True(t, exists, "expected 'workers' table to exist")

	exists, err = database.CheckTableExistance("non_existing_table")
	assert.NoError(t, err, "expected no error when checking non-existing table")
	assert.False(t, exists, "expected 'non_existing_table' to not exist")
}

func TestCreateIndex(t *testing.T) {
	dbConfig := NewTestDatabaseConfig(dbPort)
	database := NewTestDatabase(dbConfig)

	// Create a test table
	_, err := database.Instance.Exec(
		`CREATE TABLE IF NOT EXISTS test_index (
			id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
			name TEXT NOT NULL
		);`,
	)
	if err != nil {
		log.Panicf("error creating test_index table: %#v", err)
	}

	err = database.CreateIndex("test_index", "name")
	assert.NoError(t, err, "expected no error when creating index")

	err = database.CreateIndex("test_index", "name")
	assert.NoError(t, err, "expected no error when creating existing index")

	// Clean up should work when the index exists
	err = database.DropIndex("test_index", "name")
	assert.NoError(t, err, "expected no error when dropping index")
}

func TestCreateIndexes(t *testing.T) {
	dbConfig := NewTestDatabaseConfig(dbPort)
	database := NewTestDatabase(dbConfig)

	// Create a test table
	_, err := database.Instance.Exec(
		`CREATE TABLE IF NOT EXISTS test_indexes (
			id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
			name1 TEXT NOT NULL,
			name2 TEXT NOT NULL
		);`,
	)
	if err != nil {
		log.Panicf("error creating test_indexes table: %#v", err)
	}

	err = database.CreateIndexes("test_indexes", []string{"name1", "name2"}...)
	assert.NoError(t, err, "expected no error when creating indexes")

	err = database.CreateIndexes("test_indexes", []string{"name1"}...)
	assert.NoError(t, err, "expected no error when creating existing indexes")

	// Clean up should work when the index exists
	err = database.DropIndex("test_indexes", "name1")
	assert.NoError(t, err, "expected no error when dropping index for name1")
	err = database.DropIndex("test_indexes", "name2")
	assert.NoError(t, err, "expected no error when dropping index for name2")
}

func TestCreateCombinedIndex(t *testing.T) {
	dbConfig := NewTestDatabaseConfig(dbPort)
	database := NewTestDatabase(dbConfig)

	// Create a test table
	_, err := database.Instance.Exec(
		`CREATE TABLE IF NOT EXISTS test_combined_index (
			id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
			name1 TEXT NOT NULL,
			name2 TEXT NOT NULL
		);`,
	)
	if err != nil {
		log.Panicf("error creating test_combined_index table: %#v", err)
	}

	err = database.CreateCombinedIndex("test_combined_index", "name1", "name2")
	assert.NoError(t, err, "expected no error when creating combined index")

	err = database.CreateCombinedIndex("test_combined_index", "name1", "name2")
	assert.NoError(t, err, "expected no error when creating existing combined index")

	// Clean up should work when the index exists
	err = database.DropIndex("test_combined_index", "name1_name2")
	assert.NoError(t, err, "expected no error when dropping combined index")
}

func TestCreateUniqueCombinedIndex(t *testing.T) {
	dbConfig := NewTestDatabaseConfig(dbPort)
	database := NewTestDatabase(dbConfig)

	// Create a test table
	_, err := database.Instance.Exec(
		`CREATE TABLE IF NOT EXISTS test_unique_combined_index (
			id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
			name1 TEXT NOT NULL,
			name2 TEXT NOT NULL
		);`,
	)
	if err != nil {
		log.Panicf("error creating test_unique_combined_index table: %#v", err)
	}

	err = database.CreateUniqueCombinedIndex("test_unique_combined_index", "name1", "name2")
	assert.NoError(t, err, "expected no error when creating unique combined index")

	err = database.CreateUniqueCombinedIndex("test_unique_combined_index", "name1", "name2")
	assert.NoError(t, err, "expected no error when creating existing unique combined index")

	// Clean up should work when the index exists
	err = database.DropIndex("test_unique_combined_index", "name1_name2")
	assert.NoError(t, err, "expected no error when dropping unique combined index")
}

func TestDropIndex(t *testing.T) {
	dbConfig := NewTestDatabaseConfig(dbPort)
	database := NewTestDatabase(dbConfig)

	// Create a test table
	_, err := database.Instance.Exec(
		`CREATE TABLE IF NOT EXISTS test_drop_index (
			id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
			name TEXT NOT NULL
		);`,
	)
	if err != nil {
		log.Panicf("error creating test_drop_index table: %#v", err)
	}

	err = database.CreateIndex("test_drop_index", "name")
	assert.NoError(t, err, "expected no error when creating index")

	err = database.DropIndex("test_drop_index", "name")
	assert.NoError(t, err, "expected no error when dropping index")

	err = database.DropIndex("test_drop_index", "name")
	assert.NoError(t, err, "expected no error when dropping non-existing index")
}
