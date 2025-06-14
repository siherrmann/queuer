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
