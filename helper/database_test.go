package helper

import (
	"context"
	"fmt"
	"log"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	_ "github.com/lib/pq"
)

const (
	dbName = "database"
	dbUser = "user"
	dbPwd  = "password"
)

var port string

func mustStartPostgresContainer() (func(ctx context.Context, opts ...testcontainers.TerminateOption) error, string, error) {
	ctx := context.Background()

	pgContainer, err := postgres.Run(
		ctx,
		"postgres:latest",
		postgres.WithDatabase(dbName),
		postgres.WithUsername(dbUser),
		postgres.WithPassword(dbPwd),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).WithStartupTimeout(5*time.Second),
		),
	)
	if err != nil {
		return nil, "", fmt.Errorf("error starting postgres container: %w", err)
	}

	connStr, err := pgContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		return nil, "", fmt.Errorf("error getting connection string: %w", err)
	}

	u, err := url.Parse(connStr)
	if err != nil {
		return nil, "", fmt.Errorf("error parsing connection string: %v\n", err)
	}

	return pgContainer.Terminate, u.Port(), err
}

func TestMain(m *testing.M) {
	var teardown func(ctx context.Context, opts ...testcontainers.TerminateOption) error
	var err error
	teardown, port, err = mustStartPostgresContainer()
	if err != nil {
		log.Fatalf("error starting postgres container: %v", err)
	}

	m.Run()

	if teardown != nil && teardown(context.Background()) != nil {
		log.Fatalf("error tearing down postgres container: %v", err)
	}
}

func TestNew(t *testing.T) {
	srv := NewDatabase(
		"test_db",
		&DatabaseConfiguration{
			Host:     "localhost",
			Port:     port,
			Database: dbName,
			Username: dbUser,
			Password: dbPwd,
			Schema:   "public",
		},
	)

	assert.NotNil(t, srv, "expected NewDatabase to return a non-nil instance")
}

func TestHealth(t *testing.T) {
	srv := NewDatabase(
		"test_db",
		&DatabaseConfiguration{
			Host:     "localhost",
			Port:     port,
			Database: dbName,
			Username: dbUser,
			Password: dbPwd,
			Schema:   "public",
		},
	)

	stats := srv.Health()

	assert.Equal(t, stats["status"], "up", "expected status to be 'up', got %s", stats["status"])
	assert.NotContains(t, stats, "error", "expected error to not be present in health check")
	assert.Contains(t, stats, "message", "expected message to be present in health check")
	assert.Equal(t, stats["message"], "It's healthy", "expected message to be 'It's healthy', got %s", stats["message"])
}

func TestClose(t *testing.T) {
	srv := NewDatabase(
		"test_db",
		&DatabaseConfiguration{
			Host:     "localhost",
			Port:     port,
			Database: dbName,
			Username: dbUser,
			Password: dbPwd,
			Schema:   "public",
		},
	)

	assert.NotNil(t, srv, "expected NewDatabase to return a non-nil instance")
}
