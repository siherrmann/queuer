package helper

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	dbName = "database"
	dbUser = "user"
	dbPwd  = "password"
)

// MustStartPostgresContainer starts a PostgreSQL container for testing purposes.
func MustStartPostgresContainer() (func(ctx context.Context, opts ...testcontainers.TerminateOption) error, string, error) {
	ctx := context.Background()

	pgContainer, err := postgres.Run(
		ctx,
		"timescale/timescaledb:latest-pg17",
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
		return nil, "", fmt.Errorf("error parsing connection string: %v", err)
	}

	return pgContainer.Terminate, u.Port(), err
}

func NewTestDatabase(config *DatabaseConfiguration) *Database {
	return NewDatabase(
		"test_db",
		config,
	)
}

func NewTestDatabaseConfig(port string) *DatabaseConfiguration {
	return &DatabaseConfiguration{
		Host:     "localhost",
		Port:     port,
		Database: dbName,
		Username: dbUser,
		Password: dbPwd,
		Schema:   "public",
	}
}
