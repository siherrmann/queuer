package helper

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"testing"
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
// It uses the timescale/timescaledb image with PostgreSQL 17.
// It returns a function to terminate the container, the port on which the database is accessible,
// and an error if the container could not be started.
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

// NewTestDatabase creates a new Database instance for testing purposes.
// It initializes the database with the provided configuration and the name "test_db".
// It returns a pointer to the new Database instance.
func NewTestDatabase(config *DatabaseConfiguration) *Database {
	return NewDatabase(
		"test_db",
		config,
		slog.Default(),
	)
}

// SetTestDatabaseConfigEnvs sets the environment variables for the test database configuration.
// It sets the host, port, database name, username, password, schema,
// and table drop options for the test database.
func SetTestDatabaseConfigEnvs(t *testing.T, port string) {
	t.Setenv("QUEUER_DB_HOST", "localhost")
	t.Setenv("QUEUER_DB_PORT", port)
	t.Setenv("QUEUER_DB_DATABASE", dbName)
	t.Setenv("QUEUER_DB_USERNAME", dbUser)
	t.Setenv("QUEUER_DB_PASSWORD", dbPwd)
	t.Setenv("QUEUER_DB_SCHEMA", "public")
	t.Setenv("QUEUER_DB_SSLMODE", "disable")
	t.Setenv("QUEUER_DB_WITH_TABLE_DROP", "true")
}
