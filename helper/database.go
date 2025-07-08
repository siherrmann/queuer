package helper

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/joho/godotenv/autoload"
	"github.com/lib/pq"
)

// Database represents a service that interacts with a database.
type Database struct {
	Name     string
	Logger   *log.Logger
	Instance *sql.DB
}

func NewDatabase(name string, dbConfig *DatabaseConfiguration) *Database {
	logger := log.New(os.Stdout, "Database "+name+": ", log.Ltime)

	if dbConfig != nil {
		db := &Database{Name: name, Logger: logger}
		db.ConnectToDatabase(dbConfig, logger)
		if db.Instance == nil {
			logger.Fatal("failed to connect to database")
		}

		err := db.AddNotifyFunction()
		if err != nil {
			logger.Panicf("failed to add notify function: %v", err)
		}

		return db
	} else {
		return &Database{
			Name:     name,
			Logger:   logger,
			Instance: nil,
		}
	}
}

func NewDatabaseWithDB(name string, dbConnnection *sql.DB) *Database {
	logger := log.New(os.Stdout, "Database "+name+": ", log.Ltime)
	return &Database{
		Name:     name,
		Logger:   logger,
		Instance: dbConnnection,
	}
}

type DatabaseConfiguration struct {
	Host          string
	Port          string
	Database      string
	Username      string
	Password      string
	Schema        string
	WithTableDrop bool
}

func NewDatabaseConfiguration() (*DatabaseConfiguration, error) {
	config := &DatabaseConfiguration{
		Host:          os.Getenv("QUEUER_DB_HOST"),
		Port:          os.Getenv("QUEUER_DB_PORT"),
		Database:      os.Getenv("QUEUER_DB_DATABASE"),
		Username:      os.Getenv("QUEUER_DB_USERNAME"),
		Password:      os.Getenv("QUEUER_DB_PASSWORD"),
		Schema:        os.Getenv("QUEUER_DB_SCHEMA"),
		WithTableDrop: os.Getenv("QUEUER_DB_WITH_TABLE_DROP") == "true",
	}
	if len(strings.TrimSpace(config.Host)) == 0 || len(strings.TrimSpace(config.Port)) == 0 || len(strings.TrimSpace(config.Database)) == 0 || len(strings.TrimSpace(config.Username)) == 0 || len(strings.TrimSpace(config.Password)) == 0 || len(strings.TrimSpace(config.Schema)) == 0 {
		return nil, fmt.Errorf("QUEUER_DB_HOST, QUEUER_DB_PORT, QUEUER_DB_DATABASE, QUEUER_DB_USERNAME, QUEUER_DB_PASSWORD and QUEUER_DB_SCHEMA environment variables must be set")
	}
	return config, nil
}

func (d *DatabaseConfiguration) DatabaseConnectionString() string {
	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable&search_path=%s", d.Username, d.Password, d.Host, d.Port, d.Database, d.Schema)
}

// Internal function for the service creation to connect to a database.
// DatabaseConfiguration must contain uri, username and password.
func (d *Database) ConnectToDatabase(dbConfig *DatabaseConfiguration, logger *log.Logger) {
	if len(strings.TrimSpace(dbConfig.Host)) == 0 || len(strings.TrimSpace(dbConfig.Port)) == 0 || len(strings.TrimSpace(dbConfig.Database)) == 0 || len(strings.TrimSpace(dbConfig.Username)) == 0 || len(strings.TrimSpace(dbConfig.Password)) == 0 || len(strings.TrimSpace(dbConfig.Schema)) == 0 {
		logger.Fatalln("database configuration must contain uri, username and password.")
	}

	var connectOnce sync.Once
	var db *sql.DB

	connectOnce.Do(func() {
		dsn, err := pq.ParseURL(dbConfig.DatabaseConnectionString())
		if err != nil {
			logger.Fatalf("error parsing database connection string: %v", err)
		}

		base, err := pq.NewConnector(dsn)
		if err != nil {
			log.Panic(err)
		}

		db = sql.OpenDB(pq.ConnectorWithNoticeHandler(base, func(notice *pq.Error) {
			// log.Printf("Notice sent: %s", notice.Message)
		}))
		db.SetMaxOpenConns(0)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, err = db.ExecContext(
			ctx,
			"CREATE EXTENSION IF NOT EXISTS pg_trgm;",
		)
		if err != nil {
			logger.Fatal(err)
		}

		pingErr := db.Ping()
		if pingErr != nil {
			logger.Fatal(pingErr)
		}
		logger.Println("Connected to db")
	})

	d.Instance = db
}

func (d *Database) AddNotifyFunction() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// RAISE NOTICE 'Trigger called on table: %, operation: %', TG_TABLE_NAME, TG_OP;
	_, err := d.Instance.ExecContext(
		ctx,
		`CREATE OR REPLACE FUNCTION notify_event() RETURNS TRIGGER AS $$
			DECLARE
				data JSON;
				channel TEXT;
			BEGIN
				IF (TG_TABLE_NAME = 'job') OR (TG_TABLE_NAME = 'worker') THEN
					channel := TG_TABLE_NAME;
				ELSE
					channel := 'job_archive';
				END IF;

				IF (TG_OP = 'DELETE') THEN
					data = row_to_json(OLD);
				ELSE
					data = row_to_json(NEW);
				END IF;
				PERFORM pg_notify(channel, data::text);
				RETURN NEW;
			END;
		$$ LANGUAGE plpgsql;`,
	)

	if err != nil {
		return fmt.Errorf("error creating notify function: %#v", err)
	}
	return nil
}

func (d *Database) CheckTableExistance(tableName string) (bool, error) {
	exists := false

	tableNameQuoted := pq.QuoteLiteral(tableName)
	row := d.Instance.QueryRow(
		fmt.Sprintf(`
				SELECT EXISTS (
					SELECT 1
					FROM information_schema.tables
					WHERE table_name = %s
				) AS table_existence`,
			tableNameQuoted,
		),
	)
	err := row.Scan(
		&exists,
	)
	if err != nil {
		return false, err
	}

	return exists, nil
}

func (d *Database) CreateIndex(tableName string, columnName string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tableNameQuoted := pq.QuoteIdentifier(tableName)
	indexQuoted := pq.QuoteIdentifier("idx_" + tableName + "_" + columnName)
	columnNameQuoted := pq.QuoteIdentifier(columnName)
	_, err := d.Instance.ExecContext(
		ctx,
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS %s ON %s(%s)", indexQuoted, tableNameQuoted, columnNameQuoted),
	)
	if err != nil {
		return fmt.Errorf("error creating %s index: %#v", indexQuoted, err)
	}
	return nil
}

func (d *Database) CreateIndexes(tableName string, columnNames ...string) error {
	for _, columnName := range columnNames {
		err := d.CreateIndex(tableName, columnName)
		if err != nil {
			return err
		}
	}
	return nil
}

func (d *Database) CreateCombinedIndex(tableName string, columnName1 string, columnName2 string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tableNameQuoted := pq.QuoteIdentifier(tableName)
	indexQuoted := pq.QuoteIdentifier("idx_" + tableName + "_" + columnName1 + "_" + columnName2)
	columnName1Quoted := pq.QuoteIdentifier(columnName1)
	columnName2Quoted := pq.QuoteIdentifier(columnName2)
	_, err := d.Instance.ExecContext(
		ctx,
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s ON %s (%s, %s)`, indexQuoted, tableNameQuoted, columnName1Quoted, columnName2Quoted),
	)
	if err != nil {
		return fmt.Errorf("error creating %s index: %#v", indexQuoted, err)
	}
	return nil
}

func (d *Database) CreateUniqueCombinedIndex(tableName string, columnName1 string, columnName2 string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tableNameQuoted := pq.QuoteIdentifier(tableName)
	indexQuoted := pq.QuoteIdentifier("idx_" + tableName + "_" + columnName1 + "_" + columnName2)
	columnName1Quoted := pq.QuoteIdentifier(columnName1)
	columnName2Quoted := pq.QuoteIdentifier(columnName2)
	_, err := d.Instance.ExecContext(
		ctx,
		fmt.Sprintf(`CREATE UNIQUE INDEX IF NOT EXISTS %s ON %s (%s, %s)`, indexQuoted, tableNameQuoted, columnName1Quoted, columnName2Quoted),
	)
	if err != nil {
		return fmt.Errorf("error creating %s index: %#v", indexQuoted, err)
	}
	return nil
}

func (d *Database) DropIndex(tableName string, jsonMapKey string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	indexQuoted := pq.QuoteIdentifier("idx_" + tableName + "_" + jsonMapKey)
	_, err := d.Instance.ExecContext(
		ctx,
		fmt.Sprintf(`DROP INDEX IF EXISTS %s;`, indexQuoted),
	)
	if err != nil {
		return fmt.Errorf("error dropping %s index: %#v", indexQuoted, err)
	}

	return nil
}

// Health checks the health of the database connection by pinging the database.
// It returns a map with keys indicating various health statistics.
func (d *Database) Health() map[string]string {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	stats := make(map[string]string)

	// Ping the database
	err := d.Instance.PingContext(ctx)
	if err != nil {
		stats["status"] = "down"
		stats["error"] = fmt.Sprintf("db down: %v", err)
		log.Panicf("db down: %v", err) // Log the error and terminate the program
		return stats
	}

	// Database is up, add more statistics
	stats["status"] = "up"
	stats["message"] = "It's healthy"

	// Get database stats (like open connections, in use, idle, etc.)
	dbStats := d.Instance.Stats()
	stats["open_connections"] = strconv.Itoa(dbStats.OpenConnections)
	stats["in_use"] = strconv.Itoa(dbStats.InUse)
	stats["idle"] = strconv.Itoa(dbStats.Idle)
	stats["wait_count"] = strconv.FormatInt(dbStats.WaitCount, 10)
	stats["wait_duration"] = dbStats.WaitDuration.String()
	stats["max_idle_closed"] = strconv.FormatInt(dbStats.MaxIdleClosed, 10)
	stats["max_lifetime_closed"] = strconv.FormatInt(dbStats.MaxLifetimeClosed, 10)

	// Evaluate stats to provide a health message
	if dbStats.OpenConnections > 40 { // Assuming 50 is the max for this example
		stats["message"] = "The database is experiencing heavy load."
	}
	if dbStats.WaitCount > 1000 {
		stats["message"] = "The database has a high number of wait events, indicating potential bottlenecks."
	}
	if dbStats.MaxIdleClosed > int64(dbStats.OpenConnections)/2 {
		stats["message"] = "Many idle connections are being closed, consider revising the connection pool settings."
	}
	if dbStats.MaxLifetimeClosed > int64(dbStats.OpenConnections)/2 {
		stats["message"] = "Many connections are being closed due to max lifetime, consider increasing max lifetime or revising the connection usage pattern."
	}

	return stats
}

// Close closes the database connection.
// It logs a message indicating the disconnection from the specific database.
// If the connection is successfully closed, it returns nil.
// If an error occurs while closing the connection, it returns the error.
func (d *Database) Close() error {
	log.Printf("Disconnected from database: %v", d.Instance)
	return d.Instance.Close()
}
