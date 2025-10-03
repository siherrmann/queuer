package database

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
	"github.com/siherrmann/queuer/helper"
	"github.com/siherrmann/queuer/model"
	loadSql "github.com/siherrmann/queuer/sql"
)

// WorkerDBHandlerFunctions defines the interface for Worker database operations.
type WorkerDBHandlerFunctions interface {
	CheckTableExistance() (bool, error)
	CreateTable() error
	DropTable() error
	InsertWorker(worker *model.Worker) (*model.Worker, error)
	UpdateWorker(worker *model.Worker) (*model.Worker, error)
	DeleteWorker(rid uuid.UUID) error
	SelectWorker(rid uuid.UUID) (*model.Worker, error)
	SelectAllWorkers(lastID int, entries int) ([]*model.Worker, error)
	SelectAllWorkersBySearch(search string, lastID int, entries int) ([]*model.Worker, error)
}

// WorkerDBHandler implements WorkerDBHandlerFunctions and holds the database connection.
type WorkerDBHandler struct {
	db *helper.Database
}

// NewWorkerDBHandler creates a new instance of WorkerDBHandler.
// It initializes the database connection and optionally drops the existing worker table.
// If withTableDrop is true, it will drop the existing worker table before creating a new one.
func NewWorkerDBHandler(dbConnection *helper.Database, withTableDrop bool) (*WorkerDBHandler, error) {
	if dbConnection == nil {
		return nil, fmt.Errorf("database connection is nil")
	}

	workerDbHandler := &WorkerDBHandler{
		db: dbConnection,
	}

	if withTableDrop {
		err := workerDbHandler.DropTable()
		if err != nil {
			return nil, fmt.Errorf("error dropping worker table: %#v", err)
		}
	}

	err := workerDbHandler.CreateTable()
	if err != nil {
		return nil, fmt.Errorf("error creating worker table: %#v", err)
	}

	err = loadSql.LoadWorkerSql(dbConnection.Instance, withTableDrop)
	if err != nil {
		return nil, fmt.Errorf("error loading worker SQL functions: %w", err)
	}

	return workerDbHandler, nil
}

// CheckTableExistance checks if the 'worker' table exists in the database.
// It returns true if the table exists, otherwise false.
func (r WorkerDBHandler) CheckTableExistance() (bool, error) {
	exists := false
	exists, err := r.db.CheckTableExistance("worker")
	return exists, err
}

// CreateTable creates the 'worker' table in the database if it doesn't already exist.
// It defines the structure of the table with appropriate columns and types.
// If the table already exists, it will not create it again.
// It also creates necessary indexes for efficient querying.
func (r WorkerDBHandler) CreateTable() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := r.db.Instance.ExecContext(
		ctx,
		`CREATE TABLE IF NOT EXISTS worker (
			id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
			rid UUID UNIQUE DEFAULT gen_random_uuid(),
			name VARCHAR(100) DEFAULT '',
			options JSONB DEFAULT '{}',
			available_tasks VARCHAR[] DEFAULT ARRAY[]::VARCHAR[],
			available_next_interval VARCHAR[] DEFAULT ARRAY[]::VARCHAR[],
			current_concurrency INT DEFAULT 0,
			max_concurrency INT DEFAULT 1,
			status VARCHAR(50) DEFAULT 'READY',
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)`,
	)
	if err != nil {
		log.Panicf("error creating worker table: %#v", err)
	}

	err = r.db.CreateIndexes("worker", "rid", "name", "status")
	if err != nil {
		panic(fmt.Sprintf("error creating indexes on worker table: %#v", err))
	}

	r.db.Logger.Info("Created table worker")
	return nil
}

// DropTable drops the 'worker' table from the database.
// It will remove the table and all its data.
// This operation is irreversible, so it should be used with caution.
// It is used during testing or when resetting the database schema.
// If the table does not exist, it will not return an error.
func (r WorkerDBHandler) DropTable() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	query := `DROP TABLE IF EXISTS worker`
	_, err := r.db.Instance.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("error dropping worker table: %#v", err)
	}

	r.db.Logger.Info("Dropped table worker")
	return nil
}

// InsertWorker inserts a new worker record with name, options and max concurrency into the database.
// It returns the newly created worker with an automatically generated RID.
// If the insertion fails, it returns an error.
func (r WorkerDBHandler) InsertWorker(worker *model.Worker) (*model.Worker, error) {
	row := r.db.Instance.QueryRow(
		`SELECT
			output_id,
			output_rid,
			output_name,
			output_options,
			output_max_concurrency,
			output_status,
			output_created_at,
			output_updated_at
		FROM insert_worker($1, $2, $3);`,
		worker.Name,
		worker.Options,
		worker.MaxConcurrency,
	)

	newWorker := &model.Worker{}
	err := row.Scan(
		&newWorker.ID,
		&newWorker.RID,
		&newWorker.Name,
		&newWorker.Options,
		&newWorker.MaxConcurrency,
		&newWorker.Status,
		&newWorker.CreatedAt,
		&newWorker.UpdatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("error scanning new worker: %w", err)
	}

	return newWorker, nil
}

// UpdateWorker updates an existing worker record in the database based on its RID.
// It updates the worker's name, options, available tasks, next interval functions, max concurrency, and status.
// It returns the updated worker record with an automatically updated updated_at timestamp.
// If the update fails, it returns an error.
func (r WorkerDBHandler) UpdateWorker(worker *model.Worker) (*model.Worker, error) {
	row := r.db.Instance.QueryRow(
		`SELECT
			output_id,
			output_rid,
			output_name,
			output_options,
			output_available_tasks,
			output_available_next_interval,
			output_max_concurrency,
			output_status,
			output_created_at,
			output_updated_at
		FROM update_worker($1, $2, $3, $4, $5, $6, $7);`,
		worker.Name,
		worker.Options,
		pq.Array(worker.AvailableTasks),
		pq.Array(worker.AvailableNextIntervalFuncs),
		worker.MaxConcurrency,
		worker.Status,
		worker.RID,
	)

	updatedWorker := &model.Worker{}
	err := row.Scan(
		&updatedWorker.ID,
		&updatedWorker.RID,
		&updatedWorker.Name,
		&updatedWorker.Options,
		pq.Array(&updatedWorker.AvailableTasks),
		pq.Array(&updatedWorker.AvailableNextIntervalFuncs),
		&updatedWorker.MaxConcurrency,
		&updatedWorker.Status,
		&updatedWorker.CreatedAt,
		&updatedWorker.UpdatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("error scanning updated worker: %w", err)
	}

	return updatedWorker, err
}

// DeleteWorker deletes a worker record from the database based on its RID.
// It removes the worker from the database and returns an error if the deletion fails.
func (r WorkerDBHandler) DeleteWorker(rid uuid.UUID) error {
	_, err := r.db.Instance.Exec(
		`SELECT delete_worker($1);`,
		rid,
	)
	if err != nil {
		return fmt.Errorf("error deleting worker with RID %s: %w", rid.String(), err)
	}

	return nil
}

// SelectWorker retrieves a single worker record from the database based on its RID.
// It returns the worker record.
// If the worker is not found or an error occurs during the query, it returns an error.
func (r WorkerDBHandler) SelectWorker(rid uuid.UUID) (*model.Worker, error) {
	worker := &model.Worker{}

	row := r.db.Instance.QueryRow(
		`SELECT
			id,
			rid,
			name,
			options,
			available_tasks,
			available_next_interval,
			max_concurrency,
			status,
			created_at,
			updated_at
		FROM
			worker
		WHERE
			rid = $1`,
		rid,
	)
	err := row.Scan(
		&worker.ID,
		&worker.RID,
		&worker.Name,
		&worker.Options,
		pq.Array(&worker.AvailableTasks),
		pq.Array(&worker.AvailableNextIntervalFuncs),
		&worker.MaxConcurrency,
		&worker.Status,
		&worker.CreatedAt,
		&worker.UpdatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("error scanning worker with RID %s: %w", rid.String(), err)
	}

	return worker, nil
}

// SelectAllWorkers retrieves a paginated list of all workers.
// It returns a slice of worker records, ordered by creation date in descending order.
// It returns workers that were created before the specified lastID, or the newest workers if lastID is 0.
func (r WorkerDBHandler) SelectAllWorkers(lastID int, entries int) ([]*model.Worker, error) {
	var workers []*model.Worker

	rows, err := r.db.Instance.Query(
		`SELECT
			id,
			rid,
			name,
			options,
			available_tasks,
			available_next_interval,
			max_concurrency,
			status,
			created_at,
			updated_at
		FROM
			worker
		WHERE (0 = $1
			OR created_at < (
				SELECT
					d.created_at
				FROM
					worker AS d
				WHERE
					d.id = $1))
		ORDER BY
			created_at DESC
		LIMIT $2`,
		lastID,
		entries,
	)
	if err != nil {
		return []*model.Worker{}, fmt.Errorf("error querying all workers: %w", err)
	}

	defer rows.Close()

	for rows.Next() {
		worker := &model.Worker{}
		err := rows.Scan(
			&worker.ID,
			&worker.RID,
			&worker.Name,
			&worker.Options,
			pq.Array(&worker.AvailableTasks),
			pq.Array(&worker.AvailableNextIntervalFuncs),
			&worker.MaxConcurrency,
			&worker.Status,
			&worker.CreatedAt,
			&worker.UpdatedAt,
		)
		if err != nil {
			return []*model.Worker{}, fmt.Errorf("error scanning worker row: %w", err)
		}

		workers = append(workers, worker)
	}
	if err = rows.Err(); err != nil {
		return []*model.Worker{}, fmt.Errorf("error iterating rows: %w", err)
	}

	return workers, nil
}

// SelectAllWorkersBySearch retrieves a paginated list of workers, filtered by search string.
// It searches across 'queue_name', 'name', and 'status' fields.
// The search is case-insensitive and uses ILIKE for partial matches.
// It returns a slice of worker records, ordered by creation date in descending order.
// It returns workers that were created before the specified lastID, or the newest workers if last
func (r WorkerDBHandler) SelectAllWorkersBySearch(search string, lastID int, entries int) ([]*model.Worker, error) {
	var workers []*model.Worker

	rows, err := r.db.Instance.Query(`
		SELECT
			id,
			rid,
			name,
			options,
			available_tasks,
			available_next_interval,
			max_concurrency,
			status,
			created_at,
			updated_at
		FROM worker
		WHERE (name ILIKE '%' || $1 || '%'
				OR array_to_string(available_tasks, ',') ILIKE '%' || $1 || '%'
				OR status ILIKE '%' || $1 || '%')
			AND (0 = $2
				OR created_at < (
					SELECT
						u.created_at
					FROM
						worker AS u
					WHERE
						u.id = $2))
		ORDER BY
			created_at DESC
		LIMIT $3`,
		search,
		lastID,
		entries,
	)
	if err != nil {
		return []*model.Worker{}, fmt.Errorf("error querying workers by search: %w", err)
	}

	defer rows.Close()

	for rows.Next() {
		worker := &model.Worker{}
		err := rows.Scan(
			&worker.ID,
			&worker.RID,
			&worker.Name,
			&worker.Options,
			pq.Array(&worker.AvailableTasks),
			pq.Array(&worker.AvailableNextIntervalFuncs),
			&worker.MaxConcurrency,
			&worker.Status,
			&worker.CreatedAt,
			&worker.UpdatedAt,
		)
		if err != nil {
			return []*model.Worker{}, fmt.Errorf("error scanning worker row during search: %w", err)
		}

		workers = append(workers, worker)
	}
	if err = rows.Err(); err != nil {
		return []*model.Worker{}, fmt.Errorf("error iterating rows: %w", err)
	}

	return workers, nil
}
