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
	loadSql "github.com/siherrmann/queuerSql"
)

// WorkerDBHandlerFunctions defines the interface for Worker database operations.
type WorkerDBHandlerFunctions interface {
	CheckTableExistance() (bool, error)
	CreateTable() error
	DropTable() error
	InsertWorker(worker *model.Worker) (*model.Worker, error)
	UpdateWorker(worker *model.Worker) (*model.Worker, error)
	UpdateStaleWorkers(staleThreshold time.Duration) (int, error)
	DeleteWorker(rid uuid.UUID) error
	DeleteStaleWorkers(deleteThreshold time.Duration) (int, error)
	SelectWorker(rid uuid.UUID) (*model.Worker, error)
	SelectAllWorkers(lastID int, entries int) ([]*model.Worker, error)
	SelectAllWorkersBySearch(search string, lastID int, entries int) ([]*model.Worker, error)
	// Connections
	SelectAllConnections() ([]*model.Connection, error)
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
		return nil, helper.NewError("check", fmt.Errorf("database connection is nil"))
	}

	workerDbHandler := &WorkerDBHandler{
		db: dbConnection,
	}

	err := loadSql.LoadWorkerSql(dbConnection.Instance, withTableDrop)
	if err != nil {
		return nil, helper.NewError("load worker sql", err)
	}

	if withTableDrop {
		err := workerDbHandler.DropTable()
		if err != nil {
			return nil, helper.NewError("drop worker table", err)
		}
	}

	err = workerDbHandler.CreateTable()
	if err != nil {
		return nil, helper.NewError("create worker table", err)
	}

	return workerDbHandler, nil
}

// CheckTableExistance checks if the 'worker' table exists in the database.
// It returns true if the table exists, otherwise false.
func (r WorkerDBHandler) CheckTableExistance() (bool, error) {
	workerTableExists, err := r.db.CheckTableExistance("worker")
	if err != nil {
		return false, helper.NewError("worker table", err)
	}
	return workerTableExists, nil
}

// CreateTable creates the 'worker' table in the database if it doesn't already exist.
// It defines the structure of the table with appropriate columns and types.
// If the table already exists, it will not create it again.
// It also creates necessary indexes for efficient querying.
func (r WorkerDBHandler) CreateTable() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := r.db.Instance.ExecContext(ctx, `SELECT init_worker();`)
	if err != nil {
		log.Panicf("error initializing worker table: %#v", err)
	}

	r.db.Logger.Info("Checked/created table worker")

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
		return helper.NewError("worker table", err)
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
		return nil, helper.NewError("scan", err)
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
		return nil, helper.NewError("scan", err)
	}

	return updatedWorker, err
}

// UpdateStaleWorkers updates all stale workers to STOPPED status based on the provided threshold.
// It returns the number of workers that were updated.
// Workers are considered stale if they have READY or RUNNING status and their updated_at
// timestamp is older than the threshold.
func (r WorkerDBHandler) UpdateStaleWorkers(staleThreshold time.Duration) (int, error) {
	cutoffTime := time.Now().UTC().Add(-staleThreshold)

	var rowsAffected int
	err := r.db.Instance.QueryRow(
		`SELECT update_stale_workers($1, $2, $3, $4)`,
		model.WorkerStatusStopped,
		model.WorkerStatusReady,
		model.WorkerStatusRunning,
		cutoffTime,
	).Scan(&rowsAffected)
	if err != nil {
		return 0, helper.NewError("update stale workers", err)
	}

	return rowsAffected, nil
}

// DeleteWorker deletes a worker record from the database based on its RID.
// It removes the worker from the database and returns an error if the deletion fails.
func (r WorkerDBHandler) DeleteWorker(rid uuid.UUID) error {
	_, err := r.db.Instance.Exec(
		`SELECT delete_worker($1);`,
		rid,
	)
	if err != nil {
		return helper.NewError("delete", err)
	}

	return nil
}

// DeleteStaleWorkers deletes workers that have been in STOPPED status for longer than the deleteThreshold.
// It returns the number of workers that were deleted.
func (r WorkerDBHandler) DeleteStaleWorkers(deleteThreshold time.Duration) (int, error) {
	cutoffTime := time.Now().UTC().Add(-deleteThreshold)

	var rowsAffected int
	err := r.db.Instance.QueryRow(
		`SELECT delete_stale_workers($1)`,
		cutoffTime,
	).Scan(&rowsAffected)
	if err != nil {
		return 0, helper.NewError("delete stale workers", err)
	}

	return rowsAffected, nil
}

// SelectWorker retrieves a single worker record from the database based on its RID.
// It returns the worker record.
// If the worker is not found or an error occurs during the query, it returns an error.
func (r WorkerDBHandler) SelectWorker(rid uuid.UUID) (*model.Worker, error) {
	worker := &model.Worker{}
	row := r.db.Instance.QueryRow(
		`SELECT * FROM select_worker($1)`,
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
		return nil, helper.NewError("scan", err)
	}

	return worker, nil
}

// SelectAllWorkers retrieves a paginated list of all workers.
// It returns a slice of worker records, ordered by creation date in descending order.
// It returns workers that were created before the specified lastID, or the newest workers if lastID is 0.
func (r WorkerDBHandler) SelectAllWorkers(lastID int, entries int) ([]*model.Worker, error) {
	rows, err := r.db.Instance.Query(
		`SELECT * FROM select_all_workers($1, $2)`,
		lastID,
		entries,
	)
	if err != nil {
		return []*model.Worker{}, helper.NewError("query", err)
	}

	defer rows.Close()

	var workers []*model.Worker
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
			return []*model.Worker{}, helper.NewError("scan", err)
		}

		workers = append(workers, worker)
	}

	err = rows.Err()
	if err != nil {
		return []*model.Worker{}, helper.NewError("rows error", err)
	}

	return workers, nil
}

// SelectAllWorkersBySearch retrieves a paginated list of workers, filtered by search string.
// It searches across 'queue_name', 'name', and 'status' fields.
// The search is case-insensitive and uses ILIKE for partial matches.
// It returns a slice of worker records, ordered by creation date in descending order.
// It returns workers that were created before the specified lastID, or the newest workers if last
func (r WorkerDBHandler) SelectAllWorkersBySearch(search string, lastID int, entries int) ([]*model.Worker, error) {
	rows, err := r.db.Instance.Query(
		`SELECT * FROM select_all_workers_by_search($1, $2, $3)`,
		search,
		lastID,
		entries,
	)
	if err != nil {
		return []*model.Worker{}, helper.NewError("query", err)
	}

	defer rows.Close()

	var workers []*model.Worker
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
			return []*model.Worker{}, helper.NewError("scan", err)
		}

		workers = append(workers, worker)
	}

	err = rows.Err()
	if err != nil {
		return []*model.Worker{}, helper.NewError("rows error", err)
	}

	return workers, nil
}

// SelectAllConnections retrieves all active connections from the database.
// It returns a slice of Connection records.
// If the query fails, it returns an error.
func (r WorkerDBHandler) SelectAllConnections() ([]*model.Connection, error) {
	rows, err := r.db.Instance.Query(
		`SELECT * FROM select_all_connections()`,
	)
	if err != nil {
		return nil, fmt.Errorf("error querying active connections: %w", err)
	}

	defer rows.Close()

	var connections []*model.Connection
	for rows.Next() {
		conn := &model.Connection{}
		err := rows.Scan(
			&conn.PID,
			&conn.Database,
			&conn.Username,
			&conn.ApplicationName,
			&conn.Query,
			&conn.State,
		)
		if err != nil {
			return nil, helper.NewError("scan", err)
		}
		connections = append(connections, conn)
	}
	if err = rows.Err(); err != nil {
		return nil, helper.NewError("rows error", err)
	}

	return connections, nil
}
