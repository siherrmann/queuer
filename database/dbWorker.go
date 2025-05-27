package database

import (
	"context"
	"fmt"
	"log"
	"queuer/helper"
	"queuer/model"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
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
func NewWorkerDBHandler(dbConnection *helper.Database) (*WorkerDBHandler, error) {
	workerDbHandler := &WorkerDBHandler{
		db: dbConnection,
	}

	// TODO Remove table drop
	err := workerDbHandler.DropTable()
	if err != nil {
		return nil, fmt.Errorf("error dropping worker table: %#v", err)
	}

	err = workerDbHandler.CreateTable()
	if err != nil {
		return nil, fmt.Errorf("error creating worker table: %#v", err)
	}

	return workerDbHandler, nil
}

// CheckTableExistance checks if the 'worker' table exists in the database.
func (r WorkerDBHandler) CheckTableExistance() (bool, error) {
	exists := false
	exists, err := r.db.CheckTableExistance("worker")
	return exists, err
}

// CreateTable creates the 'worker' table in the database if it doesn't already exist.
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
			status VARCHAR(50) DEFAULT 'RUNNING',
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)`,
	)
	if err != nil {
		log.Fatalf("error creating worker table: %#v", err)
	}

	err = r.db.CreateIndexes("worker", "name", "status")
	if err != nil {
		r.db.Logger.Fatal(err)
	}

	r.db.Logger.Println("created table worker")
	return nil
}

// DropTable drops the 'worker' table from the database.
func (r WorkerDBHandler) DropTable() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	query := `DROP TABLE IF EXISTS worker`
	_, err := r.db.Instance.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("error dropping worker table: %#v", err)
	}

	r.db.Logger.Printf("dropped table worker")
	return nil
}

// InsertWorker inserts a new worker record into the database.
func (r WorkerDBHandler) InsertWorker(worker *model.Worker) (*model.Worker, error) {
	newWorker := &model.Worker{}

	row := r.db.Instance.QueryRow(
		`INSERT INTO worker (name)
			VALUES ($1)
		RETURNING
			id, rid, name, status, created_at, updated_at`,
		worker.Name,
	)

	err := row.Scan(
		&newWorker.ID,
		&newWorker.RID,
		&newWorker.Name,
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
func (r WorkerDBHandler) UpdateWorker(worker *model.Worker) (*model.Worker, error) {
	row := r.db.Instance.QueryRow(
		`UPDATE
			worker
		SET
			name = $1,
			available_tasks = $2,
			status = $3,
			updated_at = CURRENT_TIMESTAMP
		WHERE
			rid = $4
		RETURNING
			id,
			rid,
			name,
			available_tasks,
			status,
			created_at,
			updated_at;`,
		worker.Name,
		pq.Array(worker.AvailableTasks),
		worker.Status,
		worker.RID,
	)

	updatedWorker := &model.Worker{}
	err := row.Scan(
		&updatedWorker.ID,
		&updatedWorker.RID,
		&updatedWorker.Name,
		pq.Array(&updatedWorker.AvailableTasks),
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
func (r WorkerDBHandler) DeleteWorker(rid uuid.UUID) error {
	_, err := r.db.Instance.Exec(
		`DELETE FROM worker
		WHERE rid = $1`,
		rid,
	)
	if err != nil {
		return fmt.Errorf("error deleting worker with RID %s: %w", rid.String(), err)
	}

	return nil
}

// SelectWorker retrieves a single worker record from the database based on its RID.
func (r WorkerDBHandler) SelectWorker(rid uuid.UUID) (*model.Worker, error) {
	worker := &model.Worker{}

	row := r.db.Instance.QueryRow(
		`SELECT
			id,
			rid,
			name,
			available_tasks,
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
		pq.Array(&worker.AvailableTasks),
		&worker.Status,
		&worker.CreatedAt,
		&worker.UpdatedAt,
	)

	return worker, err
}

// SelectAllWorkers retrieves a paginated list of all workers.
func (r WorkerDBHandler) SelectAllWorkers(lastID int, entries int) ([]*model.Worker, error) {
	var workers []*model.Worker

	rows, err := r.db.Instance.Query(
		`SELECT
			id,
			rid,
			name,
			available_tasks,
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
			pq.Array(&worker.AvailableTasks),
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
func (r WorkerDBHandler) SelectAllWorkersBySearch(search string, lastID int, entries int) ([]*model.Worker, error) {
	var workers []*model.Worker

	rows, err := r.db.Instance.Query(`
		SELECT
			id,
			rid,
			name,
			available_tasks,
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
			pq.Array(&worker.AvailableTasks),
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
