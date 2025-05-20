package database

import (
	"context"
	"fmt"
	"log"
	"queue/helper"
	"queue/model"
	"time"
)

// JobDBHandlerFunctions defines the interface for Job database operations.
type JobDBHandlerFunctions interface {
	CheckTableExistance() (bool, error)
	CreateTable() error
	DropTable() error
	InsertJob(job *model.Job) (*model.Job, error)
	UpdateJob(job *model.Job) error
	DeleteJob(rid string) error
	SelectJob(rid string) (*model.Job, error)
	SelectAllJobs(workerRID string, lastID int, entries int) ([]*model.Job, error)
	SelectAllJobsBySearch(workerRID string, search string, lastID int, entries int) ([]*model.Job, error)
}

// JobDBHandler implements JobDBHandlerFunctions and holds the database connection.
type JobDBHandler struct {
	db *helper.Database
}

// NewJobDBHandler creates a new instance of JobDBHandler.
func NewJobDBHandler(dbConnection *helper.Database) *JobDBHandler {
	return &JobDBHandler{
		db: dbConnection,
	}
}

// CheckTableExistance checks if the 'job' table exists in the database.
func (r JobDBHandler) CheckTableExistance() (bool, error) {
	exists := false
	exists, err := r.db.CheckTableExistance("job")
	return exists, err
}

// CreateTable creates the 'job' table in the database if it doesn't already exist.
func (r JobDBHandler) CreateTable() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := r.db.Instance.ExecContext(
		ctx,
		`CREATE TABLE IF NOT EXISTS job (
            id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            rid UUID UNIQUE DEFAULT gen_random_uuid(),
            worker_id VARCHAR(200) DEFAULT '',
            worker_rid UUID NOT NULL,
			parameters JSONB DEFAULT '{}',
            status VARCHAR(50) DEFAULT 'QUEUED',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )`,
	)
	if err != nil {
		log.Fatalf("error creating job table: %#v", err)
	}

	err = r.db.CreateIndexes("job", "worker_rid", "status") // Indexes on common search/filter fields
	if err != nil {
		log.Fatal(err)
	}

	r.db.Logger.Println("created table job")
	return nil
}

// DropTable drops the 'job' table from the database.
func (r JobDBHandler) DropTable() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	query := `DROP TABLE IF EXISTS job`
	_, err := r.db.Instance.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("error dropping job table: %#v", err)
	}

	r.db.Logger.Printf("dropped table job")
	return nil
}

// InsertJob inserts a new job record into the database.
func (r JobDBHandler) InsertJob(job *model.Job) (*model.Job, error) {
	newJob := &model.Job{}

	row := r.db.Instance.QueryRow(
		`INSERT INTO job (parameters)
            VALUES ($1)
        RETURNING
            id, rid, worker_id, worker_rid, status, created_at, updated_at`,
		job.Parameters,
	)

	err := row.Scan(
		&newJob.ID,
		&newJob.RID,
		&newJob.WorkerID,
		&newJob.WorkerRID,
		&newJob.Status,
		&newJob.CreatedAt,
		&newJob.UpdatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("error scanning new job: %w", err)
	}

	return newJob, nil
}

// UpdateJob updates an existing job record in the database based on its RID.
func (r JobDBHandler) UpdateJob(job *model.Job) error {
	_, err := r.db.Instance.Exec(
		`UPDATE
            job
        SET
            worker_id = $1,
            worker_rid = $2,
            status = $3,
            updated_at = CURRENT_TIMESTAMP
        WHERE
            rid = $4`,
		job.WorkerID,
		job.WorkerRID,
		job.Status,
		job.RID,
	)

	return err
}

// DeleteJob deletes a job record from the database based on its RID.
func (r JobDBHandler) DeleteJob(rid string) error {
	_, err := r.db.Instance.Exec(
		`DELETE FROM job
        WHERE rid = $1`,
		rid,
	)
	if err != nil {
		return fmt.Errorf("error deleting job with RID %s: %w", rid, err)
	}

	return nil
}

// SelectJob retrieves a single job record from the database based on its RID.
func (r JobDBHandler) SelectJob(rid string) (*model.Job, error) {
	job := &model.Job{}
	row := r.db.Instance.QueryRow(
		`SELECT
            id,
            rid,
            worker_id,
            worker_rid,
			parameters,
            status,
            created_at,
            updated_at
        FROM
            job
        WHERE
            rid = $1`,
		rid,
	)
	err := row.Scan(
		&job.ID,
		&job.RID,
		&job.WorkerID,
		&job.WorkerRID,
		&job.Parameters,
		&job.Status,
		&job.CreatedAt,
		&job.UpdatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("error scanning job with RID %s: %w", rid, err)
	}

	return job, nil
}

// SelectAllJobs retrieves a paginated list of jobs for a specific worker.
func (r JobDBHandler) SelectAllJobs(workerRID string, lastID int, entries int) ([]*model.Job, error) {
	var jobs []*model.Job

	rows, err := r.db.Instance.Query(
		`SELECT
            id,
            rid,
            worker_id,
            worker_rid,
			parameters,
            status,
            created_at,
            updated_at
        FROM
            job
        WHERE worker_rid = $1
        AND (0 = $2
            OR created_at < (
                SELECT
                    d.created_at
                FROM
                    job AS d
                WHERE
                    d.id = $2))
        ORDER BY
            created_at DESC
        LIMIT $3`,
		workerRID,
		lastID,
		entries,
	)
	if err != nil {
		return []*model.Job{}, fmt.Errorf("error querying all jobs: %w", err)
	}

	defer rows.Close()

	for rows.Next() {
		job := &model.Job{}
		err := rows.Scan(
			&job.ID,
			&job.RID,
			&job.WorkerID,
			&job.WorkerRID,
			&job.Parameters,
			&job.Status,
			&job.CreatedAt,
			&job.UpdatedAt,
		)
		if err != nil {
			return []*model.Job{}, fmt.Errorf("error scanning job row: %w", err)
		}

		jobs = append(jobs, job)
	}

	return jobs, nil
}

// SelectAllJobsBySearch retrieves a paginated list of jobs for a worker, filtered by search string.
// It searches across 'rid', 'worker_id', and 'status' fields.
func (r JobDBHandler) SelectAllJobsBySearch(workerRID string, search string, lastID int, entries int) ([]*model.Job, error) {
	var jobs []*model.Job

	rows, err := r.db.Instance.Query(`
        SELECT
            id,
            rid,
            worker_id,
            worker_rid,
			parameters,
            status,
            created_at,
            updated_at
        FROM job
        WHERE worker_rid = $1
        AND (rid ILIKE '%' || $2 || '%'
                OR worker_id ILIKE '%' || $2 || '%'
                OR status ILIKE '%' || $2 || '%')
            AND (0 = $3
                OR created_at < (
                    SELECT
                        u.created_at
                    FROM
                        job AS u
                    WHERE
                        u.id = $3))
        ORDER BY
            created_at DESC
        LIMIT $4`,
		workerRID,
		search,
		lastID,
		entries,
	)
	if err != nil {
		return []*model.Job{}, fmt.Errorf("error querying jobs by search: %w", err)
	}

	defer rows.Close()

	for rows.Next() {
		job := &model.Job{}
		err := rows.Scan(
			&job.ID,
			&job.RID,
			&job.WorkerID,
			&job.WorkerRID,
			&job.Parameters,
			&job.Status,
			&job.CreatedAt,
			&job.UpdatedAt,
		)
		if err != nil {
			return []*model.Job{}, fmt.Errorf("error scanning job row during search: %w", err)
		}

		jobs = append(jobs, job)
	}

	if err = rows.Err(); err != nil {
		return []*model.Job{}, fmt.Errorf("error after iterating rows during search: %w", err)
	}

	return jobs, nil
}
