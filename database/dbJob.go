package database

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"queuer/helper"
	"queuer/model"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
)

// JobDBHandlerFunctions defines the interface for Job database operations.
type JobDBHandlerFunctions interface {
	CheckTableExistance() (bool, error)
	CreateTable() error
	DropTable() error
	InsertJob(job *model.Job) (*model.Job, error)
	BatchInsertJobs(jobs []*model.Job) error
	UpdateJobInitial(worker *model.Worker) (*model.Job, error)
	UpdateJobFinal(job *model.Job) (*model.Job, error)
	DeleteJob(rid uuid.UUID) error
	SelectJob(rid uuid.UUID) (*model.Job, error)
	SelectAllJobs(lastID int, entries int) ([]*model.Job, error)
	SelectAllJobsByWorkerRID(workerRid uuid.UUID, lastID int, entries int) ([]*model.Job, error)
	SelectAllJobsBySearch(search string, lastID int, entries int) ([]*model.Job, error)
	// Job Archive
	SelectJobFromArchive(rid uuid.UUID) (*model.Job, error)
	SelectAllJobsFromArchive(lastID int, entries int) ([]*model.Job, error)
	SelectAllJobsFromArchiveBySearch(search string, lastID int, entries int) ([]*model.Job, error)
}

// JobDBHandler implements JobDBHandlerFunctions and holds the database connection.
type JobDBHandler struct {
	db *helper.Database
}

// NewJobDBHandler creates a new instance of JobDBHandler.
func NewJobDBHandler(dbConnection *helper.Database) (*JobDBHandler, error) {
	jobDbHandler := &JobDBHandler{
		db: dbConnection,
	}

	// TODO Remove table drop
	err := jobDbHandler.DropTable()
	if err != nil {
		return nil, fmt.Errorf("error dropping job table: %#v", err)
	}

	err = jobDbHandler.CreateTable()
	if err != nil {
		return nil, fmt.Errorf("error creating job table: %#v", err)
	}

	return jobDbHandler, nil
}

// CheckTableExistance checks if the 'job' table exists in the database.
func (r JobDBHandler) CheckTableExistance() (bool, error) {
	exists := false
	exists, err := r.db.CheckTableExistance("job")
	return exists, err
}

// CreateTable creates the 'job' table in the database if it doesn't already exist.
// It also creates a trigger for notifying events on the table and all necessary indexes.
func (r JobDBHandler) CreateTable() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := r.db.Instance.ExecContext(
		ctx,
		`CREATE TABLE IF NOT EXISTS job (
			id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
			rid UUID UNIQUE DEFAULT gen_random_uuid(),
			worker_id BIGINT DEFAULT 0,
			worker_rid UUID DEFAULT NULL,
			options JSONB DEFAULT '{}',
			task_name VARCHAR(100) DEFAULT '',
			parameters JSONB DEFAULT '{}',
			status VARCHAR(50) DEFAULT 'QUEUED',
			scheduled_at TIMESTAMP DEFAULT NULL,
			started_at TIMESTAMP DEFAULT NULL,
			attempts INT DEFAULT 0,
			results JSONB DEFAULT '{}',
			error TEXT DEFAULT '',
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);
		
		CREATE TABLE IF NOT EXISTS job_archive (
			LIKE job INCLUDING DEFAULTS INCLUDING CONSTRAINTS,
			PRIMARY KEY (id, updated_at)
		);
		SELECT create_hypertable('job_archive', by_range('updated_at'), if_not_exists => TRUE);`,
	)
	if err != nil {
		log.Fatalf("error creating job table: %#v", err)
	}

	_, err = r.db.Instance.ExecContext(
		ctx,
		`CREATE OR REPLACE TRIGGER job_notify_event
			AFTER INSERT OR UPDATE OR DELETE ON job
			FOR EACH ROW EXECUTE PROCEDURE notify_event();`,
	)
	if err != nil {
		log.Fatalf("error creating notify trigger on job table: %#v", err)
	}

	err = r.db.CreateIndexes("job", "worker_id", "worker_rid", "status", "created_at", "updated_at") // Indexes on common search/filter fields
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

	query = `DROP TABLE IF EXISTS job_archive`
	_, err = r.db.Instance.ExecContext(ctx, query)
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
		`INSERT INTO job (options, task_name, parameters, status)
			VALUES ($1, $2, $3, $4)
		RETURNING
			id,
			rid,
			worker_id,
			worker_rid,
			options,
			task_name,
			parameters,
			status,
			scheduled_at,
			attempts,
			created_at,
			updated_at;`,
		job.Options,
		job.TaskName,
		job.Parameters,
		job.Status,
	)

	err := row.Scan(
		&newJob.ID,
		&newJob.RID,
		&newJob.WorkerID,
		&newJob.WorkerRID,
		&newJob.Options,
		&newJob.TaskName,
		&newJob.Parameters,
		&newJob.Status,
		&newJob.ScheduledAt,
		&newJob.Attempts,
		&newJob.CreatedAt,
		&newJob.UpdatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("error scanning new job: %w", err)
	}

	return newJob, nil
}

func (r JobDBHandler) BatchInsertJobs(jobs []*model.Job) error {
	if len(jobs) == 0 {
		return nil
	}

	tx, err := r.db.Instance.Begin()
	if err != nil {
		return fmt.Errorf("error starting transaction: %w", err)
	}

	stmt, err := tx.Prepare(pq.CopyIn("job", "options", "task_name", "parameters", "scheduled_at"))
	if err != nil {
		return fmt.Errorf("error preparing statement for batch insert: %w", err)
	}

	for _, job := range jobs {
		optionsJSON, err := job.Options.Marshal()
		if err != nil {
			return fmt.Errorf("error marshaling job options for batch insert: %w", err)
		}
		parametersJSON, err := job.Parameters.Marshal()
		if err != nil {
			return fmt.Errorf("error marshaling job parameters for batch insert: %v", err)
		}

		_, err = stmt.Exec(
			string(optionsJSON),
			job.TaskName,
			string(parametersJSON),
			job.ScheduledAt,
		)
		if err != nil {
			return fmt.Errorf("error executing batch insert for job %s: %w", job.RID, err)
		}
	}

	_, err = stmt.Exec()
	if err != nil {
		return fmt.Errorf("error executing final batch insert: %w", err)
	}

	err = stmt.Close()
	if err != nil {
		return fmt.Errorf("error closing prepared statement: %w", err)
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("error committing transaction: %w", err)
	}

	return nil
}

// UpdateJobInitial updates an existing queued non locked job record in the database.
// Checks if the job is in 'QUEUED' or 'FAILED' status and if the worker can handle the task.
func (r JobDBHandler) UpdateJobInitial(worker *model.Worker) (*model.Job, error) {
	row := r.db.Instance.QueryRow(
		`WITH current_worker AS (
			SELECT id, rid, available_tasks
			FROM worker
			WHERE id = $1
			AND max_concurrency > (
				SELECT COUNT(*)
				FROM job
				WHERE worker_id = $1
				AND status = 'RUNNING'
			)
		)
		UPDATE job SET 
			worker_id = cw.id,
			worker_rid = cw.rid,
			status = 'RUNNING',
			started_at = CURRENT_TIMESTAMP,
			attempts = attempts + 1,
			updated_at = CURRENT_TIMESTAMP
		FROM current_worker AS cw
		WHERE job.id = (
			SELECT id FROM job 
			WHERE 
				task_name = ANY(cw.available_tasks::VARCHAR[])
				AND status = 'QUEUED'
			ORDER BY created_at ASC
			LIMIT 1
			FOR UPDATE SKIP LOCKED
		)
		AND EXISTS (SELECT 1 FROM current_worker)
		RETURNING
			job.id,
			job.rid,
			job.worker_id,
			job.worker_rid,
			job.options,
			job.task_name,
			job.parameters,
			job.status,
			job.scheduled_at,
			job.started_at,
			job.attempts,
			job.created_at,
			job.updated_at;`,
		worker.ID,
	)

	job := &model.Job{}
	err := row.Scan(
		&job.ID,
		&job.RID,
		&job.WorkerID,
		&job.WorkerRID,
		&job.Options,
		&job.TaskName,
		&job.Parameters,
		&job.Status,
		&job.ScheduledAt,
		&job.StartedAt,
		&job.Attempts,
		&job.CreatedAt,
		&job.UpdatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	} else if err != nil {
		return nil, fmt.Errorf("error updating initial job for worker id %v: %w", job.WorkerRID, err)
	}

	return job, nil
}

// UpdateJobFinal updates an existing job record in the database to state 'FAILED' or 'SUCCEEDED'.
func (r JobDBHandler) UpdateJobFinal(job *model.Job) (*model.Job, error) {
	row := r.db.Instance.QueryRow(
		`WITH jobs_old AS (
			DELETE FROM job
			WHERE id = $1
			RETURNING
				id,
				rid,
				worker_id,
				worker_rid,
				options,
				task_name,
				parameters,
				scheduled_at,
				started_at,
				attempts,
				created_at,
				updated_at
		) INSERT INTO job_archive (
			id,
			rid,
			worker_id,
			worker_rid,
			options,
			task_name,
			parameters,
			status,
			scheduled_at,
			started_at,
			attempts,
			results,
			error,
			created_at,
			updated_at
		)
		SELECT
			jobs_old.id,
			jobs_old.rid,
			jobs_old.worker_id,
			jobs_old.worker_rid,
			jobs_old.options,
			jobs_old.task_name,
			jobs_old.parameters,
			$2,
			jobs_old.scheduled_at,
			jobs_old.started_at,
			jobs_old.attempts,
			$3,
			$4,
			jobs_old.created_at,
			jobs_old.updated_at
		FROM jobs_old
		RETURNING
			id,
			rid,
			worker_id,
			worker_rid,
			options,
			task_name,
			parameters,
			status,
			scheduled_at,
			started_at,
			attempts,
			results,
			error,
			created_at,
			updated_at;`,
		job.ID,
		job.Status,
		job.Results,
		job.Error,
	)

	archivedJob := &model.Job{}
	err := row.Scan(
		&archivedJob.ID,
		&archivedJob.RID,
		&archivedJob.WorkerID,
		&archivedJob.WorkerRID,
		&archivedJob.Options,
		&archivedJob.TaskName,
		&archivedJob.Parameters,
		&archivedJob.Status,
		&archivedJob.ScheduledAt,
		&archivedJob.StartedAt,
		&archivedJob.Attempts,
		&archivedJob.Results,
		&archivedJob.Error,
		&archivedJob.CreatedAt,
		&archivedJob.UpdatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("error updating final job for worker id %v: %w", job.WorkerRID, err)
	}

	return archivedJob, nil
}

// DeleteJob deletes a job record from the database based on its RID.
func (r JobDBHandler) DeleteJob(rid uuid.UUID) error {
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
func (r JobDBHandler) SelectJob(rid uuid.UUID) (*model.Job, error) {
	row := r.db.Instance.QueryRow(
		`SELECT
			id,
			rid,
			worker_id,
			worker_rid,
			options,
			task_name,
			parameters,
			status,
			scheduled_at,
			started_at,
			attempts,
			results,
			error,
			created_at,
			updated_at
		FROM
			job
		WHERE
			rid = $1`,
		rid,
	)

	job := &model.Job{}
	err := row.Scan(
		&job.ID,
		&job.RID,
		&job.WorkerID,
		&job.WorkerRID,
		&job.Options,
		&job.TaskName,
		&job.Parameters,
		&job.Status,
		&job.ScheduledAt,
		&job.StartedAt,
		&job.Attempts,
		&job.Results,
		&job.Error,
		&job.CreatedAt,
		&job.UpdatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("error scanning job with RID %s: %w", rid, err)
	}

	return job, nil
}

// SelectAllJobs retrieves a paginated list of jobs for a specific worker.
func (r JobDBHandler) SelectAllJobs(lastID int, entries int) ([]*model.Job, error) {
	rows, err := r.db.Instance.Query(
		`SELECT
			id,
			rid,
			worker_id,
			worker_rid,
			options,
			task_name,
			parameters,
			status,
			scheduled_at,
			started_at,
			attempts,
			results,
			error,
			created_at,
			updated_at
		FROM
			job
		WHERE (0 = $1
			OR created_at < (
				SELECT
					d.created_at
				FROM
					job AS d
				WHERE
					d.id = $1))
		ORDER BY
			created_at DESC
		LIMIT $2;`,
		lastID,
		entries,
	)
	if err != nil {
		return []*model.Job{}, fmt.Errorf("error querying all jobs: %w", err)
	}

	defer rows.Close()

	var jobs []*model.Job
	for rows.Next() {
		job := &model.Job{}
		err := rows.Scan(
			&job.ID,
			&job.RID,
			&job.WorkerID,
			&job.WorkerRID,
			&job.Options,
			&job.TaskName,
			&job.Parameters,
			&job.Status,
			&job.ScheduledAt,
			&job.StartedAt,
			&job.Attempts,
			&job.Results,
			&job.Error,
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

// SelectAllJobsByWorkerRID retrieves a paginated list of jobs for a specific worker, filtered by worker RID.
func (r JobDBHandler) SelectAllJobsByWorkerRID(workerRid uuid.UUID, lastID int, entries int) ([]*model.Job, error) {
	rows, err := r.db.Instance.Query(
		`SELECT
			id,
			rid,
			worker_id,
			worker_rid,
			options,
			task_name,
			parameters,
			status,
			scheduled_at,
			started_at,
			attempts,
			results,
			error,
			created_at,
			updated_at
		FROM job
		WHERE worker_rid = $1
			AND (0 = $2
				OR created_at < (
					SELECT
						d.created_at
					FROM
						job AS d
					WHERE
						d.id = $2))
		ORDER BY created_at DESC
		LIMIT $3;`,
		workerRid,
		lastID,
		entries,
	)
	if err != nil {
		return []*model.Job{}, fmt.Errorf("error querying jobs by worker RID: %w", err)
	}

	defer rows.Close()

	var jobs []*model.Job
	for rows.Next() {
		job := &model.Job{}
		err := rows.Scan(
			&job.ID,
			&job.RID,
			&job.WorkerID,
			&job.WorkerRID,
			&job.Options,
			&job.TaskName,
			&job.Parameters,
			&job.Status,
			&job.ScheduledAt,
			&job.StartedAt,
			&job.Attempts,
			&job.Results,
			&job.Error,
			&job.CreatedAt,
			&job.UpdatedAt,
		)
		if err != nil {
			return []*model.Job{}, fmt.Errorf("error scanning job row for worker: %w", err)
		}

		jobs = append(jobs, job)
	}

	return jobs, nil
}

// SelectAllJobsBySearch retrieves a paginated list of jobs for a worker, filtered by search string.
// It searches across 'rid', 'worker_id', and 'status' fields.
func (r JobDBHandler) SelectAllJobsBySearch(search string, lastID int, entries int) ([]*model.Job, error) {
	rows, err := r.db.Instance.Query(`
		SELECT
			id,
			rid,
			worker_id,
			worker_rid,
			options,
			task_name,
			parameters,
			status,
			scheduled_at,
			started_at,
			attempts,
			results,
			error,
			created_at,
			updated_at
		FROM job
		WHERE (rid ILIKE '%' || $1 || '%'
				OR worker_id ILIKE '%' || $1 || '%'
				OR task_name ILIKE '%' || $1 || '%'
				OR status ILIKE '%' || $1 || '%')
			AND (0 = $2
				OR created_at < (
					SELECT
						u.created_at
					FROM
						job AS u
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
		return []*model.Job{}, fmt.Errorf("error querying jobs by search: %w", err)
	}

	defer rows.Close()

	var jobs []*model.Job
	for rows.Next() {
		job := &model.Job{}
		err := rows.Scan(
			&job.ID,
			&job.RID,
			&job.WorkerID,
			&job.WorkerRID,
			&job.Options,
			&job.TaskName,
			&job.Parameters,
			&job.Status,
			&job.ScheduledAt,
			&job.StartedAt,
			&job.Attempts,
			&job.Results,
			&job.Error,
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

// Job Archive
func (r JobDBHandler) SelectJobFromArchive(rid uuid.UUID) (*model.Job, error) {
	row := r.db.Instance.QueryRow(
		`SELECT
			id,
			rid,
			worker_id,
			worker_rid,
			options,
			task_name,
			parameters,
			status,
			scheduled_at,
			started_at,
			attempts,
			results,
			error,
			created_at,
			updated_at
		FROM
			job_archive
		WHERE
			rid = $1`,
		rid,
	)

	job := &model.Job{}
	err := row.Scan(
		&job.ID,
		&job.RID,
		&job.WorkerID,
		&job.WorkerRID,
		&job.Options,
		&job.TaskName,
		&job.Parameters,
		&job.Status,
		&job.ScheduledAt,
		&job.StartedAt,
		&job.Attempts,
		&job.Results,
		&job.Error,
		&job.CreatedAt,
		&job.UpdatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("error scanning archived job with RID %s: %w", rid, err)
	}

	return job, nil
}

func (r JobDBHandler) SelectAllJobsFromArchive(lastID int, entries int) ([]*model.Job, error) {
	rows, err := r.db.Instance.Query(
		`SELECT
			id,
			rid,
			worker_id,
			worker_rid,
			options,
			task_name,
			parameters,
			status,
			scheduled_at,
			started_at,
			attempts,
			results,
			error,
			created_at,
			updated_at
		FROM
			job_archive
		WHERE (0 = $1
			OR created_at < (
				SELECT
					d.created_at
				FROM
					job_archive AS d
				WHERE
					d.id = $1))
		ORDER BY
			created_at DESC
		LIMIT $2;`,
		lastID,
		entries,
	)
	if err != nil {
		return []*model.Job{}, fmt.Errorf("error querying all archived jobs: %w", err)
	}

	defer rows.Close()

	var jobs []*model.Job
	for rows.Next() {
		job := &model.Job{}
		err := rows.Scan(
			&job.ID,
			&job.RID,
			&job.WorkerID,
			&job.WorkerRID,
			&job.Options,
			&job.TaskName,
			&job.Parameters,
			&job.Status,
			&job.ScheduledAt,
			&job.StartedAt,
			&job.Attempts,
			&job.Results,
			&job.Error,
			&job.CreatedAt,
			&job.UpdatedAt,
		)
		if err != nil {
			return []*model.Job{}, fmt.Errorf("error scanning archived job row: %w", err)
		}

		jobs = append(jobs, job)
	}

	return jobs, nil
}

func (r JobDBHandler) SelectAllJobsFromArchiveBySearch(search string, lastID int, entries int) ([]*model.Job, error) {
	rows, err := r.db.Instance.Query(`
		SELECT
			id,
			rid,
			worker_id,
			worker_rid,
			options,
			task_name,
			parameters,
			status,
			scheduled_at,
			started_at,
			attempts,
			results,
			error,
			created_at,
			updated_at
		FROM job_archive
		WHERE (rid ILIKE '%' || $1 || '%'
				OR worker_id ILIKE '%' || $1 || '%'
				OR task_name ILIKE '%' || $1 || '%'
				OR status ILIKE '%' || $1 || '%')
			AND (0 = $2
				OR created_at < (
					SELECT
						u.created_at
					FROM
						job_archive AS u
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
		return []*model.Job{}, fmt.Errorf("error querying archived jobs by search: %w", err)
	}

	defer rows.Close()

	var jobs []*model.Job
	for rows.Next() {
		job := &model.Job{}
		err := rows.Scan(
			&job.ID,
			&job.RID,
			&job.WorkerID,
			&job.WorkerRID,
			&job.Options,
			&job.TaskName,
			&job.Parameters,
			&job.Status,
			&job.ScheduledAt,
			&job.StartedAt,
			&job.Attempts,
			&job.Results,
			&job.Error,
			&job.CreatedAt,
			&job.UpdatedAt,
		)
		if err != nil {
			return []*model.Job{}, fmt.Errorf("error scanning archived job row during search: %w", err)
		}

		jobs = append(jobs, job)
	}

	if err = rows.Err(); err != nil {
		return []*model.Job{}, fmt.Errorf("error after iterating rows during search: %w", err)
	}

	return jobs, nil
}
