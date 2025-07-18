package database

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/lib/pq"
	"github.com/siherrmann/queuer/helper"
	"github.com/siherrmann/queuer/model"
)

// JobDBHandlerFunctions defines the interface for Job database operations.
type JobDBHandlerFunctions interface {
	CheckTablesExistance() (bool, error)
	CreateTable() error
	DropTables() error
	InsertJob(job *model.Job) (*model.Job, error)
	InsertJobTx(tx *sql.Tx, job *model.Job) (*model.Job, error)
	BatchInsertJobs(jobs []*model.Job) error
	UpdateJobsInitial(worker *model.Worker) ([]*model.Job, error)
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
// It initializes the database connection and optionally drops existing tables.
// If withTableDrop is true, it will drop the existing job tables before creating new ones
func NewJobDBHandler(dbConnection *helper.Database, withTableDrop bool) (*JobDBHandler, error) {
	if dbConnection == nil {
		return nil, fmt.Errorf("database connection is nil")
	}

	jobDbHandler := &JobDBHandler{
		db: dbConnection,
	}

	if withTableDrop {
		err := jobDbHandler.DropTables()
		if err != nil {
			return nil, fmt.Errorf("error dropping job table: %#v", err)
		}
	}

	err := jobDbHandler.CreateTable()
	if err != nil {
		return nil, fmt.Errorf("error creating job table: %#v", err)
	}

	return jobDbHandler, nil
}

// CheckTablesExistance checks if the 'job' and 'job_archive' tables exist in the database.
// It returns true if both tables exist, otherwise false.
func (r JobDBHandler) CheckTablesExistance() (bool, error) {
	jobExists, err := r.db.CheckTableExistance("job")
	if err != nil {
		return false, fmt.Errorf("error checking job table existence: %w", err)
	}
	jobArchiveExists, err := r.db.CheckTableExistance("job_archive")
	if err != nil {
		return false, fmt.Errorf("error checking job archive table existence: %w", err)
	}
	return jobExists && jobArchiveExists, err
}

// CreateTable creates the 'job' and 'job_archive' tables in the database.
// If the tables already exist, it does not create them again.
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
			parameters JSONB DEFAULT '[]',
			status VARCHAR(50) DEFAULT 'QUEUED',
			scheduled_at TIMESTAMP DEFAULT NULL,
			started_at TIMESTAMP DEFAULT NULL,
			schedule_count INT DEFAULT 0,
			attempts INT DEFAULT 0,
			results JSONB DEFAULT '[]',
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
		log.Panicf("error creating job table: %#v", err)
	}

	_, err = r.db.Instance.ExecContext(
		ctx,
		`CREATE OR REPLACE TRIGGER job_notify_event
			BEFORE INSERT OR UPDATE ON job
			FOR EACH ROW EXECUTE PROCEDURE notify_event();`,
	)
	if err != nil {
		log.Panicf("error creating notify trigger on job table: %#v", err)
	}

	_, err = r.db.Instance.ExecContext(
		ctx,
		`CREATE OR REPLACE TRIGGER job_archive_notify_event
			BEFORE INSERT ON job_archive
			FOR EACH ROW EXECUTE PROCEDURE notify_event();`,
	)
	if err != nil {
		log.Panicf("error creating notify trigger on job_archive table: %#v", err)
	}

	_, err = r.db.Instance.Exec(
		`CREATE INDEX IF NOT EXISTS idx_next_interval
		ON job
		USING HASH ((options->'schedule'->>'next_interval'));`,
	)
	if err != nil {
		log.Panicf("error creating index on next_interval: %#v", err)
	}

	err = r.db.CreateIndexes("job", "worker_id", "worker_rid", "status", "created_at", "updated_at")
	if err != nil {
		log.Panic(err)
	}

	r.db.Logger.Println("created table job")
	return nil
}

// DropTables drops the 'job' and 'job_archive' tables from the database.
func (r JobDBHandler) DropTables() error {
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
		`INSERT INTO job (options, task_name, parameters, status, scheduled_at, schedule_count)
			VALUES ($1, $2, $3, $4, $5, $6)
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
			schedule_count,
			attempts,
			created_at,
			updated_at;`,
		job.Options,
		job.TaskName,
		job.Parameters,
		job.Status,
		job.ScheduledAt,
		job.ScheduleCount,
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
		&newJob.ScheduleCount,
		&newJob.Attempts,
		&newJob.CreatedAt,
		&newJob.UpdatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("error scanning new job: %w", err)
	}

	return newJob, nil
}

// InsertJobTx inserts a new job record into the database within a transaction.
func (r JobDBHandler) InsertJobTx(tx *sql.Tx, job *model.Job) (*model.Job, error) {
	newJob := &model.Job{}
	row := tx.QueryRow(
		`INSERT INTO job (options, task_name, parameters, status, scheduled_at)
			VALUES ($1, $2, $3, $4, $5)
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
			schedule_count,
			attempts,
			created_at,
			updated_at;`,
		job.Options,
		job.TaskName,
		job.Parameters,
		job.Status,
		job.ScheduledAt,
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
		&newJob.ScheduleCount,
		&newJob.Attempts,
		&newJob.CreatedAt,
		&newJob.UpdatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("error scanning new job: %w", err)
	}

	return newJob, nil
}

// BatchInsertJobs inserts multiple job records into the database in a single transaction.
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
		var err error
		optionsJSON := []byte("{}")
		parametersJSON := []byte("[]")

		if job.Options != nil {
			optionsJSON, err = job.Options.Marshal()
			if err != nil {
				return fmt.Errorf("error marshaling job options for batch insert: %w", err)
			}
		}

		if job.Parameters != nil {
			parametersJSON, err = job.Parameters.Marshal()
			if err != nil {
				return fmt.Errorf("error marshaling job parameters for batch insert: %v", err)
			}
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

// UpdateJobsInitial updates an existing queued non locked job record in the database.
//
// Checks if the job is in 'QUEUED' or 'FAILED' status and if the worker can handle the task.
// The worker must have the task in its available tasks and the next interval must be available if set.
// If the job is scheduled it must be scheduled within the next 10 minutes.
// It updates the job to 'RUNNING' status, increments the schedule count and attempts, and sets the started_at timestamp.
// It uses the `FOR UPDATE SKIP LOCKED` clause to avoid locking issues with concurrent updates.
//
// It returns the updated job records.
func (r JobDBHandler) UpdateJobsInitial(worker *model.Worker) ([]*model.Job, error) {
	rows, err := r.db.Instance.Query(
		`WITH current_concurrency AS (
			SELECT COUNT(*) AS count
			FROM job
			WHERE worker_id = $1
			AND status = 'RUNNING'
		),
		current_worker AS (
			SELECT
				id,
				rid,
				available_tasks,
				available_next_interval,
				max_concurrency,
				COALESCE(cc.count, 0) AS current_concurrency
			FROM worker, current_concurrency AS cc
			WHERE id = $1
			AND (max_concurrency > COALESCE(cc.count, 0))
			FOR UPDATE
		),
		job_ids AS (
			SELECT j.id
			FROM current_worker AS cw, current_concurrency AS cc,
			LATERAL (
				SELECT job.id
				FROM job
				WHERE
					job.task_name = ANY(cw.available_tasks::VARCHAR[])
					AND (
						options->'schedule'->>'next_interval' IS NULL
						OR options->'schedule'->>'next_interval' = ''
						OR options->'schedule'->>'next_interval' = ANY(cw.available_next_interval::VARCHAR[]))
					AND (
						job.status = 'QUEUED'
						OR (job.status = 'SCHEDULED' AND job.scheduled_at <= (CURRENT_TIMESTAMP + '10 minutes'::INTERVAL))
					)
				ORDER BY job.created_at ASC
				LIMIT (cw.max_concurrency - COALESCE(cc.count, 0))
				FOR UPDATE SKIP LOCKED
			) AS j
		)
		UPDATE job SET
			worker_id = cw.id,
			worker_rid = cw.rid,
			status = 'RUNNING',
			started_at = CURRENT_TIMESTAMP,
			schedule_count = schedule_count + 1,
			attempts = attempts + 1,
			updated_at = CURRENT_TIMESTAMP
		FROM current_worker AS cw, job_ids
		WHERE job.id = ANY(SELECT id FROM job_ids)
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
			job.schedule_count,
			job.attempts,
			job.created_at,
			job.updated_at;`,
		worker.ID,
	)
	if err != nil {
		return nil, fmt.Errorf("error querying job for worker id %v: %w", worker.ID, err)
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
			&job.ScheduleCount,
			&job.Attempts,
			&job.CreatedAt,
			&job.UpdatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("error updating initial job for worker id %v: %w", job.WorkerRID, err)
		}

		jobs = append(jobs, job)
	}

	return jobs, nil
}

// UpdateJobFinal updates an existing job record in the database to state 'FAILED' or 'SUCCEEDED'.
//
// It deletes the job from the 'job' table and inserts it into the 'job_archive' table.
// The archived job will have the status set to the provided status, and it will include results and error information.
//
// It returns the archived job record.
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
				schedule_count,
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
			schedule_count,
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
			jobs_old.schedule_count,
			jobs_old.attempts,
			$3,
			$4,
			jobs_old.created_at,
			CURRENT_TIMESTAMP
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
			schedule_count,
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
		&archivedJob.ScheduleCount,
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
			schedule_count,
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
		&job.ScheduleCount,
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

// SelectAllJobs retrieves a paginated list of jobs for a all workers.
// It returns jobs that were created before the specified lastID, or the newest jobs if lastID is 0.
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
			schedule_count,
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
			&job.ScheduleCount,
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
// It returns jobs that were created before the specified lastID, or the newest jobs if lastID is 0.
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
			schedule_count,
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
			&job.ScheduleCount,
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
//
// It searches across 'rid', 'worker_id', and 'status' fields.
// The search is case-insensitive and uses ILIKE for partial matches.
//
// It returns jobs that were created before the specified lastID, or the newest jobs if lastID is 0.
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
			schedule_count,
			attempts,
			results,
			error,
			created_at,
			updated_at
		FROM job
		WHERE (rid::text ILIKE '%' || $1 || '%'
				OR worker_id::text ILIKE '%' || $1 || '%'
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
			&job.ScheduleCount,
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

// SelectJobFromArchive retrieves a single archived job record from the database based on its RID.
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
			schedule_count,
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
		&job.ScheduleCount,
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

// SelectAllJobsFromArchive retrieves a paginated list of archived jobs.
// It returns jobs that were created before the specified lastID, or the newest jobs if lastID is 0.
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
			schedule_count,
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
			&job.ScheduleCount,
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

// SelectAllJobsFromArchiveBySearch retrieves a paginated list of archived jobs filtered by search string.
// It searches across 'rid', 'worker_id', 'task_name', and 'status' fields.
// It returns jobs that were created before the specified lastID, or the newest jobs if lastID
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
			schedule_count,
			attempts,
			results,
			error,
			created_at,
			updated_at
		FROM job_archive
		WHERE (rid::text ILIKE '%' || $1 || '%'
				OR worker_id::text ILIKE '%' || $1 || '%'
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
			&job.ScheduleCount,
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
