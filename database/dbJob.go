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
	loadSql "github.com/siherrmann/queuer/sql"
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
	AddRetentionArchive(retention time.Duration) error
	RemoveRetentionArchive() error
	SelectJobFromArchive(rid uuid.UUID) (*model.Job, error)
	SelectAllJobsFromArchive(lastID int, entries int) ([]*model.Job, error)
	SelectAllJobsFromArchiveBySearch(search string, lastID int, entries int) ([]*model.Job, error)
}

// JobDBHandler implements JobDBHandlerFunctions and holds the database connection.
type JobDBHandler struct {
	db            *helper.Database
	EncryptionKey string
}

// NewJobDBHandler creates a new instance of JobDBHandler.
// It initializes the database connection and optionally drops existing tables.
// If withTableDrop is true, it will drop the existing job tables before creating new ones
func NewJobDBHandler(dbConnection *helper.Database, withTableDrop bool, encryptionKey ...string) (*JobDBHandler, error) {
	if dbConnection == nil {
		return nil, fmt.Errorf("database connection is nil")
	}

	jobDbHandler := &JobDBHandler{
		db: dbConnection,
	}

	if len(encryptionKey) > 0 {
		jobDbHandler.EncryptionKey = encryptionKey[0]
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

	err = loadSql.LoadJobSql(dbConnection.Instance, withTableDrop)
	if err != nil {
		return nil, fmt.Errorf("error loading job SQL functions: %w", err)
	}

	err = loadSql.LoadNotifySql(dbConnection.Instance, withTableDrop)
	if err != nil {
		return nil, fmt.Errorf("error loading notify SQL functions: %w", err)
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

	_, err := r.db.Instance.ExecContext(ctx, `CREATE EXTENSION IF NOT EXISTS pgcrypto;`)
	if err != nil {
		log.Panicf("error creating pgcrypto extension: %#v", err)
	}

	_, err = r.db.Instance.ExecContext(
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
			results_encrypted BYTEA DEFAULT '',
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

	r.db.Logger.Info("Checked/created table job")
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

	r.db.Logger.Info("Dropped table job")
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
			created_at,
			updated_at
		FROM update_job_initial($1);`,
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
			return nil, fmt.Errorf("error updating initial job with rid %v: %w", job.RID, err)
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
	var row *sql.Row

	if len(r.EncryptionKey) > 0 {
		row = r.db.Instance.QueryRow(
			`SELECT
				output_id,
				output_rid,
				output_worker_id,
				output_worker_rid,
				output_options,
				output_task_name,
				output_parameters,
				output_status,
				output_scheduled_at,
				output_started_at,
				output_schedule_count,
				output_attempts,
				output_results,
				output_error,
				output_created_at,
				output_updated_at
			FROM update_job_final_encrypted($1, $2, $3, $4, $5);`,
			job.ID,
			job.Status,
			job.Results,
			job.Error,
			r.EncryptionKey,
		)
	} else {
		row = r.db.Instance.QueryRow(
			`SELECT
				output_id,
				output_rid,
				output_worker_id,
				output_worker_rid,
				output_options,
				output_task_name,
				output_parameters,
				output_status,
				output_scheduled_at,
				output_started_at,
				output_schedule_count,
				output_attempts,
				output_results,
				output_error,
				output_created_at,
				output_updated_at
			FROM update_job_final($1, $2, $3, $4);`,
			job.ID,
			job.Status,
			job.Results,
			job.Error,
		)
	}

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
		return nil, fmt.Errorf("error updating final job with rid %v: %w", job.RID, err)
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
			CASE
				WHEN octet_length(results_encrypted) > 0 THEN pgp_sym_decrypt(results_encrypted, $1::text)::jsonb
				ELSE results
			END AS results,
			error,
			created_at,
			updated_at
		FROM
			job
		WHERE
			rid = $2`,
		r.EncryptionKey,
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
			CASE
				WHEN octet_length(results_encrypted) > 0 THEN pgp_sym_decrypt(results_encrypted, $1::text)::jsonb
				ELSE results
			END AS results,
			error,
			created_at,
			updated_at
		FROM
			job
		WHERE (0 = $2
			OR created_at < (
				SELECT
					d.created_at
				FROM
					job AS d
				WHERE
					d.id = $2))
		ORDER BY
			created_at DESC
		LIMIT $3;`,
		r.EncryptionKey,
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
			CASE
				WHEN octet_length(results_encrypted) > 0 THEN pgp_sym_decrypt(results_encrypted, $1::text)::jsonb
				ELSE results
			END AS results,
			error,
			created_at,
			updated_at
		FROM job
		WHERE worker_rid = $2
			AND (0 = $3
				OR created_at < (
					SELECT
						d.created_at
					FROM
						job AS d
					WHERE
						d.id = $3))
		ORDER BY created_at DESC
		LIMIT $4;`,
		r.EncryptionKey,
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
			CASE
				WHEN octet_length(results_encrypted) > 0 THEN pgp_sym_decrypt(results_encrypted, $1::text)::jsonb
				ELSE results
			END AS results,
			error,
			created_at,
			updated_at
		FROM job
		WHERE (rid::text ILIKE '%' || $2 || '%'
				OR worker_id::text ILIKE '%' || $2 || '%'
				OR task_name ILIKE '%' || $2 || '%'
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
		r.EncryptionKey,
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

// AddRetentionArchive updates the retention archive settings for the job archive.
func (r JobDBHandler) AddRetentionArchive(retention time.Duration) error {
	_, err := r.db.Instance.Exec(
		`SELECT add_retention_policy('job_archive', ($1 * INTERVAL '1 day'));`,
		int(retention.Hours()/24),
	)
	if err != nil {
		return fmt.Errorf("error updating retention archive: %w", err)
	}

	return nil
}

// RemoveRetentionArchive removes the retention archive settings for the job archive.
func (r JobDBHandler) RemoveRetentionArchive() error {
	_, err := r.db.Instance.Exec(
		`SELECT remove_retention_policy('job_archive');`,
	)
	if err != nil {
		return fmt.Errorf("error removing retention archive: %w", err)
	}

	return nil
}

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
			CASE
				WHEN octet_length(results_encrypted) > 0 THEN pgp_sym_decrypt(results_encrypted, $1::text)::jsonb
				ELSE results
			END AS results,
			error,
			created_at,
			updated_at
		FROM
			job_archive
		WHERE
			rid = $2`,
		r.EncryptionKey,
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
			CASE
				WHEN octet_length(results_encrypted) > 0 THEN pgp_sym_decrypt(results_encrypted, $1::text)::jsonb
				ELSE results
			END AS results,
			error,
			created_at,
			updated_at
		FROM
			job_archive
		WHERE (0 = $2
			OR created_at < (
				SELECT
					d.created_at
				FROM
					job_archive AS d
				WHERE
					d.id = $2))
		ORDER BY
			created_at DESC
		LIMIT $3;`,
		r.EncryptionKey,
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
			CASE
				WHEN octet_length(results_encrypted) > 0 THEN pgp_sym_decrypt(results_encrypted, $1::text)::jsonb
				ELSE results
			END AS results,
			error,
			created_at,
			updated_at
		FROM job_archive
		WHERE (rid::text ILIKE '%' || $2 || '%'
				OR worker_id::text ILIKE '%' || $2 || '%'
				OR task_name ILIKE '%' || $2 || '%'
				OR status ILIKE '%' || $2 || '%')
			AND (0 = $3
				OR created_at < (
					SELECT
						u.created_at
					FROM
						job_archive AS u
					WHERE
						u.id = $3))
		ORDER BY
			created_at DESC
		LIMIT $4`,
		r.EncryptionKey,
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
