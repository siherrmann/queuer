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
	loadSql "github.com/siherrmann/queuerSql"
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
	UpdateStaleJobs() (int, error)
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
		return nil, helper.NewError("database connection validation", fmt.Errorf("database connection is nil"))
	}

	jobDbHandler := &JobDBHandler{
		db: dbConnection,
	}

	if len(encryptionKey) > 0 {
		jobDbHandler.EncryptionKey = encryptionKey[0]
	}

	err := loadSql.LoadJobSql(jobDbHandler.db.Instance, false)
	if err != nil {
		return nil, helper.NewError("load job sql", err)
	}

	err = loadSql.LoadNotifySql(jobDbHandler.db.Instance, false)
	if err != nil {
		return nil, helper.NewError("load notify sql", err)
	}

	if withTableDrop {
		err := jobDbHandler.DropTables()
		if err != nil {
			return nil, helper.NewError("drop tables", err)
		}
	}

	err = jobDbHandler.CreateTable()
	if err != nil {
		return nil, helper.NewError("create table", err)
	}

	return jobDbHandler, nil
}

// CheckTablesExistance checks if the 'job' and 'job_archive' tables exist in the database.
// It returns true if both tables exist, otherwise false.
func (r JobDBHandler) CheckTablesExistance() (bool, error) {
	jobExists, err := r.db.CheckTableExistance("job")
	if err != nil {
		return false, helper.NewError("job table", err)
	}
	jobArchiveExists, err := r.db.CheckTableExistance("job_archive")
	if err != nil {
		return false, helper.NewError("job_archive table", err)
	}
	return jobExists && jobArchiveExists, err
}

// CreateTable creates the 'job' and 'job_archive' tables in the database.
// If the tables already exist, it does not create them again.
// It also creates a trigger for notifying events on the table and all necessary indexes.
func (r JobDBHandler) CreateTable() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Use the SQL init() function to create all tables, triggers, and indexes
	_, err := r.db.Instance.ExecContext(ctx, `SELECT init_job();`)
	if err != nil {
		log.Panicf("error initializing job tables: %#v", err)
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
		return helper.NewError("job table", err)
	}

	query = `DROP TABLE IF EXISTS job_archive`
	_, err = r.db.Instance.ExecContext(ctx, query)
	if err != nil {
		return helper.NewError("job_archive table", err)
	}

	r.db.Logger.Info("Dropped table job")

	return nil
}

// InsertJob inserts a new job record into the database.
func (r JobDBHandler) InsertJob(job *model.Job) (*model.Job, error) {
	newJob := &model.Job{}
	row := r.db.Instance.QueryRow(
		`SELECT * FROM insert_job($1, $2, $3, $4, $5, $6, $7)`,
		job.Options,
		job.TaskName,
		job.Parameters,
		job.ParametersKeyed,
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
		&newJob.ParametersKeyed,
		&newJob.Status,
		&newJob.ScheduledAt,
		&newJob.ScheduleCount,
		&newJob.Attempts,
		&newJob.CreatedAt,
		&newJob.UpdatedAt,
	)
	if err != nil {
		return nil, helper.NewError("scan", err)
	}

	return newJob, nil
}

// InsertJobTx inserts a new job record into the database within a transaction.
func (r JobDBHandler) InsertJobTx(tx *sql.Tx, job *model.Job) (*model.Job, error) {
	newJob := &model.Job{}
	row := tx.QueryRow(
		`SELECT * FROM insert_job($1, $2, $3, $4, $5, $6, $7)`,
		job.Options,
		job.TaskName,
		job.Parameters,
		job.ParametersKeyed,
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
		&newJob.ParametersKeyed,
		&newJob.Status,
		&newJob.ScheduledAt,
		&newJob.ScheduleCount,
		&newJob.Attempts,
		&newJob.CreatedAt,
		&newJob.UpdatedAt,
	)
	if err != nil {
		return nil, helper.NewError("scan", err)
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
		return helper.NewError("transaction start", err)
	}

	stmt, err := tx.Prepare(pq.CopyIn("job", "options", "task_name", "parameters", "parameters_keyed", "scheduled_at"))
	if err != nil {
		return helper.NewError("statement preparation", err)
	}

	for _, job := range jobs {
		var err error
		optionsJSON := []byte("{}")
		parametersJSON := []byte("[]")
		parametersKeyedJSON := []byte("{}")

		if job.Options != nil {
			optionsJSON, err = job.Options.Marshal()
			if err != nil {
				return helper.NewError("marshaling job options", err)
			}
		}

		if job.Parameters != nil {
			parametersJSON, err = job.Parameters.Marshal()
			if err != nil {
				return helper.NewError("marshaling job parameters", err)
			}
		}

		if job.ParametersKeyed != nil {
			parametersKeyedJSON, err = job.ParametersKeyed.Marshal()
			if err != nil {
				return helper.NewError("marshaling job parameters_keyed", err)
			}
		}

		_, err = stmt.Exec(
			string(optionsJSON),
			job.TaskName,
			string(parametersJSON),
			string(parametersKeyedJSON),
			job.ScheduledAt,
		)
		if err != nil {
			return helper.NewError("batch insert execution", err)
		}
	}

	_, err = stmt.Exec()
	if err != nil {
		return helper.NewError("final batch insert execution", err)
	}

	err = stmt.Close()
	if err != nil {
		return helper.NewError("prepared statement close", err)
	}

	err = tx.Commit()
	if err != nil {
		return helper.NewError("transaction commit", err)
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
		`SELECT * FROM update_job_initial($1);`,
		worker.ID,
	)
	if err != nil {
		return nil, helper.NewError("query", err)
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
			&job.ParametersKeyed,
			&job.Status,
			&job.ScheduledAt,
			&job.StartedAt,
			&job.ScheduleCount,
			&job.Attempts,
			&job.CreatedAt,
			&job.UpdatedAt,
		)
		if err != nil {
			return nil, helper.NewError("scan", err)
		}

		jobs = append(jobs, job)
	}

	err = rows.Err()
	if err != nil {
		return []*model.Job{}, helper.NewError("rows error", err)
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
			`SELECT * FROM update_job_final_encrypted($1, $2, $3, $4, $5);`,
			job.ID,
			job.Status,
			job.Results,
			job.Error,
			r.EncryptionKey,
		)
	} else {
		row = r.db.Instance.QueryRow(
			`SELECT * FROM update_job_final($1, $2, $3, $4);`,
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
		&archivedJob.ParametersKeyed,
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
		return nil, helper.NewError("scan", err)
	}

	return archivedJob, nil
}

// UpdateStaleJobs updates all jobs to QUEUED status where the assigned worker is STOPPED
// so they can be picked up by available workers again. It returns the number of jobs that were updated.
// Jobs are considered stale if their assigned worker has STOPPED status.
func (r JobDBHandler) UpdateStaleJobs() (int, error) {
	var affectedRows int
	err := r.db.Instance.QueryRow(
		`SELECT update_stale_jobs($1, $2, $3, $4, $5)`,
		model.JobStatusQueued,
		model.JobStatusSucceeded,
		model.JobStatusCancelled,
		model.JobStatusFailed,
		model.WorkerStatusStopped,
	).Scan(&affectedRows)
	if err != nil {
		return 0, helper.NewError("update stale jobs", err)
	}

	return affectedRows, nil
}

// SelectJob retrieves a single job record from the database based on its RID.
func (r JobDBHandler) SelectJob(rid uuid.UUID) (*model.Job, error) {
	row := r.db.Instance.QueryRow(
		`SELECT * FROM select_job($1, $2)`,
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
		&job.ParametersKeyed,
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
		return nil, helper.NewError("scan", err)
	}

	return job, nil
}

// SelectAllJobs retrieves a paginated list of jobs for a all workers.
// It returns jobs that were created before the specified lastID, or the newest jobs if lastID is 0.
func (r JobDBHandler) SelectAllJobs(lastID int, entries int) ([]*model.Job, error) {
	rows, err := r.db.Instance.Query(
		`SELECT * FROM select_all_jobs($1, $2, $3)`,
		r.EncryptionKey,
		lastID,
		entries,
	)
	if err != nil {
		return []*model.Job{}, helper.NewError("query", err)
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
			&job.ParametersKeyed,
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
			return []*model.Job{}, helper.NewError("scan", err)
		}

		jobs = append(jobs, job)
	}

	err = rows.Err()
	if err != nil {
		return []*model.Job{}, helper.NewError("rows error", err)
	}

	return jobs, nil
}

// SelectAllJobsByWorkerRID retrieves a paginated list of jobs for a specific worker, filtered by worker RID.
// It returns jobs that were created before the specified lastID, or the newest jobs if lastID is 0.
func (r JobDBHandler) SelectAllJobsByWorkerRID(workerRid uuid.UUID, lastID int, entries int) ([]*model.Job, error) {
	rows, err := r.db.Instance.Query(
		`SELECT * FROM select_all_jobs_by_worker_rid($1, $2, $3, $4)`,
		r.EncryptionKey,
		workerRid,
		lastID,
		entries,
	)
	if err != nil {
		return []*model.Job{}, helper.NewError("query", err)
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
			&job.ParametersKeyed,
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
			return []*model.Job{}, helper.NewError("scan", err)
		}

		jobs = append(jobs, job)
	}

	err = rows.Err()
	if err != nil {
		return []*model.Job{}, helper.NewError("rows error", err)
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
	rows, err := r.db.Instance.Query(
		`SELECT * FROM select_all_jobs_by_search($1, $2, $3, $4)`,
		r.EncryptionKey,
		search,
		lastID,
		entries,
	)
	if err != nil {
		return []*model.Job{}, helper.NewError("query", err)
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
			&job.ParametersKeyed,
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
			return []*model.Job{}, helper.NewError("scan", err)
		}

		jobs = append(jobs, job)
	}

	err = rows.Err()
	if err != nil {
		return []*model.Job{}, helper.NewError("rows error", err)
	}

	return jobs, nil
}

// Job Archive

// AddRetentionArchive updates the retention archive settings for the job archive.
func (r JobDBHandler) AddRetentionArchive(retention time.Duration) error {
	_, err := r.db.Instance.Exec(
		`SELECT add_retention_archive($1)`,
		int(retention.Hours()/24),
	)
	if err != nil {
		return helper.NewError("exec", err)
	}

	return nil
}

// RemoveRetentionArchive removes the retention archive settings for the job archive.
func (r JobDBHandler) RemoveRetentionArchive() error {
	_, err := r.db.Instance.Exec(
		`SELECT remove_retention_archive()`,
	)
	if err != nil {
		return helper.NewError("exec", err)
	}

	return nil
}

// DeleteJob deletes a job record from the job archive based on its RID.
// We only delete jobs from the archive as queued and running jobs should be cancelled first.
// Cancelling a job will move it to the archive with CANCELLED status.
func (r JobDBHandler) DeleteJob(rid uuid.UUID) error {
	_, err := r.db.Instance.Exec(
		`SELECT delete_job($1::UUID)`,
		rid,
	)
	if err != nil {
		return helper.NewError("exec", err)
	}

	return nil
}

// SelectJobFromArchive retrieves a single archived job record from the database based on its RID.
func (r JobDBHandler) SelectJobFromArchive(rid uuid.UUID) (*model.Job, error) {
	row := r.db.Instance.QueryRow(
		`SELECT * FROM select_job_from_archive($1, $2)`,
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
		&job.ParametersKeyed,
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
		return nil, helper.NewError("scan", err)
	}

	return job, nil
}

// SelectAllJobsFromArchive retrieves a paginated list of archived jobs.
// It returns jobs that were created before the specified lastID, or the newest jobs if lastID is 0.
func (r JobDBHandler) SelectAllJobsFromArchive(lastID int, entries int) ([]*model.Job, error) {
	rows, err := r.db.Instance.Query(
		`SELECT * FROM select_all_jobs_from_archive($1, $2, $3)`,
		r.EncryptionKey,
		lastID,
		entries,
	)
	if err != nil {
		return []*model.Job{}, helper.NewError("query", err)
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
			&job.ParametersKeyed,
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
			return []*model.Job{}, helper.NewError("scan", err)
		}

		jobs = append(jobs, job)
	}

	err = rows.Err()
	if err != nil {
		return []*model.Job{}, helper.NewError("rows error", err)
	}

	return jobs, nil
}

// SelectAllJobsFromArchiveBySearch retrieves a paginated list of archived jobs filtered by search string.
// It searches across 'rid', 'worker_id', 'task_name', and 'status' fields.
// It returns jobs that were created before the specified lastID, or the newest jobs if lastID
func (r JobDBHandler) SelectAllJobsFromArchiveBySearch(search string, lastID int, entries int) ([]*model.Job, error) {
	rows, err := r.db.Instance.Query(
		`SELECT * FROM select_all_jobs_from_archive_by_search($1, $2, $3, $4)`,
		r.EncryptionKey,
		search,
		lastID,
		entries,
	)
	if err != nil {
		return []*model.Job{}, helper.NewError("query", err)
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
			&job.ParametersKeyed,
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
			return []*model.Job{}, helper.NewError("scan", err)
		}

		jobs = append(jobs, job)
	}

	err = rows.Err()
	if err != nil {
		return []*model.Job{}, helper.NewError("rows error", err)
	}

	return jobs, nil
}
