package queuer

import (
	"database/sql"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/siherrmann/queuer/core"
	"github.com/siherrmann/queuer/helper"
	"github.com/siherrmann/queuer/model"
)

// AddJob adds a job to the queue with the given task and parameters.
// As a task you can either pass a function or a string with the task name
// (necessary if you want to use a task with a name set by you).
// It returns the created job or an error if something goes wrong.
func (q *Queuer) AddJob(task interface{}, parameters ...interface{}) (*model.Job, error) {
	options := q.mergeOptions(nil)
	job, err := q.addJob(task, options, parameters...)
	if err != nil {
		q.log.Error("Error adding job", slog.String("error", err.Error()))
		return nil, fmt.Errorf("error adding job: %v", err)
	}

	q.log.Info("Job added", slog.String("job_rid", job.RID.String()))

	return job, nil
}

// AddJobTx adds a job to the queue with the given task and parameters within a transaction.
// As a task you can either pass a function or a string with the task name
// (necessary if you want to use a task with a name set by you).
// It returns the created job or an error if something goes wrong.
func (q *Queuer) AddJobTx(tx *sql.Tx, task interface{}, parameters ...interface{}) (*model.Job, error) {
	options := q.mergeOptions(nil)
	job, err := q.addJobTx(tx, task, options, parameters...)
	if err != nil {
		q.log.Error("Error adding job with transaction", slog.String("error", err.Error()))
		return nil, fmt.Errorf("error adding job: %v", err)
	}

	q.log.Info("Job added", slog.String("job_rid", job.RID.String()))

	return job, nil
}

/*
AddJobWithOptions adds a job with the given task, options, and parameters.
As a task you can either pass a function or a string with the task name
(necessary if you want to use a task with a name set by you).

It returns the created job or an error if something goes wrong.
The options parameter allows you to specify additional options for the job,
such as scheduling, retry policies, and error handling.
If options are nil, the worker's default options will be used.

Example usage:

	func AddJobExample(queuer *Queuer, param1 string, param2 int) {
		options := &model.Options{
			OnError: &model.OnError{
				Timeout:      5,
				MaxRetries:   2, Runs 3 times, first is not a retry
				RetryDelay:   1,
				RetryBackoff: model.RETRY_BACKOFF_NONE,
			},
			Schedule: &model.Schedule{
				Start:         time.Now().Add(10 * time.Second),
				Interval:      5 * time.Second,
				MaxCount:      3,
			},
		}
		job, err := queuer.AddJobWithOptions(options, myTaskFunction, param1, param2)
		if err != nil {
			log.Fatalf("Failed to add job: %v", err)
		}
	}
*/
func (q *Queuer) AddJobWithOptions(options *model.Options, task interface{}, parameters ...interface{}) (*model.Job, error) {
	q.mergeOptions(options)
	job, err := q.addJob(task, options, parameters...)
	if err != nil {
		q.log.Error("Error adding job with options", slog.String("error", err.Error()))
		return nil, fmt.Errorf("error adding job: %v", err)
	}

	q.log.Info("Job with options added", slog.String("job_rid", job.RID.String()))

	return job, nil
}

// AddJobWithOptionsTx adds a job with the given task, options, and parameters within a transaction.
// As a task you can either pass a function or a string with the task name
// (necessary if you want to use a task with a name set by you).
// It returns the created job or an error if something goes wrong.
func (q *Queuer) AddJobWithOptionsTx(tx *sql.Tx, options *model.Options, task interface{}, parameters ...interface{}) (*model.Job, error) {
	q.mergeOptions(options)
	job, err := q.addJobTx(tx, task, options, parameters...)
	if err != nil {
		q.log.Error("Error adding job with transaction and options", slog.String("error", err.Error()))
		return nil, fmt.Errorf("error adding job: %v", err)
	}

	q.log.Info("Job with options added", slog.String("job_rid", job.RID.String()))

	return job, nil
}

// AddJobs adds a batch of jobs to the queue.
// It takes a slice of BatchJob, which contains the task, options, and parameters for each job.
// It returns an error if something goes wrong during the process.
func (q *Queuer) AddJobs(batchJobs []model.BatchJob) error {
	var jobs []*model.Job
	for _, batchJob := range batchJobs {
		options := q.mergeOptions(batchJob.Options)
		newJob, err := model.NewJob(batchJob.Task, options, batchJob.Parameters...)
		if err != nil {
			return fmt.Errorf("error creating job with job options: %v", err)
		}

		jobs = append(jobs, newJob)
	}

	err := q.dbJob.BatchInsertJobs(jobs)
	if err != nil {
		q.log.Error("Error inserting jobs", slog.String("error", err.Error()))
		return fmt.Errorf("error inserting jobs: %v", err)
	}

	q.log.Info("Jobs added", slog.Int("count", len(jobs)))

	return nil
}

// WaitForJobAdded waits for any job to start and returns the job.
// It listens for job insert events and returns the job when it is added to the queue.
func (q *Queuer) WaitForJobAdded() *model.Job {
	jobStarted := make(chan *model.Job, 1)
	outerReady := make(chan struct{})
	ready := make(chan struct{})
	go func() {
		close(outerReady)
		q.jobInsertListener.Listen(q.ctx, ready, func(job *model.Job) {
			jobStarted <- job
		})
	}()

	<-outerReady
	<-ready
	for {
		select {
		case job := <-jobStarted:
			return job
		case <-q.ctx.Done():
			return nil
		}
	}
}

// WaitForJobFinished waits for a job to finish and returns the job.
// It listens for job delete events and returns the job when it is finished.
func (q *Queuer) WaitForJobFinished(jobRid uuid.UUID) *model.Job {
	jobFinished := make(chan *model.Job, 1)
	outerReady := make(chan struct{})
	ready := make(chan struct{})
	go func() {
		close(outerReady)
		q.jobDeleteListener.Listen(q.ctx, ready, func(job *model.Job) {
			if job.RID == jobRid {
				jobFinished <- job
			}
		})
	}()

	<-outerReady
	<-ready
	for {
		select {
		case job := <-jobFinished:
			return job
		case <-q.ctx.Done():
			return nil
		}
	}
}

// CancelJob cancels a job with the given job RID.
// It retrieves the job from the database and cancels it.
// If the job is not found or already cancelled, it returns an error.
func (q *Queuer) CancelJob(jobRid uuid.UUID) (*model.Job, error) {
	job, err := q.dbJob.SelectJob(jobRid)
	if err != nil {
		return nil, fmt.Errorf("error selecting job with rid %v, but already cancelled: %v", jobRid, err)
	}

	err = q.cancelJob(job)
	if err != nil {
		return nil, fmt.Errorf("error cancelling job with rid %v: %v", jobRid, err)
	}

	return job, nil
}

// CancelAllJobsByWorker cancels all jobs assigned to a specific worker by its RID.
// It retrieves all jobs assigned to the worker and cancels each one.
// It returns an error if something goes wrong during the process.
func (q *Queuer) CancelAllJobsByWorker(workerRid uuid.UUID, entries int) error {
	jobs, err := q.dbJob.SelectAllJobsByWorkerRID(workerRid, 0, entries)
	if err != nil {
		return fmt.Errorf("error selecting jobs by worker RID %v: %v", workerRid, err)
	}

	for _, job := range jobs {
		err := q.cancelJob(job)
		if err != nil {
			return fmt.Errorf("error cancelling job with rid %v: %v", job.RID, err)
		}
	}
	return nil
}

// ReaddJobFromArchive readds a job from the archive back to the queue.
func (q *Queuer) ReaddJobFromArchive(jobRid uuid.UUID) (*model.Job, error) {
	job, err := q.dbJob.SelectJobFromArchive(jobRid)
	if err != nil {
		return nil, fmt.Errorf("error selecting job from archive with rid %v: %v", jobRid, err)
	}

	// Readd the job to the queue
	newJob, err := q.AddJobWithOptions(job.Options, job.TaskName, job.Parameters...)
	if err != nil {
		return nil, fmt.Errorf("error readding job: %v", err)
	}

	q.log.Info("Job readded", slog.String("job_rid", newJob.RID.String()))

	return newJob, nil
}

// GetJob retrieves a job by its RID.
func (q *Queuer) GetJob(jobRid uuid.UUID) (*model.Job, error) {
	job, err := q.dbJob.SelectJob(jobRid)
	if err != nil {
		return nil, fmt.Errorf("error selecting job with rid %v: %v", jobRid, err)
	}

	return job, nil
}

// GetJobs retrieves all jobs in the queue.
func (q *Queuer) GetJobs(lastId int, entries int) ([]*model.Job, error) {
	jobs, err := q.dbJob.SelectAllJobs(lastId, entries)
	if err != nil {
		return nil, fmt.Errorf("error selecting all jobs: %v", err)
	}

	return jobs, nil
}

// GetJobEnded retrieves a job that has ended (succeeded, cancelled or failed) by its RID.
func (q *Queuer) GetJobEnded(jobRid uuid.UUID) (*model.Job, error) {
	job, err := q.dbJob.SelectJobFromArchive(jobRid)
	if err != nil {
		return nil, fmt.Errorf("error selecting ended job with rid %v: %v", jobRid, err)
	}

	return job, nil
}

// GetJobsEnded retrieves all jobs that have ended (succeeded, cancelled or failed).
func (q *Queuer) GetJobsEnded(lastId int, entries int) ([]*model.Job, error) {
	jobs, err := q.dbJob.SelectAllJobsFromArchive(lastId, entries)
	if err != nil {
		return nil, fmt.Errorf("error selecting ended jobs: %v", err)
	}

	return jobs, nil
}

// GetJobsByWorkerRID retrieves jobs assigned to a specific worker by its RID.
func (q *Queuer) GetJobsByWorkerRID(workerRid uuid.UUID, lastId int, entries int) ([]*model.Job, error) {
	jobs, err := q.dbJob.SelectAllJobsByWorkerRID(workerRid, lastId, entries)
	if err != nil {
		return nil, fmt.Errorf("error selecting jobs by worker RID %v: %v", workerRid, err)
	}

	return jobs, nil
}

// Internal

// mergeOptions merges the worker options with optional job options.
func (q *Queuer) mergeOptions(options *model.Options) *model.Options {
	q.workerMu.RLock()
	workerOptions := q.worker.Options
	q.workerMu.RUnlock()

	if options != nil && options.OnError == nil {
		options.OnError = workerOptions
	} else if options == nil && workerOptions != nil {
		options = &model.Options{OnError: workerOptions}
	}

	return options
}

// addJob adds a job to the queue with all necessary parameters.
func (q *Queuer) addJob(task interface{}, options *model.Options, parameters ...interface{}) (*model.Job, error) {
	newJob, err := model.NewJob(task, options, parameters...)
	if err != nil {
		return nil, helper.NewError("creating job", err)
	}

	job, err := q.dbJob.InsertJob(newJob)
	if err != nil {
		return nil, helper.NewError("inserting job", err)
	}

	return job, nil
}

// addJobTx adds a job to the queue with all necessary parameters.
func (q *Queuer) addJobTx(tx *sql.Tx, task interface{}, options *model.Options, parameters ...interface{}) (*model.Job, error) {
	newJob, err := model.NewJob(task, options, parameters...)
	if err != nil {
		return nil, helper.NewError("creating job", err)
	}

	job, err := q.dbJob.InsertJobTx(tx, newJob)
	if err != nil {
		return nil, helper.NewError("inserting job", err)
	}

	return job, nil
}

// runJobInitial is called to run the next job in the queue.
func (q *Queuer) runJobInitial() error {
	// Update job status to running with worker.
	q.workerMu.RLock()
	worker := q.worker
	q.workerMu.RUnlock()

	jobs, err := q.dbJob.UpdateJobsInitial(worker)
	if err != nil {
		return fmt.Errorf("error updating job status to running: %v", err)
	} else if len(jobs) == 0 {
		return nil
	}

	for _, job := range jobs {
		if job.Options != nil && job.Options.Schedule != nil && job.Options.Schedule.Start.After(time.Now()) {
			scheduler, err := core.NewScheduler(
				&job.Options.Schedule.Start,
				q.runJob,
				job,
			)
			if err != nil {
				return fmt.Errorf("error creating scheduler: %v", err)
			}

			q.log.Info("Scheduling job", slog.String("job_rid", job.RID.String()), slog.String("schedule_start", job.Options.Schedule.Start.String()))
			go scheduler.Go(q.ctx)
		} else {
			go q.runJob(job)
		}
	}

	return nil
}

// waitForJob executes the job and returns the results or an error.
func (q *Queuer) waitForJob(job *model.Job) (results []interface{}, cancelled bool, err error) {
	// Run job and update job status to completed with results
	// TODO At this point the task should be available in the queuer,
	// but we should probably still check if the task is available?
	task := q.tasks[job.TaskName]
	runner, err := core.NewRunnerFromJob(task, job)
	if err != nil {
		return nil, false, fmt.Errorf("error creating runner: %v", err)
	}

	q.activeRunners.Store(job.RID, runner)
	go runner.Run(q.ctx)

	select {
	case err = <-runner.ErrorChannel:
		break
	case results = <-runner.ResultsChannel:
		break
	case <-q.ctx.Done():
		runner.Cancel()
		return nil, true, nil
	}

	q.activeRunners.Delete(job.RID)

	return results, false, err
}

// retryJob retries the job with the given job error.
func (q *Queuer) retryJob(job *model.Job, jobErr error) {
	if job.Options == nil || job.Options.OnError == nil || job.Options.OnError.MaxRetries <= 0 {
		q.failJob(job, jobErr)
		return
	}

	var err error
	var results []interface{}
	retryer, err := core.NewRetryer(
		func() error {
			q.log.Debug("Trying/retrying job", slog.String("job_rid", job.RID.String()))
			results, _, err = q.waitForJob(job)
			if err != nil {
				return fmt.Errorf("error retrying job: %v", err)
			}
			return nil
		},
		job.Options.OnError,
	)
	if err != nil {
		jobErr = fmt.Errorf("error creating retryer: %v, original error: %v", err, jobErr)
	}

	err = retryer.Retry()
	if err != nil {
		q.failJob(job, fmt.Errorf("error retrying job: %v, original error: %v", err, jobErr))
	} else {
		q.succeedJob(job, results)
	}
}

// runJob retries the job.
func (q *Queuer) runJob(job *model.Job) {
	q.log.Info("Running job", slog.String("job_rid", job.RID.String()))

	results, cancelled, err := q.waitForJob(job)
	if cancelled {
		return
	} else if err != nil {
		q.retryJob(job, err)
	} else {
		q.succeedJob(job, results)
	}
}

func (q *Queuer) cancelJob(job *model.Job) error {
	if job.Status == model.JobStatusRunning || job.Status == model.JobStatusScheduled || job.Status == model.JobStatusQueued {
		job.Status = model.JobStatusCancelled
		_, err := q.dbJob.UpdateJobFinal(job)
		if err != nil && err.(helper.Error).Original != sql.ErrNoRows {
			q.log.Error("Error updating job status to cancelled", slog.String("error", err.Error()))
		} else if err == nil {
			q.log.Info("Job cancelled", slog.String("job_rid", job.RID.String()))
		}
	}
	return nil
}

// succeedJob updates the job status to succeeded and runs the next job if available.
func (q *Queuer) succeedJob(job *model.Job, results []interface{}) {
	job.Status = model.JobStatusSucceeded
	job.Results = results
	q.endJob(job)
}

func (q *Queuer) failJob(job *model.Job, jobErr error) {
	job.Status = model.JobStatusFailed
	job.Error = jobErr.Error()
	q.endJob(job)
}

func (q *Queuer) endJob(job *model.Job) {
	q.workerMu.RLock()
	workerID := q.worker.ID
	q.workerMu.RUnlock()

	if job.WorkerID != workerID {
		return
	}

	endedJob, err := q.dbJob.UpdateJobFinal(job)
	if err != nil {
		if strings.Contains(err.Error(), sql.ErrNoRows.Error()) {
			return
		}
		q.log.Error("Error updating finished job", slog.String("status", job.Status), slog.String("error", err.Error()))
	} else {
		q.log.Debug("Job ended", slog.String("status", endedJob.Status), slog.String("rid", endedJob.RID.String()))

		// Readd scheduled jobs to the queue
		if endedJob.Options != nil && endedJob.Options.Schedule != nil && endedJob.ScheduleCount < endedJob.Options.Schedule.MaxCount {
			var newScheduledAt time.Time
			if len(endedJob.Options.Schedule.NextInterval) > 0 {
				// This worker should only have the current job if the NextIntervalFunc is available.
				nextIntervalFunc, ok := q.nextIntervalFuncs[endedJob.Options.Schedule.NextInterval]
				if !ok {
					q.log.Error("NextIntervalFunc not found", slog.String("name", endedJob.Options.Schedule.NextInterval), slog.String("job_rid", endedJob.RID.String()))
					return
				}
				newScheduledAt = nextIntervalFunc(*endedJob.ScheduledAt, endedJob.ScheduleCount)
			} else {
				newScheduledAt = endedJob.ScheduledAt.Add(time.Duration(endedJob.ScheduleCount) * endedJob.Options.Schedule.Interval)
			}

			endedJob.ScheduledAt = &newScheduledAt
			endedJob.Status = model.JobStatusScheduled
			job, err := q.dbJob.InsertJob(endedJob)
			if err != nil {
				q.log.Error("Error readding scheduled job", slog.String("job_rid", endedJob.RID.String()), slog.String("error", err.Error()))
			}
			q.log.Info("Job added for next iteration to the queue", slog.String("job_rid", job.RID.String()))
		}
	}

	// Try to run the next job in the queue
	err = q.runJobInitial()
	if err != nil {
		q.log.Error("Error running next job", slog.String("error", err.Error()))
	}
}
