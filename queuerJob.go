package queuer

import (
	"fmt"
	"queuer/core"
	"queuer/helper"
	"queuer/model"
	"time"

	"github.com/google/uuid"
)

// AddJob adds a job to the queue with the given task and parameters.
func (q *Queuer) AddJob(task interface{}, parameters ...interface{}) (*model.Job, error) {
	var options *model.Options
	if q.worker.Options != nil {
		options = &model.Options{OnError: q.worker.Options}
	}

	job, err := q.addJob(task, options, parameters...)
	if err != nil {
		return nil, fmt.Errorf("error adding job: %v", err)
	}

	q.log.Printf("Job added with RID %v", job.RID)

	return job, nil
}

// AddJobWithOptions adds a job with the given task, options, and parameters.
func (q *Queuer) AddJobWithOptions(task interface{}, options *model.Options, parameters ...interface{}) (*model.Job, error) {
	job, err := q.addJob(task, options, parameters...)
	if err != nil {
		return nil, fmt.Errorf("error adding job: %v", err)
	}

	q.log.Printf("Job with options added with RID %v", job.RID)

	return job, nil
}

// AddJobs adds a batch of jobs to the queue.
func (q *Queuer) AddJobs(batchJobs []model.BatchJob) error {
	var jobs []*model.Job
	for _, batchJob := range batchJobs {
		taskName, err := helper.GetTaskNameFromInterface(batchJob.Task)
		if err != nil {
			return fmt.Errorf("error getting task name: %v", err)
		}

		var options *model.Options
		if batchJob.Options != nil {
			options = batchJob.Options
		} else if q.worker.Options != nil {
			options = &model.Options{OnError: q.worker.Options}
		}

		newJob, err := model.NewJob(taskName, options, batchJob.Parameters...)
		if err != nil {
			return fmt.Errorf("error creating job with job options: %v", err)
		}

		jobs = append(jobs, newJob)
	}

	err := q.dbJob.BatchInsertJobs(jobs)
	if err != nil {
		return fmt.Errorf("error inserting jobs: %v", err)
	}

	q.log.Printf("%v jobs added", len(jobs))

	return nil
}

// CancelJob cancels a job with the given job RID.
func (q *Queuer) CancelJob(jobRid uuid.UUID) error {
	jobRunner, found := q.activeRunners.Load(jobRid)
	if !found {
		return fmt.Errorf("job with rid %v not found or not running", jobRid)
	}

	runner := jobRunner.(*core.Runner) // Type assertion
	runner.Cancel(func() {
		job, err := q.dbJob.SelectJob(jobRid)
		if err != nil {
			q.log.Printf("error selecting job with rid %v, but already cancelled: %v", jobRid, err)
		}

		job.Status = model.JobStatusCancelled
		_, err = q.dbJob.UpdateJobFinal(job)
		if err != nil {
			q.log.Printf("error updating job status to cancelled: %v", err)
		}

		q.log.Printf("Job cancelled with RID %v", job.RID)
	})

	return nil
}

// ReaddJobFromArchive readds a job from the archive back to the queue.
func (q *Queuer) ReaddJobFromArchive(jobRid uuid.UUID) error {
	job, err := q.dbJob.SelectJobFromArchive(jobRid)
	if err != nil {
		return fmt.Errorf("error selecting job from archive with rid %v: %v", jobRid, err)
	}

	// Readd the job to the queue
	newJob, err := q.AddJobWithOptions(job.TaskName, job.Options, job.Parameters...)
	if err != nil {
		return fmt.Errorf("error readding job: %v", err)
	}

	q.log.Printf("Job readded with RID %v", newJob.RID)

	return nil
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

// GetJobsByWorkerRID retrieves jobs assigned to a specific worker by its RID.
func (q *Queuer) GetJobsByWorkerRID(workerRid uuid.UUID, lastId int, entries int) ([]*model.Job, error) {
	jobs, err := q.dbJob.SelectAllJobsByWorkerRID(workerRid, lastId, entries)
	if err != nil {
		return nil, fmt.Errorf("error selecting jobs by worker RID %v: %v", workerRid, err)
	}

	return jobs, nil
}

// Internal

// addJob adds a job to the queue with all necessary parameters.
func (q *Queuer) addJob(task interface{}, options *model.Options, parameters ...interface{}) (*model.Job, error) {
	taskName, err := helper.GetTaskNameFromInterface(task)
	if err != nil {
		return nil, fmt.Errorf("error getting task name: %v", err)
	}

	newJob, err := model.NewJob(taskName, options, parameters...)
	if err != nil {
		return nil, fmt.Errorf("error creating job: %v", err)
	}

	job, err := q.dbJob.InsertJob(newJob)
	if err != nil {
		return nil, fmt.Errorf("error inserting job: %v", err)
	}

	return job, nil
}

// runJobInitial is called to run the next job in the queue.
func (q *Queuer) runJobInitial() error {
	// Update job status to running with worker.
	jobs, err := q.dbJob.UpdateJobsInitial(q.worker)
	if err != nil {
		return fmt.Errorf("error updating job status to running: %v", err)
	} else if len(jobs) == 0 {
		return nil
	}

	for _, job := range jobs {
		if job.Options != nil && job.Options.Schedule != nil && job.Options.Schedule.Start.After(time.Now()) {
			scheduler, err := core.NewScheduler(
				q.scheduleJob,
				&job.Options.Schedule.Start,
				job,
			)
			if err != nil {
				return fmt.Errorf("error creating scheduler: %v", err)
			}
			scheduler.Go(q.ctx)
		} else {
			go func() {
				q.log.Printf("Running job with RID %v", job.RID)
				resultValues, err := q.waitForJob(job)
				if err != nil {
					q.retryJob(job, err)
				} else {
					q.succeedJob(job, resultValues)
				}
			}()
		}
	}

	return nil
}

// waitForJob executes the job and returns the results or an error.
func (q *Queuer) waitForJob(job *model.Job) ([]interface{}, error) {
	// Run job and update job status to completed with results
	// TODO At this point the task should be available in the queuer,
	// but we should probably still check if the task is available?
	task := q.tasks[job.TaskName]
	runner, err := core.NewRunnerFromJob(task, job)
	if err != nil {
		return []interface{}{}, fmt.Errorf("error creating runner: %v", err)
	}

	var results []interface{}
	q.activeRunners.Store(job.RID, runner)
	go runner.Run(q.ctx)

	select {
	case err = <-runner.ErrorChannel:
		break
	case results = <-runner.ResultsChannel:
		break
	case <-q.ctx.Done():
		runner.Cancel()
		break
	}

	q.activeRunners.Delete(job.RID)

	return results, err
}

// retryJob retries the job with the given job error.
func (q *Queuer) retryJob(job *model.Job, jobErr error) {
	if job.Options == nil || job.Options.OnError.MaxRetries <= 0 {
		q.failJob(job, jobErr)
		return
	}

	var err error
	var results []interface{}
	retryer, err := core.NewRetryer(
		func() error {
			q.log.Printf("Trying/retrying job with RID %v", job.RID)
			results, err = q.waitForJob(job)
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

// scheduleJob retries the job.
func (q *Queuer) scheduleJob(job *model.Job) {
	q.log.Printf("Running scheduled job with RID %v", job.RID)

	resultValues, err := q.waitForJob(job)
	if err != nil {
		q.retryJob(job, err)
	} else {
		q.succeedJob(job, resultValues)
	}
}

// succeedJob updates the job status to succeeded and runs the next job if available.
func (q *Queuer) succeedJob(job *model.Job, results []interface{}) {
	job.Status = model.JobStatusSucceeded
	job.Results = results
	_, err := q.dbJob.UpdateJobFinal(job)
	if err != nil {
		// TODO probably add retry for updating job to succeeded
		q.log.Printf("error updating job status to succeeded: %v", err)
	}

	q.log.Printf("Job succeeded with RID %v", job.RID)

	// Try running next job if available
	err = q.runJobInitial()
	if err != nil {
		q.log.Printf("error running next job: %v", err)
	}
}

func (q *Queuer) failJob(job *model.Job, jobErr error) {
	job.Status = model.JobStatusFailed
	job.Error = jobErr.Error()
	_, err := q.dbJob.UpdateJobFinal(job)
	if err != nil {
		// TODO probably add retry for updating job to failed
		q.log.Printf("error updating job status to failed: %v", err)
	}

	q.log.Printf("Job failed with RID %v", job.RID)

	// Try running next job if available
	err = q.runJobInitial()
	if err != nil {
		q.log.Printf("error running next job: %v", err)
	}
}
