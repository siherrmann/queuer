package queuer

import (
	"fmt"
	"queuer/core"
	"queuer/helper"
	"queuer/model"

	"github.com/google/uuid"
)

// AddJob adds a job to the queue with the given task and parameters.
func (q *Queuer) AddJob(task interface{}, parameters ...interface{}) (*model.Job, error) {
	taskName, err := helper.GetFunctionName(task)
	if err != nil {
		return nil, fmt.Errorf("error getting task name: %v", err)
	}

	var newJob *model.Job
	if q.worker.Options != nil {
		newJob, err = model.NewJobWithOptions(taskName, q.worker.Options, parameters...)
		if err != nil {
			return nil, fmt.Errorf("error creating job: %v", err)
		}
	} else {
		newJob, err = model.NewJob(taskName, parameters...)
		if err != nil {
			return nil, fmt.Errorf("error creating job: %v", err)
		}
	}

	job, err := q.dbJob.InsertJob(newJob)
	if err != nil {
		return nil, fmt.Errorf("error inserting job: %v", err)
	}

	q.log.Printf("Job added with RID %v", job.RID)

	return job, nil
}

type BatchJob struct {
	Task       interface{}
	Parameters []interface{}
	Options    *model.Options
}

// AddJobs adds a batch of jobs to the queue.
func (q *Queuer) AddJobs(batchJobs []BatchJob) error {
	var jobs []*model.Job
	for _, batchJob := range batchJobs {
		taskName, err := helper.GetFunctionName(batchJob.Task)
		if err != nil {
			return fmt.Errorf("error getting task name: %v", err)
		}

		var newJob *model.Job
		if batchJob.Options != nil {
			newJob, err = model.NewJobWithOptions(taskName, batchJob.Options, batchJob.Parameters...)
			if err != nil {
				return fmt.Errorf("error creating job with job options: %v", err)
			}
		} else if q.worker.Options != nil {
			newJob, err = model.NewJobWithOptions(taskName, q.worker.Options, batchJob.Parameters...)
			if err != nil {
				return fmt.Errorf("error creating job with worker options: %v", err)
			}
		} else {
			newJob, err = model.NewJob(taskName, batchJob.Parameters...)
			if err != nil {
				return fmt.Errorf("error creating job: %v", err)
			}
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

// AddJobWithOptions adds a job with the given task, options, and parameters.
func (q *Queuer) AddJobWithOptions(task interface{}, options *model.Options, parameters ...interface{}) (*model.Job, error) {
	taskName, err := helper.GetFunctionName(task)
	if err != nil {
		return nil, fmt.Errorf("error getting task name: %v", err)
	}

	newJob, err := model.NewJobWithOptions(taskName, options, parameters...)
	if err != nil {
		return nil, fmt.Errorf("error creating job: %v", err)
	}

	job, err := q.dbJob.InsertJob(newJob)
	if err != nil {
		return nil, err
	}

	q.log.Printf("Job with options added with RID %v", job.RID)

	return job, nil
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

// Internal

// runJobInitial is called to run the next job in the queue.
func (q *Queuer) runJobInitial() error {
	// Update job status to running with worker.
	job, err := q.dbJob.UpdateJobInitial(q.worker)
	if err != nil {
		return fmt.Errorf("error updating job status to running: %v", err)
	} else if job == nil {
		return nil
	}

	q.log.Printf("Running job with RID %v", job.RID)

	resultValues, err := q.runJob(job)
	if err != nil {
		q.failJob(job, err)
	} else {
		q.succeedJob(job, resultValues)
	}

	return nil
}

// runJob executes the job and returns the results or an error.
func (q *Queuer) runJob(job *model.Job) ([]interface{}, error) {
	// Run job and update job status to completed with results
	// TODO At this point the task should be available in the queuer,
	// but we should probably still check if the task is available?
	task := q.tasks[job.TaskName]
	runner, err := core.NewRunner(task, job)
	if err != nil {
		return []interface{}{}, fmt.Errorf("error creating runner: %v", err)
	}

	q.activeRunners.Store(job.RID, runner)
	resultValues, err := runner.Run(q.ctx)
	q.activeRunners.Delete(job.RID)
	if err != nil {
		return []interface{}{}, fmt.Errorf("error running job: %v", err)
	}

	return resultValues, nil
}

// retryJob retries the job.
func (q *Queuer) retryJob(job *model.Job) error {
	q.log.Printf("Retrying job with RID %v", job.RID)

	resultValues, err := q.runJob(job)
	if err != nil {
		return fmt.Errorf("error retrying job: %v", err)
	}

	q.succeedJob(job, resultValues)
	return nil
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

// failJob updates the job status to failed and retries if configured.
func (q *Queuer) failJob(job *model.Job, jobErr error) {
	if job.Options != nil && job.Options.MaxRetries > 0 {
		retryer, err := core.NewRetryer(
			func() error {
				return q.retryJob(job)
			},
			job.Options,
		)
		if err != nil {
			jobErr = fmt.Errorf("error creating retryer: %v, initial error: %v", err, jobErr)
		}

		err = retryer.Retry()
		if err != nil {
			jobErr = fmt.Errorf("job failed after retries: %v, initial error: %v", err, jobErr)
		}
	}

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
