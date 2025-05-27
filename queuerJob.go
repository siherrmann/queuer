package queuer

import (
	"fmt"
	"queuer/core"
	"queuer/helper"
	"queuer/model"
)

func (q *Queuer) AddJob(task interface{}, parameters ...interface{}) (*model.Job, error) {
	taskName, err := helper.GetFunctionName(task)
	if err != nil {
		return nil, fmt.Errorf("error getting task name: %v", err)
	}

	newJob, err := model.NewJob(taskName, parameters...)
	if err != nil {
		return nil, fmt.Errorf("error creating job: %v", err)
	}

	job, err := q.dbJob.InsertJob(newJob)
	if err != nil {
		return nil, fmt.Errorf("error inserting job: %v", err)
	}

	q.log.Printf("Job added with RID %v", job.RID)

	return job, nil
}

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

func (q *Queuer) RunJob() error {
	// Update job status to running with worker.
	job, err := q.dbJob.UpdateJobInitial(q.worker)
	if err != nil {
		return fmt.Errorf("error updating job status to running: %v", err)
	} else if job == nil {
		return nil
	}

	q.log.Printf("Running job with RID %v", job.RID)

	// Run job and update job status to completed with results
	// TODO At this point the task should be available in the queuer,
	// but we should probably still check if the task is available?
	task := q.tasks[job.TaskName]
	runner, err := core.NewRunner(task, job)
	if err != nil {
		q.FailJob(job, fmt.Errorf("error creating runner: %v", err))
	}

	resultValues, err := runner.Run()
	if err != nil {
		q.FailJob(job, err)
	} else {
		q.SucceedJob(job, resultValues)
	}

	return nil
}

func (q *Queuer) RetryingJob(job *model.Job) error {
	q.log.Printf("Retrying job with RID %v", job.RID)

	// Run job and update job status to completed with results
	// TODO At this point the task should be available in the queuer,
	// but we should probably still check if the task is available?
	task := q.tasks[job.TaskName]
	runner, err := core.NewRunner(task, job)
	if err != nil {
		return fmt.Errorf("error creating runner: %v", err)
	}

	resultValues, err := runner.Run()
	if err != nil {
		return fmt.Errorf("error running job: %v", err)
	}

	q.SucceedJob(job, resultValues)
	return nil
}

func (q *Queuer) SucceedJob(job *model.Job, results []interface{}) {
	job.Status = model.JobStatusSucceeded
	job.Results = results
	_, err := q.dbJob.UpdateJobFinal(job)
	if err != nil {
		// TODO probably add retry for updating job to succeeded
		q.log.Printf("error updating job status to succeeded: %v", err)
	}
	q.log.Printf("Job succeeded with RID %v", job.RID)
}

func (q *Queuer) FailJob(job *model.Job, jobErr error) {
	if job.Options != nil && job.Options.MaxRetries > 0 {
		retryer, err := core.NewRetryer(
			func() error {
				return q.RetryingJob(job)
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
}
