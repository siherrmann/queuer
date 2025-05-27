package queuer

import (
	"context"
	"fmt"
	"queuer/core"
	"queuer/helper"
	"queuer/model"
	"time"
)

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
	job, err := q.StartJob()
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
	// Update job status to succeeded with results
	job.Status = model.JobStatusSucceeded
	job.Results = results
	err := q.FinishJob(job)
	if err != nil {
		q.log.Printf("error finishing job: %v", err)
		return
	}

	q.log.Printf("Job succeeded with RID %v", job.RID)

	// Run new job if available
	if err := q.RunJob(); err != nil {
		q.log.Printf("error running new job after success: %v", err)
	}
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

	// Update job status to failed with error
	job.Status = model.JobStatusFailed
	job.Error = jobErr.Error()
	err := q.FinishJob(job)
	if err != nil {
		q.log.Printf("error finishing job after failure: %v", err)
		return
	}

	q.log.Printf("Job failed with RID %v", job.RID)

	// Run new job if available
	if err := q.RunJob(); err != nil {
		q.log.Printf("error running new job after failure: %v", err)
	}
}

// Transactions
func (q *Queuer) StartJob() (*model.Job, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Create new sql transaction to update worker and job status
	tx, err := q.dbJob.CreateTransaction(ctx)
	if err != nil {
		return nil, fmt.Errorf("error creating transaction: %v", err)
	}
	defer tx.Rollback()

	// Update worker current concurrency
	worker, err := q.dbWorker.UpdateWorkerInitialTx(tx, q.worker)
	if err != nil {
		tx.Rollback()
		return nil, fmt.Errorf("error updating worker initial: %v", err)
	} else if worker == nil {
		tx.Rollback()
		return nil, nil
	}

	// Update job status to running with worker
	job, err := q.dbJob.UpdateJobInitialTx(tx, q.worker)
	if err != nil {
		tx.Rollback()
		return nil, fmt.Errorf("error updating job status to running: %v", err)
	} else if job == nil {
		tx.Rollback()
		return nil, nil
	}

	err = tx.Commit()
	if err != nil {
		return nil, fmt.Errorf("error committing transaction for job start: %v", err)
	}

	return job, nil
}

func (q *Queuer) FinishJob(job *model.Job) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Create new sql transaction to update worker and job status
	tx, err := q.dbJob.CreateTransaction(ctx)
	if err != nil {
		return fmt.Errorf("error creating transaction: %v", err)
	}
	defer tx.Rollback()

	// Update worker current concurrency
	_, err = q.dbWorker.UpdateWorkerFinalTx(tx, q.worker)
	if err != nil {
		return fmt.Errorf("error updating worker final: %v", err)
	}

	// Update job status to succeeded and set results
	_, err = q.dbJob.UpdateJobFinalTx(tx, job)
	if err != nil {
		return fmt.Errorf("error updating job status to succeeded: %v", err)
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("error committing transaction for job success: %v", err)
	}

	return nil
}
