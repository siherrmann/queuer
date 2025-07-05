package queuer

import (
	"context"
	"fmt"
	"queuer/model"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Short running example task function
func TaskMock(duration int, param2 string) (int, error) {
	// Simulate some work
	time.Sleep(time.Duration(duration) * time.Second)

	// Example for some error handling
	param2Int, err := strconv.Atoi(param2)
	if err != nil {
		return 0, err
	}

	return duration + param2Int, nil
}

type MockFailer struct {
	count int
}

func (m *MockFailer) TaskMockFailing(duration int, maxFailCount string) (int, error) {
	m.count++

	// Simulate some work
	time.Sleep(time.Duration(duration) * time.Second)

	// Example for some error handling
	maxFailCountInt, err := strconv.Atoi(maxFailCount)
	if err != nil {
		return 0, err
	}

	if m.count < maxFailCountInt {
		return 0, fmt.Errorf("fake fail max count reached: %d", maxFailCountInt)
	}

	return duration + maxFailCountInt, nil
}

func TestAddJob(t *testing.T) {
	testQueuer := newQueuerMock("TestQueuer", 100)

	t.Run("Successfully adds a job with nil options", func(t *testing.T) {
		expectedJob := &model.Job{
			TaskName:   "queuer.TaskMock",
			Parameters: model.Parameters{1.0, "2"},
		}

		params := []interface{}{1, "2"}
		job, err := testQueuer.AddJob(TaskMock, params...)

		assert.NoError(t, err, "AddJob should not return an error on success")
		assert.Equal(t, expectedJob.TaskName, job.TaskName, "AddJob should return the correct task name")
		assert.EqualValues(t, expectedJob.Parameters, job.Parameters, "AddJob should return the correct parameters")
		assert.Equal(t, expectedJob.Options, job.Options, "AddJob should return the correct options")
	})

	t.Run("Returns error for nil function", func(t *testing.T) {
		var nilTask func() // Invalid nil function
		job, err := testQueuer.AddJob(nilTask, "param1")

		assert.Error(t, err, "AddJob should return an error for nil task (via addJobFn)")
		assert.Nil(t, job, "Job should be nil for nil task")
		assert.Contains(t, err.Error(), "task value must not be nil", "Error message should reflect nil task handling")
	})

	t.Run("Returns error for invalid task type", func(t *testing.T) {
		invalidTask := 123 // Invalid integer type instead of a function
		job, err := testQueuer.AddJob(invalidTask, "param1")

		assert.Error(t, err, "AddJob should return an error for invalid task type")
		assert.Nil(t, job, "Job should be nil for invalid task type")
		assert.Contains(t, err.Error(), "task must be a function, got int", "Error message should reflect invalid task type handling")
	})
}

func TestAddJobRunning(t *testing.T) {
	testQueuer := newQueuerMock("TestQueuer", 100)
	testQueuer.AddTask(TaskMock)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	testQueuer.Start(ctx, cancel)

	t.Run("Successfully runs a job without options", func(t *testing.T) {
		job, err := testQueuer.AddJob(TaskMock, 1, "2")
		assert.NoError(t, err, "AddJob should not return an error on success")

		queuedJob, err := testQueuer.GetJob(job.RID)
		assert.NoError(t, err, "GetJob should not return an error")
		assert.NotNil(t, queuedJob, "GetJob should return the job that is currently running")

		done := make(chan struct{})
		go func() {
			job = testQueuer.WaitForJobFinished(job.RID)
			assert.NotNil(t, job, "WaitForJobFinished should return the finished job")
			assert.Equal(t, model.JobStatusSucceeded, job.Status, "WaitForJobFinished should return job with status Succeeded")
			close(done)
		}()

	outerloop:
		for {
			select {
			case <-done:
				break outerloop
			case <-time.After(2 * time.Second):
				t.Fatal("WaitForJobFinished timed out waiting for job to finish")
			}
		}

		jobNotExisting, err := testQueuer.GetJob(job.RID)
		assert.Error(t, err, "GetJob should return an error for ended job")
		assert.Nil(t, jobNotExisting, "GetJob should return nil for ended job")

		jobArchived, err := testQueuer.dbJob.SelectJobFromArchive(job.RID)
		assert.NoError(t, err, "SelectJobFromArchive should not return an error for archived job")
		assert.NotNil(t, jobArchived, "SelectJobFromArchive should return the archived job")
		assert.Equal(t, jobArchived.Status, model.JobStatusSucceeded, "Archived job should have status Succeeded")
	})

	t.Run("Successfully runs a job with options", func(t *testing.T) {
		options := &model.Options{
			OnError: &model.OnError{
				Timeout:      5,
				MaxRetries:   3,
				RetryDelay:   1,
				RetryBackoff: model.RETRY_BACKOFF_EXPONENTIAL,
			},
			Schedule: &model.Schedule{
				Start:    time.Now().Add(1 * time.Second),
				Interval: 15 * time.Second,
				MaxCount: 3,
			},
		}

		job, err := testQueuer.AddJobWithOptions(options, TaskMock, 1, "2")
		require.NoError(t, err, "AddJob should not return an error on success")

		queuedJob, err := testQueuer.GetJob(job.RID)
		require.NoError(t, err, "GetJob should not return an error")
		require.NotNil(t, queuedJob, "GetJob should return the job that is currently running")
		assert.Equal(t, model.JobStatusScheduled, queuedJob.Status, "Job should be in Running status")

		done := make(chan struct{})
		go func() {
			job = testQueuer.WaitForJobFinished(job.RID)
			assert.NotNil(t, job, "WaitForJobFinished should return the finished job")
			assert.Equal(t, model.JobStatusSucceeded, job.Status, "WaitForJobFinished should return job with status Succeeded")
			close(done)
		}()

	outerloop:
		for {
			select {
			case <-done:
				break outerloop
			case <-time.After(3 * time.Second):
				t.Fatal("WaitForJobFinished timed out waiting for job to finish")
			}
		}

		// Check if the job is archived
		jobNotExisting, err := testQueuer.GetJob(job.RID)
		assert.Error(t, err, "GetJob should return an error for ended job")
		assert.Nil(t, jobNotExisting, "GetJob should return nil for ended job")

		jobArchived, err := testQueuer.dbJob.SelectJobFromArchive(job.RID)
		assert.NoError(t, err, "SelectJobFromArchive should not return an error for archived job")
		require.NotNil(t, jobArchived, "SelectJobFromArchive should return the archived job")
		assert.Equal(t, jobArchived.Status, model.JobStatusSucceeded, "Archived job should have status Succeeded")
	})
}

func TestAddJobTx(t *testing.T) {
	testQueuer := newQueuerMock("TestQueuer", 100)

	t.Run("Successfully adds a job with nil options in transaction", func(t *testing.T) {
		expectedJob := &model.Job{
			TaskName:   "queuer.TaskMock",
			Parameters: model.Parameters{1.0, "2"},
		}

		params := []interface{}{1, "2"}
		tx, err := testQueuer.DB.Begin()
		require.NoError(t, err, "Begin transaction should not return an error")

		job, err := testQueuer.AddJobTx(tx, TaskMock, params...)
		assert.NoError(t, err, "AddJobTx should not return an error on success")
		assert.Equal(t, expectedJob.TaskName, job.TaskName, "AddJobTx should return the correct task name")
		assert.EqualValues(t, expectedJob.Parameters, job.Parameters, "AddJobTx should return the correct parameters")
		assert.Equal(t, expectedJob.Options, job.Options, "AddJobTx should return the correct options")

		err = tx.Commit()
		assert.NoError(t, err, "Commit transaction should not return an error")
	})

	t.Run("Returns error for nil function in transaction", func(t *testing.T) {
		var nilTask func() // Invalid nil function
		tx, err := testQueuer.DB.Begin()
		require.NoError(t, err, "Begin transaction should not return an error")

		job, err := testQueuer.AddJobTx(tx, nilTask, "param1")
		assert.Error(t, err, "AddJobTx should return an error for nil task (via addJobFn)")
		assert.Nil(t, job, "Job should be nil for nil task")

		err = tx.Rollback()
		assert.NoError(t, err, "Rollback transaction should not return an error")
	})

	t.Run("Returns error for invalid task type in transaction", func(t *testing.T) {
		invalidTask := 123 // Invalid integer type instead of a function
		tx, err := testQueuer.DB.Begin()
		require.NoError(t, err, "Begin transaction should not return an error")

		job, err := testQueuer.AddJobTx(tx, invalidTask, "param1")
		assert.Error(t, err, "AddJobTx should return an error for invalid task type")
		assert.Nil(t, job, "Job should be nil for invalid task type")
		assert.Contains(t, err.Error(), "task must be a function, got int", "Error message should reflect invalid task type handling")

		err = tx.Rollback()
		assert.NoError(t, err, "Rollback transaction should not return an error")
	})
}

func TestAddJobWithOptions(t *testing.T) {
	testQueuer := newQueuerMock("TestQueuer", 100)

	t.Run("Successfully adds a job with options", func(t *testing.T) {
		options := &model.Options{
			OnError: &model.OnError{
				Timeout:      5,
				MaxRetries:   3,
				RetryDelay:   1,
				RetryBackoff: model.RETRY_BACKOFF_EXPONENTIAL,
			},
			Schedule: &model.Schedule{
				Start:    time.Now().Add(10 * time.Minute),
				Interval: 15 * time.Minute,
				MaxCount: 3,
			},
		}
		expectedJob := &model.Job{
			TaskName:   "queuer.TaskMock",
			Parameters: model.Parameters{1.0, "2"},
			Options:    options,
		}

		params := []interface{}{1, "2"}
		job, err := testQueuer.AddJobWithOptions(options, TaskMock, params...)

		assert.NoError(t, err, "AddJobWithOptions should not return an error on success")
		assert.Equal(t, expectedJob.TaskName, job.TaskName, "AddJobWithOptions should return the correct task name")
		assert.EqualValues(t, expectedJob.Parameters, job.Parameters, "AddJobWithOptions should return the correct parameters")
		assert.EqualValues(t, expectedJob.Options.OnError, job.Options.OnError, "AddJobWithOptions should return the correct OnError options")
		assert.EqualExportedValues(t, expectedJob.Options.Schedule, job.Options.Schedule, "AddJobWithOptions should return the correct Schedule options")
	})

	t.Run("Successfully adds a job with nil options", func(t *testing.T) {
		expectedJob := &model.Job{
			TaskName:   "queuer.TaskMock",
			Parameters: model.Parameters{1.0, "2"},
		}

		params := []interface{}{1, "2"}
		job, err := testQueuer.AddJobWithOptions(nil, TaskMock, params...)

		assert.NoError(t, err, "AddJobWithOptions should not return an error on success")
		assert.Equal(t, expectedJob.TaskName, job.TaskName, "AddJobWithOptions should return the correct task name")
		assert.EqualValues(t, expectedJob.Parameters, job.Parameters, "AddJobWithOptions should return the correct parameters")
	})

	t.Run("Return error for invalid options", func(t *testing.T) {
		options := &model.Options{
			OnError: &model.OnError{
				Timeout:      -5, // Invalid timeout
				MaxRetries:   3,
				RetryDelay:   1,
				RetryBackoff: model.RETRY_BACKOFF_EXPONENTIAL,
			},
			Schedule: &model.Schedule{
				Start:    time.Now().Add(10 * time.Minute),
				Interval: 15 * time.Minute,
				MaxCount: 3,
			},
		}

		params := []interface{}{1, "2"}
		job, err := testQueuer.AddJobWithOptions(options, TaskMock, params...)

		assert.Error(t, err, "AddJobWithOptions should return an error for invalid options")
		assert.Nil(t, job, "Job should be nil for invalid options")
	})
}

func TestAddJobWithOptionsRunning(t *testing.T) {
	newMockFailer := &MockFailer{}

	testQueuer := newQueuerMock("TestQueuer", 100)
	testQueuer.AddTask(newMockFailer.TaskMockFailing)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	testQueuer.Start(ctx, cancel)

	t.Run("Successfully retries a job with options", func(t *testing.T) {
		options := &model.Options{
			OnError: &model.OnError{
				Timeout:      5,
				MaxRetries:   3,
				RetryDelay:   1,
				RetryBackoff: model.RETRY_BACKOFF_NONE,
			},
		}

		job, err := testQueuer.AddJobWithOptions(options, newMockFailer.TaskMockFailing, 1, "3")
		assert.NoError(t, err, "AddJobWithOptions should not return an error on success")

		time.Sleep(4 * time.Second)

		jobRunning, err := testQueuer.GetJob(job.RID)
		assert.NoError(t, err, "GetJob should not return an error for running job")
		require.NotNil(t, jobRunning, "GetJob should return the job that is currently running")
		assert.Equal(t, model.JobStatusRunning, jobRunning.Status, "Job should be in Running status")

		time.Sleep(2 * time.Second)

		jobNotExisting, err := testQueuer.GetJob(job.RID)
		assert.Error(t, err, "GetJob should return an error for ended job")
		assert.Nil(t, jobNotExisting, "GetJob should return nil for ended job")

		jobArchived, err := testQueuer.dbJob.SelectJobFromArchive(job.RID)
		assert.NoError(t, err, "SelectJobFromArchive should not return an error for archived job")
		assert.NotNil(t, jobArchived, "SelectJobFromArchive should return the archived job")
		assert.Equal(t, model.JobStatusSucceeded, jobArchived.Status, "Archived job should have status Succeeded")
	})

	t.Run("Fails after max retries", func(t *testing.T) {
		options := &model.Options{
			OnError: &model.OnError{
				Timeout:      5,
				MaxRetries:   2, // Runs 3 times, first is not a retry
				RetryDelay:   1,
				RetryBackoff: model.RETRY_BACKOFF_NONE,
			},
		}

		job, err := testQueuer.AddJobWithOptions(options, newMockFailer.TaskMockFailing, 1, "100")
		assert.NoError(t, err, "AddJobWithOptions should not return an error on success")

		time.Sleep(4 * time.Second)

		jobRunning, err := testQueuer.GetJob(job.RID)
		assert.NoError(t, err, "GetJob should not return an error for running job")
		require.NotNil(t, jobRunning, "GetJob should return the job that is currently running")
		assert.Equal(t, model.JobStatusRunning, jobRunning.Status, "Job should be in Running status")

		time.Sleep(2 * time.Second)

		jobNotExisting, err := testQueuer.GetJob(job.RID)
		assert.Error(t, err, "GetJob should return an error for ended job")
		assert.Nil(t, jobNotExisting, "GetJob should return nil for ended job")

		jobArchived, err := testQueuer.dbJob.SelectJobFromArchive(job.RID)
		assert.NoError(t, err, "SelectJobFromArchive should not return an error for archived job")
		assert.NotNil(t, jobArchived, "SelectJobFromArchive should return the archived job")
		assert.Equal(t, model.JobStatusFailed, jobArchived.Status, "Archived job should have status Failed")
	})
}

func TestAddJobWithOptionsTx(t *testing.T) {
	testQueuer := newQueuerMock("TestQueuer", 100)

	t.Run("Successfully adds a job with options in transaction", func(t *testing.T) {
		options := &model.Options{
			OnError: &model.OnError{
				Timeout:      5,
				MaxRetries:   3,
				RetryDelay:   1,
				RetryBackoff: model.RETRY_BACKOFF_EXPONENTIAL,
			},
			Schedule: &model.Schedule{
				Start:    time.Now().Add(10 * time.Minute),
				Interval: 15 * time.Minute,
				MaxCount: 3,
			},
		}
		expectedJob := &model.Job{
			TaskName:   "queuer.TaskMock",
			Parameters: model.Parameters{1.0, "2"},
			Options:    options,
		}

		params := []interface{}{1, "2"}
		tx, err := testQueuer.DB.Begin()
		require.NoError(t, err, "Begin transaction should not return an error")

		job, err := testQueuer.AddJobWithOptionsTx(tx, options, TaskMock, params...)
		assert.NoError(t, err, "AddJobWithOptionsTx should not return an error on success")
		assert.Equal(t, expectedJob.TaskName, job.TaskName, "AddJobWithOptionsTx should return the correct task name")
		assert.EqualValues(t, expectedJob.Parameters, job.Parameters, "AddJobWithOptionsTx should return the correct parameters")
		assert.EqualValues(t, expectedJob.Options.OnError, job.Options.OnError, "AddJobWithOptionsTx should return the correct OnError options")
		assert.EqualExportedValues(t, expectedJob.Options.Schedule, job.Options.Schedule, "AddJobWithOptionsTx should return the correct Schedule options")

		err = tx.Commit()
		assert.NoError(t, err, "Commit transaction should not return an error")
	})

	t.Run("Returns error for nil function in transaction", func(t *testing.T) {
		var nilTask func() // Invalid nil function
		tx, err := testQueuer.DB.Begin()
		require.NoError(t, err, "Begin transaction should not return an error")

		job, err := testQueuer.AddJobWithOptionsTx(tx, nil, nilTask)
		assert.Error(t, err, "AddJobWithOptionsTx should return an error for nil task (via addJobFn)")
		assert.Nil(t, job, "Job should be nil for nil task")
		assert.Contains(t, err.Error(), "task value must not be nil", "Error message should reflect nil task handling")
		err = tx.Rollback()
		assert.NoError(t, err, "Rollback transaction should not return an error")
	})

	t.Run("Returns error for invalid task type in transaction", func(t *testing.T) {
		invalidTask := 123 // Invalid integer type instead of a function
		tx, err := testQueuer.DB.Begin()
		require.NoError(t, err, "Begin transaction should not return an error")

		job, err := testQueuer.AddJobWithOptionsTx(tx, nil, invalidTask, "param1")
		assert.Error(t, err, "AddJobWithOptionsTx should return an error for invalid task type")
		assert.Nil(t, job, "Job should be nil for invalid task type")
		assert.Contains(t, err.Error(), "task must be a function, got int", "Error message should reflect invalid task type handling")

		err = tx.Rollback()
		assert.NoError(t, err, "Rollback transaction should not return an error")
	})
}

func TestAddJobs(t *testing.T) {
	testQueuer := newQueuerMock("TestQueuer", 100)

	t.Run("Successfully adds multiple jobs with nil options", func(t *testing.T) {
		batchJobs := []model.BatchJob{
			{
				Task:       TaskMock,
				Parameters: []interface{}{1, "2"},
				Options:    nil,
			},
			{
				Task:       TaskMock,
				Parameters: []interface{}{3, "4"},
				Options:    nil,
			},
		}

		err := testQueuer.AddJobs(batchJobs)
		assert.NoError(t, err, "AddJobs should not return an error on success")

		jobs, err := testQueuer.GetJobs(0, 10)
		assert.NoError(t, err, "GetJobs should not return an error")
		assert.Len(t, jobs, 2, "AddJobs should return the correct number of jobs")
	})

	t.Run("Returns error for invalid batch job", func(t *testing.T) {
		batchJobs := []model.BatchJob{
			{
				Task:       nil, // Invalid nil function
				Parameters: []interface{}{1, "2"},
				Options:    nil,
			},
		}

		err := testQueuer.AddJobs(batchJobs)
		assert.Error(t, err, "AddJobs should return an error for invalid batch job")
	})
}

func TestWaitForJobFinished(t *testing.T) {
	testQueuer := newQueuerMock("TestQueuer", 100)
	testQueuer.AddTask(TaskMock)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	testQueuer.Start(ctx, cancel)

	t.Run("Successfully waits for a job to finish", func(t *testing.T) {
		job, err := testQueuer.AddJob(TaskMock, 1, "2")
		assert.NoError(t, err, "AddJob should not return an error on success")

		job = testQueuer.WaitForJobFinished(job.RID)
		assert.NotNil(t, job, "WaitForJobFinished should return the finished job")
		assert.Equal(t, model.JobStatusSucceeded, job.Status, "WaitForJobFinished should return job with status Succeeded")

		jobArchived, err := testQueuer.dbJob.SelectJobFromArchive(job.RID)
		assert.NoError(t, err, "SelectJobFromArchive should not return an error for archived job")
		assert.NotNil(t, jobArchived, "SelectJobFromArchive should return the archived job")
		assert.Equal(t, model.JobStatusSucceeded, jobArchived.Status, "Archived job should have status Succeeded")
	})

	t.Run("Successfully cancel context while waiting for job", func(t *testing.T) {
		job, err := testQueuer.AddJob(TaskMock, 1, "2")
		assert.NoError(t, err, "AddJob should not return an error on success")

		go func() {
			time.Sleep(500 * time.Millisecond)
			cancel()
		}()

		job = testQueuer.WaitForJobFinished(job.RID)
		assert.Nil(t, job, "WaitForJobFinished should return nil when context is cancelled")
	})
}

func TestCancelJob(t *testing.T) {
	testQueuer := newQueuerMock("TestQueuer", 100)

	t.Run("Successfully cancels a queued job", func(t *testing.T) {
		job, err := testQueuer.AddJob(TaskMock, 1, "2")
		assert.NoError(t, err, "AddJob should not return an error on success")

		cancelledJob, err := testQueuer.CancelJob(job.RID)
		assert.NoError(t, err, "CancelJob should not return an error on success")

		assert.NoError(t, err, "GetJobs should not return an error")
		assert.Equal(t, job.RID, cancelledJob.RID, "CancelJob should return the correct job RID")

		jobs, err := testQueuer.GetJobs(0, 10)
		assert.NoError(t, err, "GetJobs should not return an error")
		assert.NotContains(t, jobs, cancelledJob, "Cancelled job should not be in the job list")
	})

	t.Run("Returns error for non-existent job", func(t *testing.T) {
		cancelledJob, err := testQueuer.CancelJob(uuid.New())
		assert.Error(t, err, "CancelJob should return an error for non-existent job")
		assert.Nil(t, cancelledJob, "Cancelled job should be nil for non-existent job")
	})
}

func TestCancelJobRunning(t *testing.T) {
	// Only works with a running queuer because the worker needs to process jobs
	// to be able to cancel them.
	testQueuer := newQueuerMock("TestQueuer", 100)
	testQueuer.AddTask(TaskMock)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	testQueuer.Start(ctx, cancel)

	t.Run("Successfully cancels a running job", func(t *testing.T) {
		job, err := testQueuer.AddJob(TaskMock, 3, "2")
		assert.NoError(t, err, "AddJob should not return an error on success")

		queuedJob, err := testQueuer.GetJob(job.RID)
		assert.NoError(t, err, "GetJob should not return an error")
		assert.NotNil(t, queuedJob, "GetJob should return the job that is currently running")

		time.Sleep(1 * time.Second)

		cancelledJob, err := testQueuer.CancelJob(job.RID)
		assert.NoError(t, err, "CancelJob should not return an error on success")
		assert.Equal(t, job.RID, cancelledJob.RID, "CancelJob should return the correct job RID")

		jobNotExisting, err := testQueuer.GetJob(job.RID)
		assert.Error(t, err, "GetJob should return an error for cancelled job")
		assert.Nil(t, jobNotExisting, "GetJob should return nil for cancelled job")

		jobArchived, err := testQueuer.dbJob.SelectJobFromArchive(job.RID)
		assert.NoError(t, err, "SelectJobFromArchive should not return an error for archived job")
		assert.NotNil(t, jobArchived, "SelectJobFromArchive should return the archived job")
		assert.Equal(t, jobArchived.Status, model.JobStatusCancelled, "Archived job should have status Cancelled")
	})
}

func TestCancelAllJobsByWorkerRunning(t *testing.T) {
	// Only works with a running queuer because the worker needs to process jobs
	// to be able to cancel them.
	testQueuer := newQueuerMock("TestQueuer", 100)
	testQueuer.AddTask(TaskMock)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	testQueuer.Start(ctx, cancel)

	t.Run("Successfully cancels all jobs by worker RID", func(t *testing.T) {
		job1, err := testQueuer.AddJob(TaskMock, 10, "2")
		require.NoError(t, err, "AddJob should not return an error on success")

		job2, err := testQueuer.AddJob(TaskMock, 10, "4")
		require.NoError(t, err, "AddJob should not return an error on success")

		time.Sleep(1 * time.Second)

		jobs, err := testQueuer.GetJobsByWorkerRID(testQueuer.worker.RID, 0, 10)
		assert.NoError(t, err, "SelectAllJobsByWorkerRID should not return an error")
		require.Len(t, jobs, 2, "There should be two jobs for the worker")
		assert.Equal(t, model.JobStatusRunning, jobs[0].Status, "Job1 should be in Running status")
		assert.Equal(t, model.JobStatusRunning, jobs[1].Status, "Job2 should be in Running status")

		err = testQueuer.CancelAllJobsByWorker(testQueuer.worker.RID, 10)
		assert.NoError(t, err, "CancelAllJobsByWorker should not return an error on success")

		jobs, err = testQueuer.GetJobs(0, 10)
		assert.NoError(t, err, "GetJobs should not return an error")
		assert.NotContains(t, jobs, job1, "Cancelled job1 should not be in the job list")
		assert.NotContains(t, jobs, job2, "Cancelled job2 should not be in the job list")

		jobArchived1, err := testQueuer.dbJob.SelectJobFromArchive(job1.RID)
		assert.NoError(t, err, "SelectJobFromArchive should not return an error for archived job1")
		require.NotNil(t, jobArchived1, "SelectJobFromArchive should return the archived job1")
		assert.Equal(t, jobArchived1.Status, model.JobStatusCancelled, "Archived job1 should have status Cancelled")

		jobArchived2, err := testQueuer.dbJob.SelectJobFromArchive(job2.RID)
		assert.NoError(t, err, "SelectJobFromArchive should not return an error for archived job2")
		require.NotNil(t, jobArchived2, "SelectJobFromArchive should return the archived job2")
		assert.Equal(t, jobArchived2.Status, model.JobStatusCancelled, "Archived job2 should have status Cancelled")
	})
}

func TestReaddJobFromArchive(t *testing.T) {
	testQueuer := newQueuerMock("TestQueuer", 100)

	t.Run("Successfully readds a job from archive", func(t *testing.T) {
		job, err := testQueuer.AddJob(TaskMock, 1, "2")
		assert.NoError(t, err, "AddJob should not return an error on success")

		job, err = testQueuer.GetJob(job.RID)
		assert.NoError(t, err, "GetJob should not return an error")
		assert.NotNil(t, job, "GetJob should return the job that is currently queued")

		// Cancel the job to archive it
		cancelledJob, err := testQueuer.CancelJob(job.RID)
		assert.NoError(t, err, "CancelJob should not return an error on success")
		assert.Equal(t, job.RID, cancelledJob.RID, "CancelJob should return the correct job RID")

		jobArchived, err := testQueuer.dbJob.SelectJobFromArchive(cancelledJob.RID)
		assert.NoError(t, err, "SelectJobFromArchive should not return an error for archived job")
		assert.NotNil(t, jobArchived, "SelectJobFromArchive should return the archived job")
		assert.Equal(t, jobArchived.Status, model.JobStatusCancelled, "Archived job should have status Cancelled")

		readdedJob, err := testQueuer.ReaddJobFromArchive(job.RID)
		assert.NoError(t, err, "ReaddJobFromArchive should not return an error on success")
		assert.NotNil(t, readdedJob, "ReaddJobFromArchive should return the readded job")

		// Readded job should have a new RID and status
		job, err = testQueuer.GetJob(readdedJob.RID)
		assert.NoError(t, err, "GetJob should not return an error")
		require.NotNil(t, job, "GetJob should return the readded job")
		assert.Equal(t, model.JobStatusQueued, job.Status, "Readded job should have status Queued")
		assert.NotEqual(t, cancelledJob.RID, job.RID, "Readded job should have a new RID")
	})
}
