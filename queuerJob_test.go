package queuer

import (
	"queuer/model"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

// Short running example task function
func TaskMock(param1 int, param2 string) (int, error) {
	// Simulate some work
	time.Sleep(1 * time.Second)

	// Example for some error handling
	param2Int, err := strconv.Atoi(param2)
	if err != nil {
		return 0, err
	}

	return param1 + param2Int, nil
}

// TestAddJob is a comprehensive test function for the AddJob method.
func TestAddJob(t *testing.T) {
	testQueuer := newQueuerMock("TestQueuer", 1)

	t.Run("Successfully adds a job with nil options", func(t *testing.T) {
		expectedJob := &model.Job{
			TaskName:   "queuer.TaskMock",
			Parameters: model.Parameters{1.0, "2"},
			Options:    nil,
		}

		params := []interface{}{1, "2"}
		job, err := testQueuer.AddJob(TaskMock, params...)

		assert.NoError(t, err, "AddJob should not return an error on success")
		assert.Equal(t, expectedJob.TaskName, job.TaskName, "AddJob should return the correct task name")
		assert.EqualValues(t, expectedJob.Parameters, job.Parameters, "AddJob should return the correct parameters")
		assert.Equal(t, expectedJob.Options, job.Options, "AddJob should return the correct options")
	})

	t.Run("Returns error for nil function", func(t *testing.T) {
		var nilTask func() // A nil function is a common nil interface{} variant
		job, err := testQueuer.AddJob(nilTask, "param1")

		assert.Error(t, err, "AddJob should return an error for nil task (via addJobFn)")
		assert.Nil(t, job, "Job should be nil for nil task")
		assert.Contains(t, err.Error(), "task value must not be nil", "Error message should reflect nil task handling")
	})

	t.Run("Returns error for invalid task type", func(t *testing.T) {
		invalidTask := 123 // An integer is not a valid task type
		job, err := testQueuer.AddJob(invalidTask, "param1")

		assert.Error(t, err, "AddJob should return an error for invalid task type")
		assert.Nil(t, job, "Job should be nil for invalid task type")
		assert.Contains(t, err.Error(), "task must be a function, got int", "Error message should reflect invalid task type handling")
	})
}

func TestAddJobWithOptions(t *testing.T) {
	testQueuer := newQueuerMock("TestQueuer", 1)

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
				Timeout:      -5,
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

func TestAddJobs(t *testing.T) {
	testQueuer := newQueuerMock("TestQueuer", 1)

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
				Task:       nil, // Invalid task
				Parameters: []interface{}{1, "2"},
				Options:    nil,
			},
		}

		err := testQueuer.AddJobs(batchJobs)
		assert.Error(t, err, "AddJobs should return an error for invalid batch job")
	})
}

func TestCancelJob(t *testing.T) {
	testQueuer := newQueuerMock("TestQueuer", 1)

	t.Run("Successfully cancels a job", func(t *testing.T) {
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
