package database

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/siherrmann/queuer/helper"
	"github.com/siherrmann/queuer/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJobNewJobDBHandler(t *testing.T) {
	helper.SetTestDatabaseConfigEnvs(t, dbPort)
	dbConfig, err := helper.NewDatabaseConfiguration()
	if err != nil {
		t.Fatalf("failed to create database configuration: %v", err)
	}

	t.Run("Valid call NewJobDBHandler", func(t *testing.T) {
		database := helper.NewTestDatabase(dbConfig)

		jobDbHandler, err := NewJobDBHandler(database, true)
		assert.NoError(t, err, "Expected NewJobDBHandler to not return an error")
		require.NotNil(t, jobDbHandler, "Expected NewJobDBHandler to return a non-nil instance")
		require.NotNil(t, jobDbHandler.db, "Expected NewJobDBHandler to have a non-nil database instance")
		require.NotNil(t, jobDbHandler.db.Instance, "Expected NewJobDBHandler to have a non-nil database connection instance")

		exists, err := jobDbHandler.CheckTablesExistance()
		assert.NoError(t, err)
		assert.True(t, exists)

		err = jobDbHandler.DropTables()
		assert.NoError(t, err)
	})

	t.Run("Invalid call NewJobDBHandler with nil database", func(t *testing.T) {
		_, err := NewJobDBHandler(nil, true)
		assert.Error(t, err, "Expected error when creating JobDBHandler with nil database")
		assert.Contains(t, err.Error(), "database connection is nil", "Expected specific error message for nil database connection")
	})
}

func TestJobCheckTableExistance(t *testing.T) {
	helper.SetTestDatabaseConfigEnvs(t, dbPort)
	dbConfig, err := helper.NewDatabaseConfiguration()
	if err != nil {
		t.Fatalf("failed to create database configuration: %v", err)
	}
	database := helper.NewTestDatabase(dbConfig)

	jobDbHandler, err := NewJobDBHandler(database, true)
	require.NoError(t, err, "Expected NewJobDBHandler to not return an error")

	exists, err := jobDbHandler.CheckTablesExistance()
	assert.NoError(t, err, "Expected CheckTableExistance to not return an error")
	assert.True(t, exists, "Expected job table to exist")
}

func TestJobCreateTable(t *testing.T) {
	helper.SetTestDatabaseConfigEnvs(t, dbPort)
	dbConfig, err := helper.NewDatabaseConfiguration()
	if err != nil {
		t.Fatalf("failed to create database configuration: %v", err)
	}
	database := helper.NewTestDatabase(dbConfig)

	jobDbHandler, err := NewJobDBHandler(database, true)
	require.NoError(t, err, "Expected NewJobDBHandler to not return an error")

	err = jobDbHandler.CreateTable()
	assert.NoError(t, err, "Expected CreateTable to not return an error")
}

func TestJobDropTable(t *testing.T) {
	helper.SetTestDatabaseConfigEnvs(t, dbPort)
	dbConfig, err := helper.NewDatabaseConfiguration()
	if err != nil {
		t.Fatalf("failed to create database configuration: %v", err)
	}
	database := helper.NewTestDatabase(dbConfig)

	jobDbHandler, err := NewJobDBHandler(database, true)
	require.NoError(t, err, "Expected NewJobDBHandler to not return an error")

	err = jobDbHandler.DropTables()
	assert.NoError(t, err, "Expected DropTable to not return an error")
}

func TestJobInsertJob(t *testing.T) {
	helper.SetTestDatabaseConfigEnvs(t, dbPort)
	dbConfig, err := helper.NewDatabaseConfiguration()
	if err != nil {
		t.Fatalf("failed to create database configuration: %v", err)
	}
	database := helper.NewTestDatabase(dbConfig)

	jobDbHandler, err := NewJobDBHandler(database, true)
	require.NoError(t, err, "Expected NewJobDBHandler to not return an error")

	job, err := model.NewJob("TestTask", nil)
	require.NoError(t, err, "Expected NewJob to not return an error")

	insertedJob, err := jobDbHandler.InsertJob(job)
	assert.NoError(t, err, "Expected InsertJob to not return an error")
	assert.NotNil(t, insertedJob, "Expected InsertJob to return a non-nil job")
	assert.Equal(t, job.TaskName, insertedJob.TaskName, "Expected task name to match")
	assert.Equal(t, model.JobStatusQueued, insertedJob.Status, "Expected job status to be QUEUED")
	assert.Equal(t, 0, insertedJob.Attempts, "Expected job attempts to be 0")
	assert.WithinDuration(t, insertedJob.CreatedAt, time.Now(), 1*time.Second, "Expected inserted worker CreatedAt time to match")
	assert.WithinDuration(t, insertedJob.UpdatedAt, time.Now(), 1*time.Second, "Expected inserted worker UpdatedAt time to match")
}

func TestJobInsertJobTx(t *testing.T) {
	helper.SetTestDatabaseConfigEnvs(t, dbPort)
	dbConfig, err := helper.NewDatabaseConfiguration()
	if err != nil {
		t.Fatalf("failed to create database configuration: %v", err)
	}
	database := helper.NewTestDatabase(dbConfig)

	jobDbHandler, err := NewJobDBHandler(database, true)
	require.NoError(t, err, "Expected NewJobDBHandler to not return an error")

	job, err := model.NewJob("TestTask", nil)
	require.NoError(t, err, "Expected NewJob to not return an error")

	tx, err := database.Instance.Begin()
	require.NoError(t, err, "Expected Begin to not return an error")

	insertedJob, err := jobDbHandler.InsertJobTx(tx, job)
	assert.NoError(t, err, "Expected InsertJobTx to not return an error")
	assert.NotNil(t, insertedJob, "Expected InsertJobTx to return a non-nil job")
	assert.Equal(t, job.TaskName, insertedJob.TaskName, "Expected task name to match")
	assert.Equal(t, model.JobStatusQueued, insertedJob.Status, "Expected job status to be QUEUED")
	assert.Equal(t, 0, insertedJob.Attempts, "Expected job attempts to be 0")
	assert.WithinDuration(t, insertedJob.CreatedAt, time.Now(), 1*time.Second, "Expected inserted worker CreatedAt time to match")
	assert.WithinDuration(t, insertedJob.UpdatedAt, time.Now(), 1*time.Second, "Expected inserted worker UpdatedAt time to match")

	err = tx.Commit()
	assert.NoError(t, err, "Expected Commit to not return an error")
}

func TestJobBatchInsertJobs(t *testing.T) {
	helper.SetTestDatabaseConfigEnvs(t, dbPort)
	dbConfig, err := helper.NewDatabaseConfiguration()
	if err != nil {
		t.Fatalf("failed to create database configuration: %v", err)
	}
	database := helper.NewTestDatabase(dbConfig)

	jobDbHandler, err := NewJobDBHandler(database, true)
	require.NoError(t, err, "Expected NewJobDBHandler to not return an error")

	t.Run("Successful batch insert jobs", func(t *testing.T) {
		jobCount := 5
		jobs := []*model.Job{}
		for i := 0; i < jobCount; i++ {
			job, err := model.NewJob("TestTask", nil)
			require.NoError(t, err, "Expected NewJob to not return an error")
			require.NotNil(t, job, "Expected NewJob to return a non-nil job")
			jobs = append(jobs, job)
		}

		err = jobDbHandler.BatchInsertJobs(jobs)
		assert.NoError(t, err, "Expected BatchInsertJobs to not return an error")
	})

	t.Run("Successful batch insert jobs with error options", func(t *testing.T) {
		jobCount := 5
		jobs := []*model.Job{}
		for i := 0; i < jobCount; i++ {
			job, err := model.NewJob("TestTask", &model.Options{
				OnError: &model.OnError{
					Timeout:      0.1,
					RetryDelay:   1,
					RetryBackoff: model.RETRY_BACKOFF_EXPONENTIAL,
					MaxRetries:   3,
				},
			})
			require.NoError(t, err, "Expected NewJob to not return an error")
			require.NotNil(t, job, "Expected NewJob to return a non-nil job")
			jobs = append(jobs, job)
		}

		err = jobDbHandler.BatchInsertJobs(jobs)
		assert.NoError(t, err, "Expected BatchInsertJobs to not return an error")
	})

	t.Run("Successful batch insert jobs with schedule options", func(t *testing.T) {
		jobCount := 5
		jobs := []*model.Job{}
		for i := 0; i < jobCount; i++ {
			job, err := model.NewJob("TestTask", &model.Options{
				Schedule: &model.Schedule{
					Start:    time.Now().Add(time.Second * 10),
					Interval: time.Second * 5,
					MaxCount: 10,
				},
			})
			require.NoError(t, err, "Expected NewJob to not return an error")
			require.NotNil(t, job, "Expected NewJob to return a non-nil job")
			jobs = append(jobs, job)
		}

		err = jobDbHandler.BatchInsertJobs(jobs)
		assert.NoError(t, err, "Expected BatchInsertJobs to not return an error")
	})

	t.Run("Successful batch insert jobs with parameters", func(t *testing.T) {
		jobCount := 5
		jobs := []*model.Job{}
		for i := 0; i < jobCount; i++ {
			job, err := model.NewJob("TestTask", nil)
			job.Parameters = []interface{}{i, fmt.Sprintf("param-%d", i)}
			require.NoError(t, err, "Expected NewJob to not return an error")
			require.NotNil(t, job, "Expected NewJob to return a non-nil job")
			jobs = append(jobs, job)
		}

		err = jobDbHandler.BatchInsertJobs(jobs)
		assert.NoError(t, err, "Expected BatchInsertJobs to not return an error")
	})
}

func TestJobUpdateJobsInitial(t *testing.T) {
	helper.SetTestDatabaseConfigEnvs(t, dbPort)
	dbConfig, err := helper.NewDatabaseConfiguration()
	if err != nil {
		t.Fatalf("failed to create database configuration: %v", err)
	}
	database := helper.NewTestDatabase(dbConfig)

	// Prerequisite: Insert a worker for the job to be associated with
	workerDBHandler, err := NewWorkerDBHandler(database, true)
	require.NoError(t, err, "Expected NewWorkerDBHandler to not return an error")

	worker, err := model.NewWorker("TestWorker", 1)
	require.NoError(t, err, "Expected NewWorker to not return an error")

	insertedWorker, err := workerDBHandler.InsertWorker(worker)
	require.NoError(t, err, "Expected InsertWorker to not return an error")

	insertedWorker.AvailableTasks = []string{"TestTask"}

	updatedWorker, err := workerDBHandler.UpdateWorker(insertedWorker)
	require.NoError(t, err, "Expected UpdateWorker to not return an error")

	// Now we can proceed with the job insertion and update
	jobDbHandler, err := NewJobDBHandler(database, true)
	require.NoError(t, err, "Expected NewJobDBHandler to not return an error")

	job, err := model.NewJob("TestTask", nil)
	require.NoError(t, err, "Expected NewJob to not return an error")

	insertedJob, err := jobDbHandler.InsertJob(job)
	require.NoError(t, err, "Expected InsertJob to not return an error")

	updatedJobs, err := jobDbHandler.UpdateJobsInitial(updatedWorker)
	assert.NoError(t, err, "Expected UpdateJobsInitial to not return an error")
	require.Len(t, updatedJobs, 1, "Expected one job to be updated")
	assert.Equal(t, insertedJob.ID, updatedJobs[0].ID, "Expected updated job ID to match inserted job ID")
	assert.Equal(t, updatedWorker.ID, updatedJobs[0].WorkerID, "Expected updated job WorkerID to match new worker ID")
	assert.Equal(t, updatedJobs[0].WorkerRID, updatedWorker.RID, "Expected updated job WorkerRID to match new worker RID")
	assert.Equal(t, model.JobStatusRunning, updatedJobs[0].Status, "Expected job status to be RUNNING")
	assert.WithinDuration(t, updatedJobs[0].UpdatedAt, time.Now(), 1*time.Second, "Expected inserted worker UpdatedAt time to match")
}

func TestJobUpdateJobFinal(t *testing.T) {
	helper.SetTestDatabaseConfigEnvs(t, dbPort)
	dbConfig, err := helper.NewDatabaseConfiguration()
	if err != nil {
		t.Fatalf("failed to create database configuration: %v", err)
	}
	database := helper.NewTestDatabase(dbConfig)

	jobDbHandler, err := NewJobDBHandler(database, true)
	require.NoError(t, err, "Expected NewJobDBHandler to not return an error")

	job, err := model.NewJob("TestTask", nil)
	require.NoError(t, err, "Expected NewJob to not return an error")

	insertedJob, err := jobDbHandler.InsertJob(job)
	require.NoError(t, err, "Expected InsertJob to not return an error")

	// Update the job status to SUCCEEDED
	insertedJob.Status = model.JobStatusSucceeded
	updatedJob, err := jobDbHandler.UpdateJobFinal(insertedJob)
	assert.NoError(t, err, "Expected UpdateJobFinal to not return an error")
	assert.Equal(t, insertedJob.ID, updatedJob.ID, "Expected updated job ID to match inserted job ID")
	assert.Equal(t, model.JobStatusSucceeded, updatedJob.Status, "Expected job status to be SUCCEEDED")
	assert.WithinDuration(t, updatedJob.UpdatedAt, time.Now(), 1*time.Second, "Expected inserted worker UpdatedAt time to match")
}

func TestJobUpdateJobFinalEncrypted(t *testing.T) {
	helper.SetTestDatabaseConfigEnvs(t, dbPort)
	dbConfig, err := helper.NewDatabaseConfiguration()
	if err != nil {
		t.Fatalf("failed to create database configuration: %v", err)
	}
	database := helper.NewTestDatabase(dbConfig)

	jobDbHandler, err := NewJobDBHandler(database, true, "test-encryption-key")
	require.NoError(t, err, "Expected NewJobDBHandler to not return an error")

	job, err := model.NewJob("TestTask", nil)
	require.NoError(t, err, "Expected NewJob to not return an error")

	insertedJob, err := jobDbHandler.InsertJob(job)
	require.NoError(t, err, "Expected InsertJob to not return an error")

	// Update the job status to SUCCEEDED
	insertedJob.Status = model.JobStatusSucceeded
	updatedJob, err := jobDbHandler.UpdateJobFinal(insertedJob)
	assert.NoError(t, err, "Expected UpdateJobFinal to not return an error")
	assert.Equal(t, insertedJob.ID, updatedJob.ID, "Expected updated job ID to match inserted job ID")
	assert.Equal(t, model.JobStatusSucceeded, updatedJob.Status, "Expected job status to be SUCCEEDED")
	assert.WithinDuration(t, updatedJob.UpdatedAt, time.Now(), 1*time.Second, "Expected inserted worker UpdatedAt time to match")

	// Verify that the job was archived (should no longer exist in main job table)
	_, err = jobDbHandler.SelectJob(insertedJob.RID)
	assert.Error(t, err, "Expected SelectJob to return an error since job should be archived")

	// Verify that the job exists in archive and can be decrypted correctly
	archivedJob, err := jobDbHandler.SelectJobFromArchive(updatedJob.RID)
	assert.NoError(t, err, "Expected SelectJobFromArchive to not return an error")
	assert.NotNil(t, archivedJob, "Expected SelectJobFromArchive to return a non-nil job")
	assert.Equal(t, insertedJob.RID, archivedJob.RID, "Expected archived job RID to match inserted job RID")
	assert.Equal(t, model.JobStatusSucceeded, archivedJob.Status, "Expected archived job status to be SUCCEEDED")
	assert.Equal(t, insertedJob.Results, archivedJob.Results, "Expected archived job results to match original results after decryption")
}

func TestUpdateStaleJobs(t *testing.T) {
	helper.SetTestDatabaseConfigEnvs(t, dbPort)
	dbConfig, err := helper.NewDatabaseConfiguration()
	if err != nil {
		t.Fatalf("failed to create database configuration: %v", err)
	}
	database := helper.NewTestDatabase(dbConfig)

	jobDbHandler, err := NewJobDBHandler(database, true)
	assert.NoError(t, err, "Expected NewJobDBHandler to not return an error")

	workerDbHandler, err := NewWorkerDBHandler(database, true)
	assert.NoError(t, err, "Expected NewWorkerDBHandler to not return an error")

	t.Run("Cancel jobs with stopped workers", func(t *testing.T) {
		// Create workers and set one to STOPPED
		worker1, err := model.NewWorker("worker-1", 3)
		require.NoError(t, err)
		insertedWorker1, err := workerDbHandler.InsertWorker(worker1)
		require.NoError(t, err)

		worker2, err := model.NewWorker("worker-2", 3)
		require.NoError(t, err)
		insertedWorker2, err := workerDbHandler.InsertWorker(worker2)
		require.NoError(t, err)

		insertedWorker2.Status = model.WorkerStatusStopped
		_, err = workerDbHandler.UpdateWorker(insertedWorker2)
		require.NoError(t, err)

		// Create jobs: QUEUED (stopped worker), SUCCEEDED (stopped worker), QUEUED (ready worker)
		testCases := []struct {
			status       string
			workerRID    string
			shouldCancel bool
		}{
			{model.JobStatusQueued, "stopped", true},     // Should be cancelled
			{model.JobStatusSucceeded, "stopped", false}, // Should not be cancelled (final status)
			{model.JobStatusQueued, "ready", false},      // Should not be cancelled (ready worker)
		}

		jobs := make([]*model.Job, len(testCases))
		for i, tc := range testCases {
			job, err := model.NewJob("test-task", nil)
			require.NoError(t, err)

			if tc.workerRID == "stopped" {
				job.WorkerRID = insertedWorker2.RID
			} else {
				job.WorkerRID = insertedWorker1.RID
			}

			insertedJob, err := jobDbHandler.InsertJob(job)
			require.NoError(t, err)

			// Set job status and worker assignment
			_, err = database.Instance.Exec(
				"UPDATE job SET status = $1, worker_rid = $2 WHERE rid = $3",
				tc.status, job.WorkerRID, insertedJob.RID,
			)
			require.NoError(t, err)
			jobs[i] = insertedJob
		}

		// Test UpdateStaleJobs
		updatedCount, err := jobDbHandler.UpdateStaleJobs()
		assert.NoError(t, err)
		assert.Equal(t, 1, updatedCount, "Expected 1 job to be cancelled")

		// Verify results
		for i, tc := range testCases {
			updatedJob, err := jobDbHandler.SelectJob(jobs[i].RID)
			require.NoError(t, err)
			if tc.shouldCancel {
				assert.Equal(t, model.JobStatusCancelled, updatedJob.Status)
			} else {
				assert.Equal(t, tc.status, updatedJob.Status)
			}
		}

		// Clean up
		for _, job := range jobs {
			jobDbHandler.DeleteJob(job.RID)
		}
		workerDbHandler.DeleteWorker(insertedWorker1.RID)
		workerDbHandler.DeleteWorker(insertedWorker2.RID)
	})

	t.Run("No updates when no stopped workers", func(t *testing.T) {
		worker, err := model.NewWorker("ready-worker", 3)
		require.NoError(t, err)
		insertedWorker, err := workerDbHandler.InsertWorker(worker)
		require.NoError(t, err)

		job, err := model.NewJob("test-task", nil)
		require.NoError(t, err)
		job.WorkerRID = insertedWorker.RID
		insertedJob, err := jobDbHandler.InsertJob(job)
		require.NoError(t, err)

		updatedCount, err := jobDbHandler.UpdateStaleJobs()
		assert.NoError(t, err)
		assert.Equal(t, 0, updatedCount)

		// Clean up
		jobDbHandler.DeleteJob(insertedJob.RID)
		workerDbHandler.DeleteWorker(insertedWorker.RID)
	})
}

func TestJobDeleteJob(t *testing.T) {
	helper.SetTestDatabaseConfigEnvs(t, dbPort)
	dbConfig, err := helper.NewDatabaseConfiguration()
	if err != nil {
		t.Fatalf("failed to create database configuration: %v", err)
	}
	database := helper.NewTestDatabase(dbConfig)

	jobDbHandler, err := NewJobDBHandler(database, true)
	require.NoError(t, err, "Expected NewJobDBHandler to not return an error")

	job, err := model.NewJob("TestTask", nil)
	require.NoError(t, err, "Expected NewJob to not return an error")

	insertedJob, err := jobDbHandler.InsertJob(job)
	require.NoError(t, err, "Expected InsertJob to not return an error")

	err = jobDbHandler.DeleteJob(insertedJob.RID)
	assert.NoError(t, err, "Expected DeleteJob to not return an error")

	// Verify that the job no longer exists
	deletedJob, err := jobDbHandler.SelectJob(insertedJob.RID)
	require.Error(t, err, "Expected SelectJob to return an error")
	assert.Contains(t, err.Error(), sql.ErrNoRows.Error(), "Expected error to contain sql.ErrNoRows for deleted job")
	assert.Nil(t, deletedJob, "Expected deleted job to be nil")
}

func TestJobSelectJob(t *testing.T) {
	helper.SetTestDatabaseConfigEnvs(t, dbPort)
	dbConfig, err := helper.NewDatabaseConfiguration()
	if err != nil {
		t.Fatalf("failed to create database configuration: %v", err)
	}
	database := helper.NewTestDatabase(dbConfig)

	jobDbHandler, err := NewJobDBHandler(database, true)
	require.NoError(t, err, "Expected NewJobDBHandler to not return an error")

	job, err := model.NewJob("TestTask", nil)
	require.NoError(t, err, "Expected NewJob to not return an error")

	insertedJob, err := jobDbHandler.InsertJob(job)
	require.NoError(t, err, "Expected InsertJob to not return an error")

	selectedJob, err := jobDbHandler.SelectJob(insertedJob.RID)
	assert.NoError(t, err, "Expected SelectJob to not return an error")
	assert.NotNil(t, selectedJob, "Expected SelectJob to return a non-nil job")
	assert.Equal(t, insertedJob.RID, selectedJob.RID, "Expected selected job RID to match inserted job RID")
}

func TestJobSelectJobEncrypted(t *testing.T) {
	helper.SetTestDatabaseConfigEnvs(t, dbPort)
	dbConfig, err := helper.NewDatabaseConfiguration()
	if err != nil {
		t.Fatalf("failed to create database configuration: %v", err)
	}
	database := helper.NewTestDatabase(dbConfig)

	jobDbHandler, err := NewJobDBHandler(database, true, "test-encryption-key")
	require.NoError(t, err, "Expected NewJobDBHandler to not return an error")

	job, err := model.NewJob("TestTask", nil)
	require.NoError(t, err, "Expected NewJob to not return an error")

	insertedJob, err := jobDbHandler.InsertJob(job)
	require.NoError(t, err, "Expected InsertJob to not return an error")

	// Simulate job completion with results (this is when results get encrypted)
	testResults := model.Parameters{"data", float64(1)}
	insertedJob.Status = model.JobStatusSucceeded
	insertedJob.Results = testResults

	updatedJob, err := jobDbHandler.UpdateJobFinal(insertedJob)
	require.NoError(t, err, "Expected UpdateJobFinal to not return an error")

	// Job should be archived after UpdateJobFinal, so check archive
	archivedJob, err := jobDbHandler.SelectJobFromArchive(updatedJob.RID)
	assert.NoError(t, err, "Expected SelectJobFromArchive to not return an error")
	assert.NotNil(t, archivedJob, "Expected SelectJobFromArchive to return a non-nil job")
	assert.Equal(t, insertedJob.RID, archivedJob.RID, "Expected archived job RID to match inserted job RID")
	assert.Equal(t, insertedJob.TaskName, archivedJob.TaskName, "Expected archived job TaskName to match inserted job TaskName")
	assert.Equal(t, model.JobStatusSucceeded, archivedJob.Status, "Expected archived job Status to be SUCCEEDED")
	assert.Equal(t, testResults, archivedJob.Results, "Expected archived job Results to match test results after decryption")
}

func TestJobSelectAllJobs(t *testing.T) {
	helper.SetTestDatabaseConfigEnvs(t, dbPort)
	dbConfig, err := helper.NewDatabaseConfiguration()
	if err != nil {
		t.Fatalf("failed to create database configuration: %v", err)
	}
	database := helper.NewTestDatabase(dbConfig)

	newJobCount := 5
	jobDbHandler, err := NewJobDBHandler(database, true)
	require.NoError(t, err, "Expected NewJobDBHandler to not return an error")

	for i := 0; i < newJobCount; i++ {
		job, err := model.NewJob(fmt.Sprintf("TestJob%v", i), nil)
		require.NoError(t, err, "Expected NewJob to not return an error")

		_, err = jobDbHandler.InsertJob(job)
		require.NoError(t, err, "Expected InsertJob to not return an error")
	}

	jobs, err := jobDbHandler.SelectAllJobs(0, 10)
	assert.NoError(t, err, "Expected SelectAllJobs to not return an error")
	assert.Len(t, jobs, newJobCount, "Expected SelectAllJobs to return two jobs")

	pageLength := 3
	paginatedJobs, err := jobDbHandler.SelectAllJobs(0, pageLength)
	assert.NoError(t, err, "Expected SelectAllJobs to not return an error")
	assert.Len(t, paginatedJobs, pageLength, "Expected SelectAllJobs to return two jobs")
}

func TestJobSelectAllJobsEncrypted(t *testing.T) {
	helper.SetTestDatabaseConfigEnvs(t, dbPort)
	dbConfig, err := helper.NewDatabaseConfiguration()
	if err != nil {
		t.Fatalf("failed to create database configuration: %v", err)
	}
	database := helper.NewTestDatabase(dbConfig)

	newJobCount := 5
	jobDbHandler, err := NewJobDBHandler(database, true, "test-encryption-key")
	require.NoError(t, err, "Expected NewJobDBHandler to not return an error")

	expectedResultsByTaskName := make(map[string]model.Parameters)
	for i := 0; i < newJobCount; i++ {
		taskName := fmt.Sprintf("TestJob%v", i)
		job, err := model.NewJob(taskName, nil)
		require.NoError(t, err, "Expected NewJob to not return an error")

		insertedJob, err := jobDbHandler.InsertJob(job)
		require.NoError(t, err, "Expected InsertJob to not return an error")

		testResults := model.Parameters{"data", float64(i)}
		expectedResultsByTaskName[taskName] = testResults
		insertedJob.Status = model.JobStatusSucceeded
		insertedJob.Results = testResults

		_, err = jobDbHandler.UpdateJobFinal(insertedJob)
		require.NoError(t, err, "Expected UpdateJobFinal to not return an error")
	}

	// Jobs are archived after UpdateJobFinal, so test archive retrieval
	archivedJobs, err := jobDbHandler.SelectAllJobsFromArchive(0, 10)
	assert.NoError(t, err, "Expected SelectAllJobsFromArchive to not return an error")
	assert.Len(t, archivedJobs, newJobCount, "Expected SelectAllJobsFromArchive to return all archived jobs")

	pageLength := 3
	paginatedArchivedJobs, err := jobDbHandler.SelectAllJobsFromArchive(0, pageLength)
	assert.NoError(t, err, "Expected SelectAllJobsFromArchive to not return an error")
	assert.Len(t, paginatedArchivedJobs, pageLength, "Expected SelectAllJobsFromArchive to return paginated archived jobs")

	for _, job := range archivedJobs {
		assert.NotEmpty(t, job.TaskName, "Expected job TaskName to not be empty")
		assert.Equal(t, model.JobStatusSucceeded, job.Status, "Expected job status to be SUCCEEDED")

		expectedResults, exists := expectedResultsByTaskName[job.TaskName]
		assert.True(t, exists, "Expected to find results for task name %s", job.TaskName)
		assert.Equal(t, expectedResults, job.Results, "Expected job Results to match test results after decryption for task %s", job.TaskName)
	}
}

func TestJobSelectAllJobsByWorkerRID(t *testing.T) {
	helper.SetTestDatabaseConfigEnvs(t, dbPort)
	dbConfig, err := helper.NewDatabaseConfiguration()
	if err != nil {
		t.Fatalf("failed to create database configuration: %v", err)
	}
	database := helper.NewTestDatabase(dbConfig)

	workerConcurrency := 3
	newJobCount := 5

	// Prerequisite: Insert a worker for the job to be associated with
	workerDBHandler, err := NewWorkerDBHandler(database, true)
	require.NoError(t, err, "Expected NewWorkerDBHandler to not return an error")

	worker, err := model.NewWorker("TestWorker", workerConcurrency)
	require.NoError(t, err, "Expected NewWorker to not return an error")

	insertedWorker, err := workerDBHandler.InsertWorker(worker)
	require.NoError(t, err, "Expected InsertWorker to not return an error")

	insertedWorker.AvailableTasks = []string{"TestTask"}

	updatedWorker, err := workerDBHandler.UpdateWorker(insertedWorker)
	require.NoError(t, err, "Expected UpdateWorker to not return an error")

	// Insert jobs associated with the worker
	jobDbHandler, err := NewJobDBHandler(database, true)
	require.NoError(t, err, "Expected NewJobDBHandler to not return an error")

	for i := 0; i < newJobCount; i++ {
		job, err := model.NewJob("TestTask", nil)
		require.NoError(t, err, "Expected NewJob to not return an error")

		_, err = jobDbHandler.InsertJob(job)
		require.NoError(t, err, "Expected InsertJob to not return an error")
	}

	updatedJobs, err := jobDbHandler.UpdateJobsInitial(updatedWorker)
	require.NoError(t, err, "Expected UpdateJobsInitial to not return an error")
	require.Len(t, updatedJobs, workerConcurrency, "Expected UpdateJobsInitial to update 3 jobs for the worker")

	jobsByWorkerRID, err := jobDbHandler.SelectAllJobsByWorkerRID(updatedWorker.RID, 0, 10)
	assert.NoError(t, err, "Expected SelectAllJobsByWorkerRID to not return an error")
	assert.Len(t, jobsByWorkerRID, workerConcurrency, "Expected SelectAllJobsByWorkerRID to return 3 jobs")
}

func TestJobSelectAllJobsBySearch(t *testing.T) {
	helper.SetTestDatabaseConfigEnvs(t, dbPort)
	dbConfig, err := helper.NewDatabaseConfiguration()
	if err != nil {
		t.Fatalf("failed to create database configuration: %v", err)
	}
	database := helper.NewTestDatabase(dbConfig)

	searchTerm := "TestTaskSearch"
	newJobCountSearch := 5
	newJobCountOther := 3

	jobDbHandler, err := NewJobDBHandler(database, true)
	require.NoError(t, err, "Expected NewJobDBHandler to not return an error")

	// Insert multiple jobs with different names
	for i := 0; i < newJobCountSearch; i++ {
		job, err := model.NewJob(searchTerm, nil)
		require.NoError(t, err, "Expected NewJob to not return an error")

		_, err = jobDbHandler.InsertJob(job)
		require.NoError(t, err, "Expected InsertJob to not return an error")
	}

	for i := 0; i < newJobCountOther; i++ {
		job, err := model.NewJob("TestTask", nil)
		require.NoError(t, err, "Expected NewJob to not return an error")

		_, err = jobDbHandler.InsertJob(job)
		require.NoError(t, err, "Expected InsertJob to not return an error")
	}

	jobsBySearch, err := jobDbHandler.SelectAllJobsBySearch(searchTerm, 0, 10)
	assert.NoError(t, err, "Expected SelectAllJobsBySearch to not return an error")
	assert.Len(t, jobsBySearch, newJobCountSearch, "Expected SelectAllJobsBySearch to return all jobs matching the search term")

	pageLength := 3
	paginatedJobsBySearch, err := jobDbHandler.SelectAllJobsBySearch(searchTerm, 0, pageLength)
	assert.NoError(t, err, "Expected SelectAllJobsBySearch to not return an error")
	assert.Len(t, paginatedJobsBySearch, pageLength, "Expected SelectAllJobsBySearch to return 3 jobs")
}

func TestJobSelectJobFromArchive(t *testing.T) {
	helper.SetTestDatabaseConfigEnvs(t, dbPort)
	dbConfig, err := helper.NewDatabaseConfiguration()
	if err != nil {
		t.Fatalf("failed to create database configuration: %v", err)
	}
	database := helper.NewTestDatabase(dbConfig)

	jobDbHandler, err := NewJobDBHandler(database, true)
	require.NoError(t, err, "Expected NewJobDBHandler to not return an error")

	job, err := model.NewJob("TestTask", nil)
	require.NoError(t, err, "Expected NewJob to not return an error")

	insertedJob, err := jobDbHandler.InsertJob(job)
	require.NoError(t, err, "Expected InsertJob to not return an error")

	// Update the job status to SUCCEEDED
	insertedJob.Status = model.JobStatusSucceeded
	updatedJob, err := jobDbHandler.UpdateJobFinal(insertedJob)
	require.NoError(t, err, "Expected UpdateJobFinal to not return an error")

	// Now select the job from archive
	archivedJob, err := jobDbHandler.SelectJobFromArchive(updatedJob.RID)
	assert.NoError(t, err, "Expected SelectJobFromArchive to not return an error")
	assert.NotNil(t, archivedJob, "Expected SelectJobFromArchive to return a non-nil job")
	assert.Equal(t, insertedJob.RID, archivedJob.RID, "Expected archived job RID to match inserted job RID")
	assert.Equal(t, model.JobStatusSucceeded, archivedJob.Status, "Expected archived job status to be SUCCEEDED")
}

func TestJobSelectAllJobsFromArchive(t *testing.T) {
	helper.SetTestDatabaseConfigEnvs(t, dbPort)
	dbConfig, err := helper.NewDatabaseConfiguration()
	if err != nil {
		t.Fatalf("failed to create database configuration: %v", err)
	}
	database := helper.NewTestDatabase(dbConfig)

	jobDbHandler, err := NewJobDBHandler(database, true)
	require.NoError(t, err, "Expected NewJobDBHandler to not return an error")

	newJobCount := 5
	for i := 0; i < newJobCount; i++ {
		job, err := model.NewJob(fmt.Sprintf("TestJob%v", i), nil)
		require.NoError(t, err, "Expected NewJob to not return an error")

		insertedJob, err := jobDbHandler.InsertJob(job)
		require.NoError(t, err, "Expected InsertJob to not return an error")

		// Update the job status to SUCCEEDED
		insertedJob.Status = model.JobStatusSucceeded
		_, err = jobDbHandler.UpdateJobFinal(insertedJob)
		require.NoError(t, err, "Expected UpdateJobFinal to not return an error")
	}

	jobsFromArchive, err := jobDbHandler.SelectAllJobsFromArchive(0, 10)
	assert.NoError(t, err, "Expected SelectAllJobsFromArchive to not return an error")
	assert.Len(t, jobsFromArchive, newJobCount, "Expected SelectAllJobsFromArchive to return all archived jobs")

	pageLength := 3
	paginatedJobsFromArchive, err := jobDbHandler.SelectAllJobsFromArchive(0, pageLength)
	assert.NoError(t, err, "Expected SelectAllJobsFromArchive to not return an error")
	assert.Len(t, paginatedJobsFromArchive, pageLength, "Expected SelectAllJobsFromArchive to return 3 archived jobs")
}

func TestJobSelectAllJobsFromArchiveBySearch(t *testing.T) {
	helper.SetTestDatabaseConfigEnvs(t, dbPort)
	dbConfig, err := helper.NewDatabaseConfiguration()
	if err != nil {
		t.Fatalf("failed to create database configuration: %v", err)
	}
	database := helper.NewTestDatabase(dbConfig)

	searchTerm := "TestTaskSearch"
	newJobCountSearch := 5
	newJobCountOther := 3

	jobDbHandler, err := NewJobDBHandler(database, true)
	require.NoError(t, err, "Expected NewJobDBHandler to not return an error")

	// Insert multiple jobs with different names
	for i := 0; i < newJobCountSearch; i++ {
		job, err := model.NewJob(searchTerm, nil)
		require.NoError(t, err, "Expected NewJob to not return an error")

		insertedJob, err := jobDbHandler.InsertJob(job)
		require.NoError(t, err, "Expected InsertJob to not return an error")

		// Update the job status to SUCCEEDED
		insertedJob.Status = model.JobStatusSucceeded
		_, err = jobDbHandler.UpdateJobFinal(insertedJob)
		require.NoError(t, err, "Expected UpdateJobFinal to not return an error")
	}

	for i := 0; i < newJobCountOther; i++ {
		job, err := model.NewJob("TestTask", nil)
		require.NoError(t, err, "Expected NewJob to not return an error")

		insertedJob, err := jobDbHandler.InsertJob(job)
		require.NoError(t, err, "Expected InsertJob to not return an error")

		// Update the job status to SUCCEEDED
		insertedJob.Status = model.JobStatusSucceeded
		_, err = jobDbHandler.UpdateJobFinal(insertedJob)
		require.NoError(t, err, "Expected UpdateJobFinal to not return an error")
	}

	jobsBySearchFromArchive, err := jobDbHandler.SelectAllJobsFromArchiveBySearch(searchTerm, 0, 10)
	assert.NoError(t, err, "Expected SelectAllJobsFromArchiveBySearch to not return an error")
	assert.Len(t, jobsBySearchFromArchive, newJobCountSearch, "Expected SelectAllJobsFromArchiveBySearch to return all archived jobs matching the search term")

	pageLength := 3
	paginatedJobsBySearchFromArchive, err := jobDbHandler.SelectAllJobsFromArchiveBySearch(searchTerm, 0, pageLength)
	assert.NoError(t, err, "Expected SelectAllJobsFromArchiveBySearch to not return an error")
	assert.Len(t, paginatedJobsBySearchFromArchive, pageLength, "Expected SelectAllJobsFromArchiveBySearch to return 3 archived jobs")
}
