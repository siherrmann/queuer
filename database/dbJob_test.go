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
	assert.Equal(t, insertedJob.TaskName, job.TaskName, "Expected task name to match")
	assert.Equal(t, insertedJob.Status, model.JobStatusQueued, "Expected job status to be QUEUED")
	assert.Equal(t, insertedJob.Attempts, 0, "Expected job attempts to be 0")
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
	assert.Equal(t, insertedJob.TaskName, job.TaskName, "Expected task name to match")
	assert.Equal(t, insertedJob.Status, model.JobStatusQueued, "Expected job status to be QUEUED")
	assert.Equal(t, insertedJob.Attempts, 0, "Expected job attempts to be 0")
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
	assert.Equal(t, updatedJobs[0].ID, insertedJob.ID, "Expected updated job ID to match inserted job ID")
	assert.Equal(t, updatedJobs[0].WorkerID, updatedWorker.ID, "Expected updated job WorkerID to match new worker ID")
	assert.Equal(t, updatedJobs[0].WorkerRID, updatedWorker.RID, "Expected updated job WorkerRID to match new worker RID")
	assert.Equal(t, updatedJobs[0].Status, model.JobStatusRunning, "Expected job status to be RUNNING")
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
	assert.Equal(t, updatedJob.ID, insertedJob.ID, "Expected updated job ID to match inserted job ID")
	assert.Equal(t, updatedJob.Status, model.JobStatusSucceeded, "Expected job status to be SUCCEEDED")
	assert.WithinDuration(t, updatedJob.UpdatedAt, time.Now(), 1*time.Second, "Expected inserted worker UpdatedAt time to match")
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
	assert.Equal(t, selectedJob.RID, insertedJob.RID, "Expected selected job RID to match inserted job RID")
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
	assert.Equal(t, archivedJob.RID, insertedJob.RID, "Expected archived job RID to match inserted job RID")
	assert.Equal(t, archivedJob.Status, model.JobStatusSucceeded, "Expected archived job status to be SUCCEEDED")
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
