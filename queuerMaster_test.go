package queuer

import (
	"context"
	"testing"
	"time"

	"github.com/siherrmann/queuer/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMasterTicker(t *testing.T) {
	envs := map[string]string{
		"QUEUER_DB_HOST":     "localhost",
		"QUEUER_DB_PORT":     dbPort,
		"QUEUER_DB_DATABASE": "database",
		"QUEUER_DB_USERNAME": "user",
		"QUEUER_DB_PASSWORD": "password",
		"QUEUER_DB_SCHEMA":   "public",
		"QUEUER_DB_SSLMODE":  "disable",
	}
	for key, value := range envs {
		t.Setenv(key, value)
	}

	queuer := NewQueuer("test", 10)
	require.NotNil(t, queuer, "Expected Queuer to be created successfully")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	t.Run("Master ticker fails with nil old master", func(t *testing.T) {
		var oldMaster *model.Master
		masterSettings := &model.MasterSettings{
			MasterPollInterval: 5 * time.Second,
		}

		err := queuer.masterTicker(ctx, oldMaster, masterSettings)
		assert.Error(t, err, "Expected error starting master ticker with nil old master")
	})
}

func TestCheckStaleWorkers(t *testing.T) {
	envs := map[string]string{
		"QUEUER_DB_HOST":     "localhost",
		"QUEUER_DB_PORT":     dbPort,
		"QUEUER_DB_DATABASE": "database",
		"QUEUER_DB_USERNAME": "user",
		"QUEUER_DB_PASSWORD": "password",
		"QUEUER_DB_SCHEMA":   "public",
		"QUEUER_DB_SSLMODE":  "disable",
	}
	for key, value := range envs {
		t.Setenv(key, value)
	}

	t.Run("Check stale workers with no workers", func(t *testing.T) {
		queuer := NewQueuer("test", 10)
		require.NotNil(t, queuer, "Expected Queuer to be created successfully")

		err := queuer.checkStaleWorkers()
		assert.NoError(t, err, "Expected checkStaleWorkers to complete without error when no workers exist")

		err = queuer.Stop()
		assert.NoError(t, err, "Expected Queuer to stop successfully")
	})

	t.Run("Check stale workers with fresh workers", func(t *testing.T) {
		queuer := NewQueuer("test", 10)
		require.NotNil(t, queuer, "Expected Queuer to be created successfully")

		queuer.Start(context.Background(), func() {})
		defer queuer.Stop()

		err := queuer.checkStaleWorkers()
		require.NoError(t, err, "Expected checkStaleWorkers to complete without error")

		worker, err := queuer.dbWorker.SelectWorker(queuer.worker.RID)
		require.NoError(t, err, "Expected to select worker successfully")
		assert.Equal(t, model.WorkerStatusRunning, worker.Status, "Expected fresh worker to remain RUNNING")
	})

	t.Run("Check stale workers marks old workers as offline", func(t *testing.T) {
		queuer := NewQueuer("test", 10)
		require.NotNil(t, queuer, "Expected Queuer to be created successfully")
		defer queuer.Stop()

		// Create a non running worker with old timestamp
		testWorker, err := model.NewWorker("stale-worker", 5)
		require.NoError(t, err, "Expected to create test worker")

		insertedWorker, err := queuer.dbWorker.InsertWorker(testWorker)
		require.NoError(t, err, "Expected to insert test worker")

		_, err = queuer.DB.Exec(
			"UPDATE worker SET updated_at = $1, status = $2 WHERE rid = $3",
			time.Now().UTC().Add(-1*time.Hour), model.WorkerStatusRunning, insertedWorker.RID,
		)
		require.NoError(t, err, "Expected to update worker timestamp directly")

		// Run the stale check
		err = queuer.checkStaleWorkers()
		assert.NoError(t, err, "Expected checkStaleWorkers to complete without error")

		staleWorkerAfter, err := queuer.dbWorker.SelectWorker(insertedWorker.RID)
		require.NoError(t, err, "Expected to select stale worker successfully")
		assert.Equal(t, model.WorkerStatusStopped, staleWorkerAfter.Status, "Expected stale worker to be marked as STOPPED")
	})

	t.Run("Check stale workers ignores already stopped workers", func(t *testing.T) {
		queuer := NewQueuer("test", 10)
		require.NotNil(t, queuer, "Expected Queuer to be created successfully")
		defer queuer.Stop()

		// Create a worker that's already stopped
		testWorker, err := model.NewWorker("stopped-worker", 5)
		require.NoError(t, err, "Expected to create test worker")

		insertedWorker, err := queuer.dbWorker.InsertWorker(testWorker)
		require.NoError(t, err, "Expected to insert test worker")

		_, err = queuer.DB.Exec(
			"UPDATE worker SET updated_at = $1, status = $2 WHERE rid = $3",
			time.Now().Add(-10*time.Minute), model.WorkerStatusStopped, insertedWorker.RID,
		)
		require.NoError(t, err, "Expected to update worker status directly")

		// Run the stale check
		err = queuer.checkStaleWorkers()
		assert.NoError(t, err, "Expected checkStaleWorkers to complete without error")

		offlineWorker, err := queuer.dbWorker.SelectWorker(insertedWorker.RID)
		require.NoError(t, err, "Expected to select offline worker successfully")
		assert.Equal(t, model.WorkerStatusStopped, offlineWorker.Status, "Expected offline worker to remain STOPPED")
	})
}

func TestCheckStaleJobs(t *testing.T) {
	envs := map[string]string{
		"QUEUER_DB_HOST":     "localhost",
		"QUEUER_DB_PORT":     dbPort,
		"QUEUER_DB_DATABASE": "database",
		"QUEUER_DB_USERNAME": "user",
		"QUEUER_DB_PASSWORD": "password",
		"QUEUER_DB_SCHEMA":   "public",
		"QUEUER_DB_SSLMODE":  "disable",
	}
	for key, value := range envs {
		t.Setenv(key, value)
	}

	t.Run("Check stale jobs with no jobs", func(t *testing.T) {
		queuer := NewQueuer("test", 10)
		require.NotNil(t, queuer, "Expected Queuer to be created successfully")

		err := queuer.checkStaleJobs()
		assert.NoError(t, err, "Expected checkStaleJobs to complete without error when no jobs exist")

		err = queuer.Stop()
		assert.NoError(t, err, "Expected Queuer to stop successfully")
	})

	t.Run("Check stale jobs cancels jobs with stopped workers", func(t *testing.T) {
		queuer := NewQueuer("test", 10)
		require.NotNil(t, queuer, "Expected Queuer to be created successfully")
		defer queuer.Stop()

		// Create a worker and set it to STOPPED
		testWorker, err := model.NewWorker("stopped-worker", 3)
		require.NoError(t, err, "Expected to create test worker")

		insertedWorker, err := queuer.dbWorker.InsertWorker(testWorker)
		require.NoError(t, err, "Expected to insert test worker")

		insertedWorker.Status = model.WorkerStatusStopped
		_, err = queuer.dbWorker.UpdateWorker(insertedWorker)
		require.NoError(t, err, "Expected to update worker to STOPPED")

		// Create a job assigned to the stopped worker
		testJob, err := model.NewJob("test-task", nil)
		require.NoError(t, err, "Expected to create test job")
		testJob.WorkerRID = insertedWorker.RID

		insertedJob, err := queuer.dbJob.InsertJob(testJob)
		require.NoError(t, err, "Expected to insert test job")

		// Update job to have worker assignment
		_, err = queuer.DB.Exec(
			"UPDATE job SET worker_rid = $1, status = $2 WHERE rid = $3",
			insertedWorker.RID, model.JobStatusRunning, insertedJob.RID,
		)
		require.NoError(t, err, "Expected to update job worker assignment")

		// Run the stale job check
		err = queuer.checkStaleJobs()
		assert.NoError(t, err, "Expected checkStaleJobs to complete without error")

		// Verify job was cancelled
		updatedJob, err := queuer.dbJob.SelectJob(insertedJob.RID)
		require.NoError(t, err, "Expected to select job successfully")
		assert.Equal(t, model.JobStatusCancelled, updatedJob.Status, "Expected job assigned to stopped worker to be cancelled")
	})

	t.Run("Check stale jobs ignores jobs with final statuses", func(t *testing.T) {
		queuer := NewQueuer("test", 10)
		require.NotNil(t, queuer, "Expected Queuer to be created successfully")
		defer queuer.Stop()

		// Create a worker and set it to STOPPED
		testWorker, err := model.NewWorker("stopped-worker", 3)
		require.NoError(t, err, "Expected to create test worker")

		insertedWorker, err := queuer.dbWorker.InsertWorker(testWorker)
		require.NoError(t, err, "Expected to insert test worker")

		insertedWorker.Status = model.WorkerStatusStopped
		_, err = queuer.dbWorker.UpdateWorker(insertedWorker)
		require.NoError(t, err, "Expected to update worker to STOPPED")

		// Create a job with final status assigned to the stopped worker
		testJob, err := model.NewJob("test-task", nil)
		require.NoError(t, err, "Expected to create test job")
		testJob.WorkerRID = insertedWorker.RID

		insertedJob, err := queuer.dbJob.InsertJob(testJob)
		require.NoError(t, err, "Expected to insert test job")

		// Update job to have final status
		_, err = queuer.DB.Exec(
			"UPDATE job SET worker_rid = $1, status = $2 WHERE rid = $3",
			insertedWorker.RID, model.JobStatusSucceeded, insertedJob.RID,
		)
		require.NoError(t, err, "Expected to update job status to final")

		// Run the stale job check
		err = queuer.checkStaleJobs()
		assert.NoError(t, err, "Expected checkStaleJobs to complete without error")

		// Verify job status remained unchanged
		updatedJob, err := queuer.dbJob.SelectJob(insertedJob.RID)
		require.NoError(t, err, "Expected to select job successfully")
		assert.Equal(t, model.JobStatusSucceeded, updatedJob.Status, "Expected job with final status to remain unchanged")
	})

	t.Run("Check stale jobs ignores jobs with ready workers", func(t *testing.T) {
		queuer := NewQueuer("test", 10)
		require.NotNil(t, queuer, "Expected Queuer to be created successfully")
		defer queuer.Stop()

		// Create a worker and keep it READY
		testWorker, err := model.NewWorker("ready-worker", 3)
		require.NoError(t, err, "Expected to create test worker")

		insertedWorker, err := queuer.dbWorker.InsertWorker(testWorker)
		require.NoError(t, err, "Expected to insert test worker")

		// Create a job assigned to the ready worker
		testJob, err := model.NewJob("test-task", nil)
		require.NoError(t, err, "Expected to create test job")
		testJob.WorkerRID = insertedWorker.RID

		insertedJob, err := queuer.dbJob.InsertJob(testJob)
		require.NoError(t, err, "Expected to insert test job")

		// Update job to have worker assignment
		_, err = queuer.DB.Exec(
			"UPDATE job SET worker_rid = $1, status = $2 WHERE rid = $3",
			insertedWorker.RID, model.JobStatusRunning, insertedJob.RID,
		)
		require.NoError(t, err, "Expected to update job worker assignment")

		// Run the stale job check
		err = queuer.checkStaleJobs()
		assert.NoError(t, err, "Expected checkStaleJobs to complete without error")

		// Verify job status remained unchanged
		updatedJob, err := queuer.dbJob.SelectJob(insertedJob.RID)
		require.NoError(t, err, "Expected to select job successfully")
		assert.Equal(t, model.JobStatusRunning, updatedJob.Status, "Expected job assigned to ready worker to remain unchanged")
	})
}
