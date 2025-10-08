package database

import (
	"fmt"
	"testing"
	"time"

	"github.com/siherrmann/queuer/helper"
	"github.com/siherrmann/queuer/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWorkerNewWorkerDBHandler(t *testing.T) {
	helper.SetTestDatabaseConfigEnvs(t, dbPort)
	dbConfig, err := helper.NewDatabaseConfiguration()
	if err != nil {
		t.Fatalf("failed to create database configuration: %v", err)
	}
	database := helper.NewTestDatabase(dbConfig)

	workerDbHandler, err := NewWorkerDBHandler(database, true)
	assert.NoError(t, err, "Expected NewWorkerDBHandler to not return an error")
	require.NotNil(t, workerDbHandler, "Expected NewWorkerDBHandler to return a non-nil instance")
	require.NotNil(t, workerDbHandler.db, "Expected NewWorkerDBHandler to have a non-nil database instance")
	require.NotNil(t, workerDbHandler.db.Instance, "Expected NewWorkerDBHandler to have a non-nil database connection instance")

	exists, err := workerDbHandler.CheckTableExistance()
	assert.NoError(t, err)
	assert.True(t, exists)

	err = workerDbHandler.DropTable()
	assert.NoError(t, err)
}

func TestWorkerCheckTableExistance(t *testing.T) {
	helper.SetTestDatabaseConfigEnvs(t, dbPort)
	dbConfig, err := helper.NewDatabaseConfiguration()
	if err != nil {
		t.Fatalf("failed to create database configuration: %v", err)
	}
	database := helper.NewTestDatabase(dbConfig)

	workerDbHandler, err := NewWorkerDBHandler(database, true)
	assert.NoError(t, err, "Expected NewWorkerDBHandler to not return an error")

	exists, err := workerDbHandler.CheckTableExistance()
	assert.NoError(t, err, "Expected CheckTableExistance to not return an error")
	assert.True(t, exists, "Expected worker table to exist")
}

func TestWorkerCreateTable(t *testing.T) {
	helper.SetTestDatabaseConfigEnvs(t, dbPort)
	dbConfig, err := helper.NewDatabaseConfiguration()
	if err != nil {
		t.Fatalf("failed to create database configuration: %v", err)
	}
	database := helper.NewTestDatabase(dbConfig)

	workerDbHandler, err := NewWorkerDBHandler(database, true)
	assert.NoError(t, err, "Expected NewWorkerDBHandler to not return an error")

	err = workerDbHandler.CreateTable()
	assert.NoError(t, err, "Expected CreateTable to not return an error")
}

func TestWorkerDropTable(t *testing.T) {
	helper.SetTestDatabaseConfigEnvs(t, dbPort)
	dbConfig, err := helper.NewDatabaseConfiguration()
	if err != nil {
		t.Fatalf("failed to create database configuration: %v", err)
	}
	database := helper.NewTestDatabase(dbConfig)

	workerDbHandler, err := NewWorkerDBHandler(database, true)
	assert.NoError(t, err, "Expected NewWorkerDBHandler to not return an error")

	err = workerDbHandler.DropTable()
	assert.NoError(t, err, "Expected DropTable to not return an error")
}

func TestWorkerInsertWorker(t *testing.T) {
	helper.SetTestDatabaseConfigEnvs(t, dbPort)
	dbConfig, err := helper.NewDatabaseConfiguration()
	if err != nil {
		t.Fatalf("failed to create database configuration: %v", err)
	}
	database := helper.NewTestDatabase(dbConfig)

	workerDbHandler, err := NewWorkerDBHandler(database, true)
	assert.NoError(t, err, "Expected NewWorkerDBHandler to not return an error")

	worker, err := model.NewWorker("Worker", 1)
	require.NoError(t, err, "Expected NewWorker to not return an error")

	insertedWorker, err := workerDbHandler.InsertWorker(worker)
	assert.NoError(t, err, "Expected InsertWorker to not return an error")
	assert.NotNil(t, insertedWorker, "Expected InsertWorker to return a non-nil worker")
	assert.NotEqual(t, worker.RID, insertedWorker.RID, "Expected inserted worker RID to match")
	assert.Equal(t, worker.Name, insertedWorker.Name, "Expected inserted worker Name to match")
	assert.Equal(t, worker.Status, insertedWorker.Status, "Expected inserted worker Status to match")
	assert.WithinDuration(t, insertedWorker.CreatedAt, time.Now(), 1*time.Second, "Expected inserted worker CreatedAt time to match")
	assert.WithinDuration(t, insertedWorker.UpdatedAt, time.Now(), 1*time.Second, "Expected inserted worker UpdatedAt time to match")
}

func TestWorkerUpdateWorker(t *testing.T) {
	helper.SetTestDatabaseConfigEnvs(t, dbPort)
	dbConfig, err := helper.NewDatabaseConfiguration()
	if err != nil {
		t.Fatalf("failed to create database configuration: %v", err)
	}
	database := helper.NewTestDatabase(dbConfig)

	workerDbHandler, err := NewWorkerDBHandler(database, true)
	assert.NoError(t, err, "Expected NewWorkerDBHandler to not return an error")

	worker, err := model.NewWorker("Worker", 1)
	require.NoError(t, err, "Expected NewWorker to not return an error")

	insertedWorker, err := workerDbHandler.InsertWorker(worker)
	require.NoError(t, err, "Expected InsertWorker to not return an error")

	// Update the worker's name and options
	insertedWorker.Name = "UpdatedWorker"
	insertedWorker.Options = &model.OnError{
		Timeout:      10,
		MaxRetries:   3,
		RetryDelay:   1,
		RetryBackoff: model.RETRY_BACKOFF_EXPONENTIAL,
	}
	insertedWorker.AvailableTasks = []string{"task1", "task2"}
	insertedWorker.AvailableNextIntervalFuncs = []string{"interval1", "interval2"}

	updatedWorker, err := workerDbHandler.UpdateWorker(insertedWorker)
	assert.NoError(t, err, "Expected UpdateWorker to not return an error")
	assert.Equal(t, insertedWorker.Name, updatedWorker.Name, "Expected updated worker Name to match")
	assert.Equal(t, insertedWorker.Options, updatedWorker.Options, "Expected updated worker Options to match")
	assert.Equal(t, insertedWorker.AvailableTasks, updatedWorker.AvailableTasks, "Expected updated worker AvailableTasks to match")
	assert.Equal(t, insertedWorker.AvailableNextIntervalFuncs, updatedWorker.AvailableNextIntervalFuncs, "Expected updated worker AvailableNextInterval to match")
	assert.Equal(t, insertedWorker.MaxConcurrency, updatedWorker.MaxConcurrency, "Expected updated worker MaxConcurrency to match")
}

func TestUpdateStaleWorkers(t *testing.T) {
	helper.SetTestDatabaseConfigEnvs(t, dbPort)
	dbConfig, err := helper.NewDatabaseConfiguration()
	if err != nil {
		t.Fatalf("failed to create database configuration: %v", err)
	}
	database := helper.NewTestDatabase(dbConfig)

	workerDbHandler, err := NewWorkerDBHandler(database, true)
	assert.NoError(t, err, "Expected NewWorkerDBHandler to not return an error")

	t.Run("Update stale workers with different statuses", func(t *testing.T) {
		// Create workers: READY (stale), RUNNING (stale), STOPPED (stale), READY (fresh)
		statuses := []string{model.WorkerStatusReady, model.WorkerStatusRunning, model.WorkerStatusStopped, model.WorkerStatusReady}
		workers := make([]*model.Worker, len(statuses))

		for i, status := range statuses {
			testWorker, err := model.NewWorker(fmt.Sprintf("worker-%d", i), 3)
			require.NoError(t, err, "Expected to create test worker")

			insertedWorker, err := workerDbHandler.InsertWorker(testWorker)
			require.NoError(t, err, "Expected to insert worker")

			insertedWorker.Status = status
			updatedWorker, err := workerDbHandler.UpdateWorker(insertedWorker)
			require.NoError(t, err, "Expected to update worker status")
			workers[i] = updatedWorker
		}

		// Make first 3 workers stale (READY, RUNNING, STOPPED)
		staleTime := time.Now().UTC().Add(-1 * time.Hour)
		for i := 0; i < 3; i++ {
			_, err = database.Instance.Exec(
				"UPDATE worker SET updated_at = $1 WHERE rid = $2",
				staleTime, workers[i].RID,
			)
			require.NoError(t, err, "Expected to make worker stale")
		}

		// Test UpdateStaleWorkers - should update only stale READY and RUNNING workers
		staleThreshold := 10 * time.Minute
		updatedCount, err := workerDbHandler.UpdateStaleWorkers(staleThreshold)
		assert.NoError(t, err, "Expected UpdateStaleWorkers to complete successfully")
		assert.Equal(t, 2, updatedCount, "Expected 2 workers to be updated (READY and RUNNING)")

		// Verify only stale READY and RUNNING workers were updated to STOPPED
		for i, worker := range workers {
			updatedWorker, err := workerDbHandler.SelectWorker(worker.RID)
			require.NoError(t, err, "Expected to select worker %d", i)

			if i < 2 { // First two workers (READY, RUNNING) should be STOPPED
				assert.Equal(t, model.WorkerStatusStopped, updatedWorker.Status)
			} else if i == 2 { // STOPPED worker should remain STOPPED
				assert.Equal(t, model.WorkerStatusStopped, updatedWorker.Status)
			} else { // Fresh worker should remain READY
				assert.Equal(t, model.WorkerStatusReady, updatedWorker.Status)
			}
		}

		// Clean up
		for _, worker := range workers {
			err = workerDbHandler.DeleteWorker(worker.RID)
			assert.NoError(t, err, "Expected to delete worker")
		}
	})

	t.Run("No stale workers", func(t *testing.T) {
		// Create fresh worker
		testWorker, err := model.NewWorker("fresh-worker", 3)
		require.NoError(t, err, "Expected to create test worker")

		insertedWorker, err := workerDbHandler.InsertWorker(testWorker)
		require.NoError(t, err, "Expected to insert worker")

		// Test with short threshold - no workers should be updated
		updatedCount, err := workerDbHandler.UpdateStaleWorkers(10 * time.Second)
		assert.NoError(t, err, "Expected UpdateStaleWorkers to complete successfully")
		assert.Equal(t, 0, updatedCount, "Expected no workers to be updated")

		// Clean up
		err = workerDbHandler.DeleteWorker(insertedWorker.RID)
		assert.NoError(t, err, "Expected to delete worker")
	})
}

func TestWorkerDeleteWorker(t *testing.T) {
	helper.SetTestDatabaseConfigEnvs(t, dbPort)
	dbConfig, err := helper.NewDatabaseConfiguration()
	if err != nil {
		t.Fatalf("failed to create database configuration: %v", err)
	}
	database := helper.NewTestDatabase(dbConfig)

	workerDbHandler, err := NewWorkerDBHandler(database, true)
	assert.NoError(t, err, "Expected NewWorkerDBHandler to not return an error")

	worker, err := model.NewWorker("Worker", 1)
	require.NoError(t, err, "Expected NewWorker to not return an error")

	insertedWorker, err := workerDbHandler.InsertWorker(worker)
	require.NoError(t, err, "Expected InsertWorker to not return an error")

	err = workerDbHandler.DeleteWorker(insertedWorker.RID)
	assert.NoError(t, err, "Expected DeleteWorker to not return an error")

	// Verify that the worker was deleted
	deletedWorker, err := workerDbHandler.SelectWorker(insertedWorker.RID)
	assert.Error(t, err, "Expected SelectWorker to return an error for deleted worker")
	assert.Nil(t, deletedWorker, "Expected deleted worker to be nil")
}

func TestWorkerSelectWorker(t *testing.T) {
	helper.SetTestDatabaseConfigEnvs(t, dbPort)
	dbConfig, err := helper.NewDatabaseConfiguration()
	if err != nil {
		t.Fatalf("failed to create database configuration: %v", err)
	}
	database := helper.NewTestDatabase(dbConfig)

	workerDbHandler, err := NewWorkerDBHandler(database, true)
	assert.NoError(t, err, "Expected NewWorkerDBHandler to not return an error")

	worker, err := model.NewWorker("Worker", 1)
	require.NoError(t, err, "Expected NewWorker to not return an error")

	insertedWorker, err := workerDbHandler.InsertWorker(worker)
	require.NoError(t, err, "Expected InsertWorker to not return an error")

	selectedWorker, err := workerDbHandler.SelectWorker(insertedWorker.RID)
	assert.NoError(t, err, "Expected SelectWorker to not return an error")
	assert.NotNil(t, selectedWorker, "Expected SelectWorker to return a non-nil worker")
	assert.Equal(t, insertedWorker.RID, selectedWorker.RID, "Expected selected worker RID to match")
	assert.Equal(t, insertedWorker.Name, selectedWorker.Name, "Expected selected worker Name to match")
}

func TestWorkerSelectAllWorkers(t *testing.T) {
	helper.SetTestDatabaseConfigEnvs(t, dbPort)
	dbConfig, err := helper.NewDatabaseConfiguration()
	if err != nil {
		t.Fatalf("failed to create database configuration: %v", err)
	}
	database := helper.NewTestDatabase(dbConfig)

	workerDbHandler, err := NewWorkerDBHandler(database, true)
	assert.NoError(t, err, "Expected NewWorkerDBHandler to not return an error")

	// Insert multiple workers
	for i := 0; i < 5; i++ {
		worker, err := model.NewWorker(fmt.Sprintf("Worker%v", i), 1)
		require.NoError(t, err, "Expected NewWorker to not return an error")

		_, err = workerDbHandler.InsertWorker(worker)
		require.NoError(t, err, "Expected InsertWorker to not return an error")
	}

	allWorkers, err := workerDbHandler.SelectAllWorkers(0, 10)
	assert.NoError(t, err, "Expected SelectAllWorkers to not return an error")
	assert.Equal(t, 5, len(allWorkers), "Expected SelectAllWorkers to return all workers")

	pageLength := 3
	paginatedWorkers, err := workerDbHandler.SelectAllWorkers(0, pageLength)
	assert.NoError(t, err, "Expected SelectAllWorkers to not return an error")
	assert.Equal(t, pageLength, len(paginatedWorkers), "Expected SelectAllWorkers to return 3 workers")
}

func TestWorkerSelectAllWorkersBySearch(t *testing.T) {
	helper.SetTestDatabaseConfigEnvs(t, dbPort)
	dbConfig, err := helper.NewDatabaseConfiguration()
	if err != nil {
		t.Fatalf("failed to create database configuration: %v", err)
	}
	database := helper.NewTestDatabase(dbConfig)

	workerDbHandler, err := NewWorkerDBHandler(database, true)
	assert.NoError(t, err, "Expected NewWorkerDBHandler to not return an error")

	// Insert multiple workers with different names
	for i := 0; i < 5; i++ {
		worker, err := model.NewWorker(fmt.Sprintf("Worker%v", i), 1)
		require.NoError(t, err, "Expected NewWorker to not return an error")

		_, err = workerDbHandler.InsertWorker(worker)
		require.NoError(t, err, "Expected InsertWorker to not return an error")
	}

	searchTerm := "Worker1"
	foundWorkers, err := workerDbHandler.SelectAllWorkersBySearch(searchTerm, 0, 10)
	assert.NoError(t, err, "Expected SelectAllWorkersBySearch to not return an error")
	require.Equal(t, 1, len(foundWorkers), "Expected SelectAllWorkersBySearch to return 1 worker matching search term")
	assert.Equal(t, "Worker1", foundWorkers[0].Name, "Expected found worker Name to match search term")
}

func TestWorkerSelectAllConnections(t *testing.T) {
	helper.SetTestDatabaseConfigEnvs(t, dbPort)
	dbConfig, err := helper.NewDatabaseConfiguration()
	if err != nil {
		t.Fatalf("failed to create database configuration: %v", err)
	}
	database := helper.NewTestDatabase(dbConfig)

	workerDbHandler, err := NewWorkerDBHandler(database, true)
	assert.NoError(t, err, "Expected NewWorkerDBHandler to not return an error")

	// There should already be an active connection created by NewTestDatabase
	allConnections, err := workerDbHandler.SelectAllConnections()
	assert.NoError(t, err, "Expected SelectAllConnections to not return an error")
	assert.GreaterOrEqual(t, len(allConnections), 1, "Expected at least one connection to exist")

	paginatedConnections, err := workerDbHandler.SelectAllConnections()
	assert.NoError(t, err, "Expected SelectAllConnections to not return an error")
	assert.GreaterOrEqual(t, len(paginatedConnections), 1, "Expected SelectAllConnections to return 1 connection as we connect one in NewTestDatabase")
}
