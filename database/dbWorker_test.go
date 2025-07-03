package database

import (
	"fmt"
	"queuer/helper"
	"queuer/model"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWorkerNewWorkerDBHandler(t *testing.T) {
	dbConfig := helper.NewTestDatabaseConfig(port)
	database := helper.NewTestDatabase(dbConfig)

	workerDBHandler, err := NewWorkerDBHandler(database, true)
	assert.NoError(t, err, "Expected NewWorkerDBHandler to not return an error")

	if workerDBHandler == nil || workerDBHandler.db == nil || workerDBHandler.db.Instance == nil {
		t.Error("Expected NewWorkerDBHandler to return a non-nil instance")
	}
}

func TestWorkerCheckTableExistance(t *testing.T) {
	dbConfig := helper.NewTestDatabaseConfig(port)
	database := helper.NewTestDatabase(dbConfig)

	workerDBHandler, err := NewWorkerDBHandler(database, true)
	assert.NoError(t, err, "Expected NewWorkerDBHandler to not return an error")

	exists, err := workerDBHandler.CheckTableExistance()
	assert.NoError(t, err, "Expected CheckTableExistance to not return an error")
	assert.True(t, exists, "Expected worker table to exist")
}

func TestWorkerCreateTable(t *testing.T) {
	dbConfig := helper.NewTestDatabaseConfig(port)
	database := helper.NewTestDatabase(dbConfig)

	workerDBHandler, err := NewWorkerDBHandler(database, true)
	assert.NoError(t, err, "Expected NewWorkerDBHandler to not return an error")

	err = workerDBHandler.CreateTable()
	assert.NoError(t, err, "Expected CreateTable to not return an error")
}

func TestWorkerDropTable(t *testing.T) {
	dbConfig := helper.NewTestDatabaseConfig(port)
	database := helper.NewTestDatabase(dbConfig)

	workerDBHandler, err := NewWorkerDBHandler(database, true)
	assert.NoError(t, err, "Expected NewWorkerDBHandler to not return an error")

	err = workerDBHandler.DropTable()
	assert.NoError(t, err, "Expected DropTable to not return an error")
}

func TestWorkerInsertWorker(t *testing.T) {
	dbConfig := helper.NewTestDatabaseConfig(port)
	database := helper.NewTestDatabase(dbConfig)

	workerDBHandler, err := NewWorkerDBHandler(database, true)
	assert.NoError(t, err, "Expected NewWorkerDBHandler to not return an error")

	worker, err := model.NewWorker("Worker", 1)
	require.NoError(t, err, "Expected NewWorker to not return an error")

	insertedWorker, err := workerDBHandler.InsertWorker(worker)
	assert.NoError(t, err, "Expected InsertWorker to not return an error")
	assert.NotNil(t, insertedWorker, "Expected InsertWorker to return a non-nil worker")
	assert.NotEqual(t, insertedWorker.RID, worker.RID, "Expected inserted worker RID to match")
	assert.Equal(t, insertedWorker.Name, worker.Name, "Expected inserted worker Name to match")
	assert.Equal(t, insertedWorker.Status, worker.Status, "Expected inserted worker Status to match")
	assert.WithinDuration(t, insertedWorker.CreatedAt, time.Now(), 1*time.Second, "Expected inserted worker CreatedAt time to match")
	assert.WithinDuration(t, insertedWorker.UpdatedAt, time.Now(), 1*time.Second, "Expected inserted worker UpdatedAt time to match")
}

func TestWorkerUpdateWorker(t *testing.T) {
	dbConfig := helper.NewTestDatabaseConfig(port)
	database := helper.NewTestDatabase(dbConfig)

	workerDBHandler, err := NewWorkerDBHandler(database, true)
	assert.NoError(t, err, "Expected NewWorkerDBHandler to not return an error")

	worker, err := model.NewWorker("Worker", 1)
	require.NoError(t, err, "Expected NewWorker to not return an error")

	insertedWorker, err := workerDBHandler.InsertWorker(worker)
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

	updatedWorker, err := workerDBHandler.UpdateWorker(insertedWorker)
	assert.NoError(t, err, "Expected UpdateWorker to not return an error")
	assert.Equal(t, updatedWorker.Name, insertedWorker.Name, "Expected updated worker Name to match")
	assert.Equal(t, updatedWorker.Options, insertedWorker.Options, "Expected updated worker Options to match")
	assert.Equal(t, updatedWorker.AvailableTasks, insertedWorker.AvailableTasks, "Expected updated worker AvailableTasks to match")
	assert.Equal(t, updatedWorker.AvailableNextIntervalFuncs, insertedWorker.AvailableNextIntervalFuncs, "Expected updated worker AvailableNextInterval to match")
	assert.Equal(t, updatedWorker.MaxConcurrency, insertedWorker.MaxConcurrency, "Expected updated worker MaxConcurrency to match")
}

func TestWorkerDeleteWorker(t *testing.T) {
	dbConfig := helper.NewTestDatabaseConfig(port)
	database := helper.NewTestDatabase(dbConfig)

	workerDBHandler, err := NewWorkerDBHandler(database, true)
	assert.NoError(t, err, "Expected NewWorkerDBHandler to not return an error")

	worker, err := model.NewWorker("Worker", 1)
	require.NoError(t, err, "Expected NewWorker to not return an error")

	insertedWorker, err := workerDBHandler.InsertWorker(worker)
	require.NoError(t, err, "Expected InsertWorker to not return an error")

	err = workerDBHandler.DeleteWorker(insertedWorker.RID)
	assert.NoError(t, err, "Expected DeleteWorker to not return an error")

	// Verify that the worker was deleted
	deletedWorker, err := workerDBHandler.SelectWorker(insertedWorker.RID)
	assert.Error(t, err, "Expected SelectWorker to return an error for deleted worker")
	assert.Nil(t, deletedWorker, "Expected deleted worker to be nil")
}

func TestWorkerSelectWorker(t *testing.T) {
	dbConfig := helper.NewTestDatabaseConfig(port)
	database := helper.NewTestDatabase(dbConfig)

	workerDBHandler, err := NewWorkerDBHandler(database, true)
	assert.NoError(t, err, "Expected NewWorkerDBHandler to not return an error")

	worker, err := model.NewWorker("Worker", 1)
	require.NoError(t, err, "Expected NewWorker to not return an error")

	insertedWorker, err := workerDBHandler.InsertWorker(worker)
	require.NoError(t, err, "Expected InsertWorker to not return an error")

	selectedWorker, err := workerDBHandler.SelectWorker(insertedWorker.RID)
	assert.NoError(t, err, "Expected SelectWorker to not return an error")
	assert.NotNil(t, selectedWorker, "Expected SelectWorker to return a non-nil worker")
	assert.Equal(t, selectedWorker.RID, insertedWorker.RID, "Expected selected worker RID to match")
	assert.Equal(t, selectedWorker.Name, insertedWorker.Name, "Expected selected worker Name to match")
}

func TestWorkerSelectAllWorkers(t *testing.T) {
	dbConfig := helper.NewTestDatabaseConfig(port)
	database := helper.NewTestDatabase(dbConfig)

	workerDBHandler, err := NewWorkerDBHandler(database, true)
	assert.NoError(t, err, "Expected NewWorkerDBHandler to not return an error")

	// Insert multiple workers
	for i := 0; i < 5; i++ {
		worker, err := model.NewWorker(fmt.Sprintf("Worker%v", i), 1)
		require.NoError(t, err, "Expected NewWorker to not return an error")

		_, err = workerDBHandler.InsertWorker(worker)
		require.NoError(t, err, "Expected InsertWorker to not return an error")
	}

	allWorkers, err := workerDBHandler.SelectAllWorkers(0, 10)
	assert.NoError(t, err, "Expected SelectAllWorkers to not return an error")
	assert.Equal(t, len(allWorkers), 5, "Expected SelectAllWorkers to return all workers")

	pageLength := 3
	paginatedWorkers, err := workerDBHandler.SelectAllWorkers(0, pageLength)
	assert.NoError(t, err, "Expected SelectAllWorkers to not return an error")
	assert.Equal(t, len(paginatedWorkers), pageLength, "Expected SelectAllWorkers to return 3 workers")
}

func TestWorkerSelectAllWorkersBySearch(t *testing.T) {
	dbConfig := helper.NewTestDatabaseConfig(port)
	database := helper.NewTestDatabase(dbConfig)

	workerDBHandler, err := NewWorkerDBHandler(database, true)
	assert.NoError(t, err, "Expected NewWorkerDBHandler to not return an error")

	// Insert multiple workers with different names
	for i := 0; i < 5; i++ {
		worker, err := model.NewWorker(fmt.Sprintf("Worker%v", i), 1)
		require.NoError(t, err, "Expected NewWorker to not return an error")

		_, err = workerDBHandler.InsertWorker(worker)
		require.NoError(t, err, "Expected InsertWorker to not return an error")
	}

	searchTerm := "Worker1"
	foundWorkers, err := workerDBHandler.SelectAllWorkersBySearch(searchTerm, 0, 10)
	assert.NoError(t, err, "Expected SelectAllWorkersBySearch to not return an error")
	require.Equal(t, len(foundWorkers), 1, "Expected SelectAllWorkersBySearch to return 1 worker matching search term")
	assert.Equal(t, foundWorkers[0].Name, "Worker1", "Expected found worker Name to match search term")
}
