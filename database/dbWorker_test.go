package database

import (
	"fmt"
	"log"
	"queuer/helper"
	"queuer/model"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewWorkerDBHandler(t *testing.T) {
	database := helper.NewTestDatabase(port)

	workerDBHandler, err := NewWorkerDBHandler(database)
	assert.NoError(t, err, "Expected NewWorkerDBHandler to not return an error")

	if workerDBHandler == nil || workerDBHandler.db == nil || workerDBHandler.db.Instance == nil {
		t.Fatal("Expected NewWorkerDBHandler to return a non-nil instance")
	}
}

func TestCheckTableExistance(t *testing.T) {
	database := helper.NewTestDatabase(port)

	workerDBHandler, err := NewWorkerDBHandler(database)
	assert.NoError(t, err, "Expected NewWorkerDBHandler to not return an error")

	exists, err := workerDBHandler.CheckTableExistance()
	log.Printf("Error checking worker table existence: %v", err)
	assert.NoError(t, err, "Expected CheckTableExistance to not return an error")
	assert.True(t, exists, "Expected worker table to exist")
}

func TestCreateTable(t *testing.T) {
	database := helper.NewTestDatabase(port)

	workerDBHandler, err := NewWorkerDBHandler(database)
	assert.NoError(t, err, "Expected NewWorkerDBHandler to not return an error")

	err = workerDBHandler.CreateTable()
	assert.NoError(t, err, "Expected CreateTable to not return an error")
}

func TestDropTable(t *testing.T) {
	database := helper.NewTestDatabase(port)

	workerDBHandler, err := NewWorkerDBHandler(database)
	assert.NoError(t, err, "Expected NewWorkerDBHandler to not return an error")

	err = workerDBHandler.DropTable()
	assert.NoError(t, err, "Expected DropTable to not return an error")
}

func TestInsertWorker(t *testing.T) {
	database := helper.NewTestDatabase(port)

	workerDBHandler, err := NewWorkerDBHandler(database)
	assert.NoError(t, err, "Expected NewWorkerDBHandler to not return an error")

	worker, err := model.NewWorker("Worker", 1)
	require.NoError(t, err, "Expected NewWorker to not return an error")

	insertedWorker, err := workerDBHandler.InsertWorker(worker)
	assert.NoError(t, err, "Expected InsertWorker to not return an error")
	assert.NotNil(t, insertedWorker, "Expected InsertWorker to return a non-nil worker")
	assert.NotEqual(t, worker.RID, insertedWorker.RID, "Expected inserted worker RID to match")
	assert.Equal(t, worker.Name, insertedWorker.Name, "Expected inserted worker Name to match")
	assert.Equal(t, worker.Status, insertedWorker.Status, "Expected inserted worker Status to match")
	assert.WithinDuration(t, time.Now(), insertedWorker.CreatedAt, 1*time.Second, "Expected inserted worker CreatedAt time to match")
	assert.WithinDuration(t, time.Now(), insertedWorker.UpdatedAt, 1*time.Second, "Expected inserted worker UpdatedAt time to match")
}

func TestUpdateWorker(t *testing.T) {
	database := helper.NewTestDatabase(port)

	workerDBHandler, err := NewWorkerDBHandler(database)
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

	updatedWorker, err := workerDBHandler.UpdateWorker(insertedWorker)
	assert.NoError(t, err, "Expected UpdateWorker to not return an error")
	assert.Equal(t, insertedWorker.Name, updatedWorker.Name, "Expected updated worker Name to match")
	assert.Equal(t, insertedWorker.Options, updatedWorker.Options, "Expected updated worker Options to match")
}

func TestDeleteWorker(t *testing.T) {
	database := helper.NewTestDatabase(port)

	workerDBHandler, err := NewWorkerDBHandler(database)
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

func TestSelectWorker(t *testing.T) {
	database := helper.NewTestDatabase(port)

	workerDBHandler, err := NewWorkerDBHandler(database)
	assert.NoError(t, err, "Expected NewWorkerDBHandler to not return an error")

	worker, err := model.NewWorker("Worker", 1)
	require.NoError(t, err, "Expected NewWorker to not return an error")

	insertedWorker, err := workerDBHandler.InsertWorker(worker)
	require.NoError(t, err, "Expected InsertWorker to not return an error")

	selectedWorker, err := workerDBHandler.SelectWorker(insertedWorker.RID)
	assert.NoError(t, err, "Expected SelectWorker to not return an error")
	assert.NotNil(t, selectedWorker, "Expected SelectWorker to return a non-nil worker")
	assert.Equal(t, insertedWorker.RID, selectedWorker.RID, "Expected selected worker RID to match")
	assert.Equal(t, insertedWorker.Name, selectedWorker.Name, "Expected selected worker Name to match")
}

func TestSelectAllWorkers(t *testing.T) {
	database := helper.NewTestDatabase(port)

	workerDBHandler, err := NewWorkerDBHandler(database)
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

func TestSelectAllWorkersBySearch(t *testing.T) {
	database := helper.NewTestDatabase(port)

	workerDBHandler, err := NewWorkerDBHandler(database)
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
