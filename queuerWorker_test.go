package queuer

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/siherrmann/queuer/helper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetWorker(t *testing.T) {
	helper.SetTestDatabaseConfigEnvs(t, dbPort)
	testQueuer := NewQueuer("TestQueuer", 100)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	testQueuer.StartWithoutWorker(ctx, cancel, true)

	t.Run("Successfully get worker created by the new queuer", func(t *testing.T) {
		retrievedWorker, err := testQueuer.GetWorker(testQueuer.worker.RID)
		assert.NoError(t, err, "expected no error when getting worker")
		require.NotNil(t, retrievedWorker, "expected worker to be retrieved")
		assert.Equal(t, testQueuer.worker.RID, retrievedWorker.RID, "expected retrieved worker RID to match the original worker RID")
	})

	t.Run("Returns error for non-existent worker", func(t *testing.T) {
		nonExistentWorkerRid := uuid.New()
		retrievedWorker, err := testQueuer.GetWorker(nonExistentWorkerRid)
		assert.Error(t, err, "expected error when getting non-existent worker")
		require.Nil(t, retrievedWorker, "expected no worker to be retrieved for non-existent RID")
	})
}

func TestGetWorkers(t *testing.T) {
	helper.SetTestDatabaseConfigEnvs(t, dbPort)
	testQueuer := NewQueuer("TestQueuer", 100)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	testQueuer.StartWithoutWorker(ctx, cancel, true)

	t.Run("Successfully get workers starting from the lastId", func(t *testing.T) {
		workers, err := testQueuer.GetWorkers(0, 10)
		assert.NoError(t, err, "expected no error when getting workers")
		require.NotNil(t, workers, "expected workers to be retrieved")
		assert.Equal(t, len(workers), 1, "expected one worker to be retrieved")
	})

	t.Run("Returns error for invalid lastId", func(t *testing.T) {
		workers, err := testQueuer.GetWorkers(-1, 10)
		require.Error(t, err, "expected error for invalid lastId")
		assert.Contains(t, err.Error(), "lastId cannot be negative", "expected error message for negative lastId")
		assert.Nil(t, workers, "expected no workers to be retrieved for invalid lastId")
	})

	t.Run("Returns error for invalid entries", func(t *testing.T) {
		workers, err := testQueuer.GetWorkers(0, 0)
		require.Error(t, err, "expected error for invalid entries")
		assert.Contains(t, err.Error(), "entries must be greater than zero", "expected error message for zero entries")
		assert.Nil(t, workers, "expected no workers to be retrieved for zero entries")
	})

	t.Run("Return empty slice for non existant lastId", func(t *testing.T) {
		workers, err := testQueuer.GetWorkers(1000, 10)
		assert.NoError(t, err, "expected no error when getting workers with non-existent lastId")
		require.Nil(t, workers, "expected no workers to be retrieved for non-existent lastId")
	})
}

func TestGetWorkersBySearch(t *testing.T) {
	helper.SetTestDatabaseConfigEnvs(t, dbPort)
	testQueuer := NewQueuer("TestQueuer", 100)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	testQueuer.StartWithoutWorker(ctx, cancel, true)

	t.Run("Successfully search for workers by name", func(t *testing.T) {
		// The queuer creates a worker with name "TestQueuer"
		workers, err := testQueuer.GetWorkersBySearch("TestQueuer", 0, 10)
		assert.NoError(t, err, "expected no error when searching for workers")
		require.NotNil(t, workers, "expected workers to be retrieved")
		assert.GreaterOrEqual(t, len(workers), 1, "expected at least one worker matching search term")
		assert.Equal(t, "TestQueuer", workers[0].Name, "expected worker name to match search term")
	})

	t.Run("Successfully search for workers by status", func(t *testing.T) {
		workers, err := testQueuer.GetWorkersBySearch("READY", 0, 10)
		assert.NoError(t, err, "expected no error when searching for workers by status")
		require.NotNil(t, workers, "expected workers to be retrieved")
		assert.GreaterOrEqual(t, len(workers), 1, "expected at least one worker with READY status")
	})

	t.Run("Returns empty result for non-matching search", func(t *testing.T) {
		workers, err := testQueuer.GetWorkersBySearch("NonExistentWorker", 0, 10)
		assert.NoError(t, err, "expected no error when searching with non-matching term")
		if workers != nil {
			assert.Len(t, workers, 0, "expected empty slice for non-matching search")
		}
	})
}

func TestGetConnections(t *testing.T) {
	helper.SetTestDatabaseConfigEnvs(t, dbPort)
	testQueuer := NewQueuer("TestQueuer", 100)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	testQueuer.StartWithoutWorker(ctx, cancel, true)

	t.Run("Successfully get connections", func(t *testing.T) {
		connections, err := testQueuer.GetConnections()
		assert.NoError(t, err, "expected no error when getting connections")
		require.NotNil(t, connections, "expected connections to be retrieved")
	})
}
