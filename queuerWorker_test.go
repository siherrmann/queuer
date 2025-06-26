package queuer

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetWorker(t *testing.T) {
	testQueuer := newQueuerMock("TestQueuer", 100)

	t.Run("Successfully get worker created by the new queuer", func(t *testing.T) {
		t.Parallel()

		retrievedWorker, err := testQueuer.GetWorker(testQueuer.worker.RID)
		assert.NoError(t, err, "expected no error when getting worker")
		require.NotNil(t, retrievedWorker, "expected worker to be retrieved")
		assert.Equal(t, testQueuer.worker.RID, retrievedWorker.RID, "expected retrieved worker RID to match the original worker RID")
	})

	t.Run("Returns error for non-existent worker", func(t *testing.T) {
		t.Parallel()

		nonExistentWorkerRid := uuid.New()
		retrievedWorker, err := testQueuer.GetWorker(nonExistentWorkerRid)
		assert.Error(t, err, "expected error when getting non-existent worker")
		require.Nil(t, retrievedWorker, "expected no worker to be retrieved for non-existent RID")
	})
}

func TestGetWorkers(t *testing.T) {
	testQueuer := newQueuerMock("TestQueuer", 100)

	t.Run("Successfully get workers starting from the lastId", func(t *testing.T) {
		t.Parallel()

		workers, err := testQueuer.GetWorkers(0, 10)
		assert.NoError(t, err, "expected no error when getting workers")
		require.NotNil(t, workers, "expected workers to be retrieved")
		assert.Equal(t, len(workers), 1, "expected one worker to be retrieved")
	})

	t.Run("Returns error for invalid lastId", func(t *testing.T) {
		t.Parallel()

		workers, err := testQueuer.GetWorkers(-1, 10)
		require.Error(t, err, "expected error for invalid lastId")
		assert.Contains(t, err.Error(), "lastId cannot be negative, got -1", "expected error message for negative lastId")
		assert.Nil(t, workers, "expected no workers to be retrieved for invalid lastId")
	})

	t.Run("Returns error for invalid entries", func(t *testing.T) {
		t.Parallel()

		workers, err := testQueuer.GetWorkers(0, 0)
		require.Error(t, err, "expected error for invalid entries")
		assert.Contains(t, err.Error(), "entries must be greater than zero, got 0", "expected error message for zero entries")
		assert.Nil(t, workers, "expected no workers to be retrieved for zero entries")
	})

	t.Run("Return empty slice for non existant lastId", func(t *testing.T) {
		t.Parallel()

		workers, err := testQueuer.GetWorkers(1000, 10)
		assert.NoError(t, err, "expected no error when getting workers with non-existent lastId")
		require.Nil(t, workers, "expected no workers to be retrieved for non-existent lastId")
	})
}
