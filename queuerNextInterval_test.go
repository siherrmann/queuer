package queuer

import (
	"queuer/model"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func MockNextIntervalFunc1(start time.Time, currentCount int) time.Time {
	return start.Add(time.Hour * time.Duration(currentCount))
}

func MockNextIntervalFunc2(start time.Time, currentCount int) time.Time {
	return start.Add(time.Hour * time.Duration(currentCount))
}

func TestAddNextIntervalFunc(t *testing.T) {
	testQueuer := newQueuerMock("TestQueuer", 100)

	t.Run("Successfully adds NextIntervalFunc", func(t *testing.T) {
		worker := testQueuer.AddNextIntervalFunc(MockNextIntervalFunc1)
		require.NotNil(t, worker, "Expected worker to be returned after adding NextIntervalFunc")
		assert.Contains(t, testQueuer.worker.AvailableNextIntervalFuncs, "queuer.MockNextIntervalFunc1", "Expected NextIntervalFunc to be added to worker's AvailableNextIntervalFuncs")
		assert.Equal(t, 1, len(testQueuer.worker.AvailableNextIntervalFuncs), "Expected only one NextIntervalFunc to be added to worker's AvailableNextIntervalFuncs")
	})

	t.Run("Successfully adds multiple NextIntervalFuncs", func(t *testing.T) {
		// Reset the worker to ensure a clean state
		testQueuer.worker.AvailableNextIntervalFuncs = []string{}
		_, err := testQueuer.dbWorker.UpdateWorker(testQueuer.worker)
		require.NoError(t, err, "Expected no error when updating worker after resetting AvailableNextIntervalFuncs")

		testQueuer.AddNextIntervalFunc(MockNextIntervalFunc1)
		worker := testQueuer.AddNextIntervalFunc(MockNextIntervalFunc2)
		require.NotNil(t, worker, "Expected worker to be returned after adding NextIntervalFunc")
		assert.Contains(t, testQueuer.worker.AvailableNextIntervalFuncs, "queuer.MockNextIntervalFunc1", "Expected NextIntervalFunc to be added to worker's AvailableNextIntervalFuncs")
		assert.Contains(t, testQueuer.worker.AvailableNextIntervalFuncs, "queuer.MockNextIntervalFunc2", "Expected second NextIntervalFunc to be added to worker's AvailableNextIntervalFuncs")
		assert.Equal(t, 2, len(testQueuer.worker.AvailableNextIntervalFuncs), "Expected two NextIntervalFuncs to be added to worker's AvailableNextIntervalFuncs")
	})

	t.Run("Fails to add nil NextIntervalFunc", func(t *testing.T) {
		// Reset the worker to ensure a clean state
		testQueuer.worker.AvailableNextIntervalFuncs = []string{}
		_, err := testQueuer.dbWorker.UpdateWorker(testQueuer.worker)
		require.NoError(t, err, "Expected no error when updating worker after resetting AvailableNextIntervalFuncs")

		var worker *model.Worker
		defer func() {
			r := recover()
			require.NotNil(t, r, "Expected panic when adding nil NextIntervalFunc")
			require.Nil(t, worker, "Expected worker to be nil when adding nil NextIntervalFunc")
		}()

		worker = testQueuer.AddNextIntervalFunc(nil)
	})

	t.Run("Fails to add NextIntervalFunc with existing name", func(t *testing.T) {
		// Reset the worker to ensure a clean state
		testQueuer.worker.AvailableNextIntervalFuncs = []string{}
		_, err := testQueuer.dbWorker.UpdateWorker(testQueuer.worker)
		require.NoError(t, err, "Expected no error when updating worker after resetting AvailableNextIntervalFuncs")

		var worker *model.Worker
		defer func() {
			r := recover()
			require.NotNil(t, r, "Expected panic when adding NextIntervalFunc with existing name")
			require.Nil(t, worker, "Expected worker to be nil when adding NextIntervalFunc with existing name")
		}()

		testQueuer.AddNextIntervalFunc(MockNextIntervalFunc1)
		worker = testQueuer.AddNextIntervalFunc(MockNextIntervalFunc1)
	})
}

func TestAddNextIntervalFuncWithName(t *testing.T) {
	testQueuer := newQueuerMock("TestQueuer", 100)

	t.Run("Successfully adds NextIntervalFunc with name", func(t *testing.T) {
		worker := testQueuer.AddNextIntervalFuncWithName(MockNextIntervalFunc1, "CustomFuncName")
		require.NotNil(t, worker, "Expected worker to be returned after adding NextIntervalFunc with name")
		assert.Contains(t, testQueuer.worker.AvailableNextIntervalFuncs, "CustomFuncName", "Expected NextIntervalFunc to be added to worker's AvailableNextIntervalFuncs")
		assert.Equal(t, 1, len(testQueuer.worker.AvailableNextIntervalFuncs), "Expected only one NextIntervalFunc to be added to worker's AvailableNextIntervalFuncs")
	})

	t.Run("Fails to add nil NextIntervalFunc with name", func(t *testing.T) {
		var worker *model.Worker
		defer func() {
			r := recover()
			require.NotNil(t, r, "Expected panic when adding nil NextIntervalFunc with name")
			require.Nil(t, worker, "Expected worker to be nil when adding nil NextIntervalFunc with name")
		}()

		worker = testQueuer.AddNextIntervalFuncWithName(nil, "CustomFuncName")
	})

	t.Run("Fails to add NextIntervalFunc with existing name", func(t *testing.T) {
		var worker *model.Worker
		defer func() {
			r := recover()
			require.NotNil(t, r, "Expected panic when adding NextIntervalFunc with existing name")
			require.Nil(t, worker, "Expected worker to be nil when adding NextIntervalFunc with existing name")
		}()

		testQueuer.AddNextIntervalFuncWithName(MockNextIntervalFunc1, "CustomFuncName")
		worker = testQueuer.AddNextIntervalFuncWithName(MockNextIntervalFunc2, "CustomFuncName")
	})
}
