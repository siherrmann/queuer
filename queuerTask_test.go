package queuer

import (
	"queuer/model"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAddTask(t *testing.T) {
	testQueuer := newQueuerMock("TestQueuer", 100)

	t.Run("Successfully add task", func(t *testing.T) {
		task := func() {}
		newTask := testQueuer.AddTask(task)
		require.NotNil(t, newTask, "expected task to be created")
		assert.Equal(t, "queuer.TestAddTask.func1.1", newTask.Name, "expected task name to match the function name")

		updatedWorker, err := testQueuer.GetWorker(testQueuer.worker.RID)
		require.NoError(t, err, "expected no error when getting updated worker")
		assert.Equal(t, 1, len(updatedWorker.AvailableTasks), "expected worker to have one available task")
		assert.Contains(t, updatedWorker.AvailableTasks, newTask.Name, "expected task to be added to worker's available tasks")
	})

	t.Run("Panics on nil task", func(t *testing.T) {
		var newTask *model.Task
		defer func() {
			r := recover()
			assert.NotNil(t, r, "expected panic for nil task")
			assert.Nil(t, newTask, "expected no task to be created for nil task")
		}()

		var task func()
		newTask = testQueuer.AddTask(task)
	})
}

func TestAddTaskWithName(t *testing.T) {
	testQueuer := newQueuerMock("TestQueuer", 100)

	t.Run("Successfully add task with name", func(t *testing.T) {
		task := func() {}
		newTask := testQueuer.AddTaskWithName(task, "CustomTaskName")
		require.NotNil(t, newTask, "expected task to be created")
		assert.Equal(t, "CustomTaskName", newTask.Name, "expected task name to match the provided name")

		updatedWorker, err := testQueuer.GetWorker(testQueuer.worker.RID)
		require.NoError(t, err, "expected no error when getting updated worker")
		assert.Equal(t, 1, len(updatedWorker.AvailableTasks), "expected worker to have one available task")
		assert.Contains(t, updatedWorker.AvailableTasks, newTask.Name, "expected task to be added to worker's available tasks")
	})

	t.Run("Panics on nil task with name", func(t *testing.T) {
		var newTask *model.Task
		defer func() {
			r := recover()
			assert.NotNil(t, r, "expected panic for nil task with name")
			assert.Nil(t, newTask, "expected no task to be created for nil task")
		}()

		var task func()
		newTask = testQueuer.AddTaskWithName(task, "CustomTaskName")
	})

	t.Run("Panics on empty task name", func(t *testing.T) {
		var newTask *model.Task
		defer func() {
			r := recover()
			assert.NotNil(t, r, "expected panic for empty task name")
			assert.Nil(t, newTask, "expected no task to be created for empty task name")
		}()

		task := func() {}
		newTask = testQueuer.AddTaskWithName(task, "")
	})
}
