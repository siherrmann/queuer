package queuer

import (
	"fmt"
	"log/slog"
	"slices"

	"github.com/siherrmann/queuer/model"
)

// AddTask adds a new task to the queuer.
// It creates a new task with the provided task interface, adds it to the worker's available tasks,
// and updates the worker in the database.
// The task name is automatically generated based on the task's function name (eg. main.TestTask).
//
// If the task creation fails, it logs a panic error and exits the program.
// It returns the newly created task.
func (q *Queuer) AddTask(task interface{}) *model.Task {
	newTask, err := model.NewTask(task)
	if err != nil {
		panic(fmt.Sprintf("error creating new task: %s", err.Error()))
	}
	if slices.Contains(q.worker.AvailableTasks, newTask.Name) {
		panic(fmt.Sprintf("task already exists: %s", newTask.Name))
	}

	q.tasks[newTask.Name] = newTask
	q.worker.AvailableTasks = append(q.worker.AvailableTasks, newTask.Name)

	// Update worker in DB
	_, err = q.dbWorker.UpdateWorker(q.worker)
	if err != nil {
		panic(fmt.Sprintf("error updating worker: %s", err.Error()))
	}

	q.log.Info("Task added", slog.String("task_name", newTask.Name))

	return newTask
}

// AddTaskWithName adds a new task with a specific name to the queuer.
// It creates a new task with the provided task interface and name, adds it to the worker's available tasks,
// and updates the worker in the database.
//
// If task creation fails, it logs a panic error and exits the program.
// It returns the newly created task.
func (q *Queuer) AddTaskWithName(task interface{}, name string) *model.Task {
	newTask, err := model.NewTaskWithName(task, name)
	if err != nil {
		panic(fmt.Sprintf("error creating new task: %s", err.Error()))
	}
	if slices.Contains(q.worker.AvailableTasks, name) {
		panic(fmt.Sprintf("task already exists: %s", newTask.Name))
	}

	q.tasks[newTask.Name] = newTask
	q.worker.AvailableTasks = append(q.worker.AvailableTasks, newTask.Name)

	// Update worker in DB
	_, err = q.dbWorker.UpdateWorker(q.worker)
	if err != nil {
		panic(fmt.Sprintf("error updating worker: %s", err.Error()))
	}

	q.log.Info("Task added", slog.String("name", newTask.Name))

	return newTask
}
