package queuer

import (
	"log"
	"queuer/model"
)

// AddTask adds a new task to the queuer.
// It creates a new task with the provided task interface, adds it to the worker's available tasks,
// and updates the worker in the database.
// The task name is automatically generated based on the task's function name (eg. main.TestTask).
// If the task creation fails, it logs a panic error and exits the program.
// It returns the newly created task.
func (q *Queuer) AddTask(task interface{}) *model.Task {
	newTask, err := model.NewTask(task)
	if err != nil {
		log.Panicf("error creating new task: %v", err)
	}

	q.tasks[newTask.Name] = newTask
	q.worker.AvailableTasks = append(q.worker.AvailableTasks, newTask.Name)

	// Update worker in DB
	_, err = q.dbWorker.UpdateWorker(q.worker)
	if err != nil {
		log.Panicf("error updating worker: %v", err)
	}

	q.log.Printf("Task added with name %v", newTask.Name)

	return newTask
}

// AddTaskWithName adds a new task with a specific name to the queuer.
// It creates a new task with the provided task interface and name, adds it to the worker's available tasks,
// and updates the worker in the database.
// If task creation fails, it logs a panic error and exits the program.
// It returns the newly created task.
func (q *Queuer) AddTaskWithName(task interface{}, name string) *model.Task {
	newTask, err := model.NewTaskWithName(task, name)
	if err != nil {
		log.Panicf("error creating new task: %v", err)
	}

	q.tasks[newTask.Name] = newTask
	q.worker.AvailableTasks = append(q.worker.AvailableTasks, newTask.Name)

	// Update worker in DB
	_, err = q.dbWorker.UpdateWorker(q.worker)
	if err != nil {
		log.Panicf("error updating worker: %v", err)
	}

	q.log.Printf("Task added with name %v", newTask.Name)

	return newTask
}
