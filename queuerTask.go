package queuer

import (
	"log"
	"queuer/model"
)

func (q *Queuer) AddTask(task interface{}) *model.Task {
	newTask, err := model.NewTask(task)
	if err != nil {
		log.Fatalf("error creating new task: %v", err)
	}

	q.tasks[newTask.Name] = newTask
	q.worker.AvailableTasks = append(q.worker.AvailableTasks, newTask.Name)

	// Update worker in DB
	_, err = q.dbWorker.UpdateWorker(q.worker)
	if err != nil {
		log.Fatalf("error updating worker: %v", err)
	}

	q.log.Printf("Task added with name %v", newTask.Name)

	return newTask
}

func (q *Queuer) AddTaskWithName(task interface{}, name string) *model.Task {
	newTask, err := model.NewTaskWithName(task, name)
	if err != nil {
		log.Fatalf("error creating new task: %v", err)
	}

	q.tasks[newTask.Name] = newTask
	q.worker.AvailableTasks = append(q.worker.AvailableTasks, newTask.Name)

	// Update worker in DB
	_, err = q.dbWorker.UpdateWorker(q.worker)
	if err != nil {
		log.Fatalf("error updating worker: %v", err)
	}

	q.log.Printf("Task added with name %v", newTask.Name)

	return newTask
}
