package queuer

import (
	"queuer/model"
)

func (q *Queuer) AddTask(task interface{}) {
	newTask, err := model.NewTask(task)
	if err != nil {
		q.log.Fatalf("error creating new task: %v", err)
	}

	q.tasks[newTask.Name] = newTask
	q.worker.AvailableTasks = append(q.worker.AvailableTasks, newTask.Name)

	// Update worker in DB
	_, err = q.dbWorker.UpdateWorker(q.worker)
	if err != nil {
		q.log.Fatalf("error updating worker: %v", err)
	}

	q.log.Printf("Task added with name %v", newTask.Name)
}
