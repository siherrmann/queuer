package main

import (
	"log"
	"queuer"
	"queuer/model"
	"strconv"
	"time"
)

func main() {
	// Example usage of the Queuer package
	q := queuer.NewQueuer("exampleQueue", "exampleWorker")

	// Add the task to the queuer
	q.AddTask(myTask, MyTask)

	// Start the queuer
	q.Start()

	// Example of how to add a task with parameters
	for i := 0; i < 10; i++ {
		_, err := q.AddJob(myTask, 5, "10")
		if err != nil {
			log.Fatalf("Error adding job: %v", err)
		}
	}

	_, err := q.AddJobWithOptions(myTask, &model.Options{Timeout: 0.1}, 5, "12")
	if err != nil {
		log.Fatalf("Error adding job: %v", err)
	}
}

const (
	myTask = "myTask"
)

// Simple example task function
func MyTask(param1 int, param2 string) (int, error) {
	// Simulate some work
	time.Sleep(1 * time.Second)

	// Example for some error handling
	param2Int, err := strconv.Atoi(param2)
	if err != nil {
		return 0, err
	}

	return param1 + param2Int, nil
}
