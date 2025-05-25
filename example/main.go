package main

import (
	"context"
	"log"
	"queuer"
	"queuer/model"
	"strconv"
	"time"
)

func main() {
	// Context with cancel to manage the lifecycle of the example
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Example usage of the Queuer package
	q := queuer.NewQueuer("exampleWorker")

	// Add the task to the queuer
	q.AddTask(MyTask)

	// Start the queuer
	q.Start()

	// Example adding multiple jobs to the queue
	for i := 0; i < 10; i++ {
		_, err := q.AddJob(MyTask, []interface{}{5, "10"}...)
		if err != nil {
			log.Fatalf("Error adding job: %v", err)
		}
	}

	// Example adding a job with options
	// This job will timeout after 0.1 seconds
	_, err := q.AddJobWithOptions(MyTask, &model.Options{Timeout: 0.1}, 5, "12")
	if err != nil {
		log.Fatalf("Error adding job: %v", err)
	}

	// Wait for a while to let the jobs process
	<-ctx.Done()
	log.Println("All jobs processed, exiting...")
}

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
