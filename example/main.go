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
	q := queuer.NewQueuer(
		"exampleWorker",
		3,
		&model.OnError{
			Timeout:      2,
			RetryDelay:   1,
			RetryBackoff: model.RETRY_BACKOFF_EXPONENTIAL,
			MaxRetries:   3,
		},
	)

	// Add the task to the queuer
	q.AddTask(MyTask)

	// Start the queuer
	q.Start(ctx)

	// Example adding a single job to the queue
	_, err := q.AddJob(MyTask, 5, "12")
	if err != nil {
		log.Fatalf("Error adding job: %v", err)
	}

	// Example adding multiple jobs to the queue
	batchedJobs := make([]model.BatchJob, 0, 10)
	for i := 0; i < 10; i++ {
		batchedJobs = append(batchedJobs, model.BatchJob{
			Task:       MyTask,
			Parameters: []interface{}{i, "12"},
		})
	}
	err = q.AddJobs(batchedJobs)
	if err != nil {
		log.Fatalf("Error adding jobs: %v", err)
	}

	// Example adding a single job with options
	// This job will timeout after 0.1 seconds
	options := &model.Options{
		OnError: &model.OnError{
			Timeout:      0.1,
			RetryDelay:   1,
			RetryBackoff: model.RETRY_BACKOFF_EXPONENTIAL,
			MaxRetries:   3,
		},
		Schedule: &model.Schedule{
			Start: time.Now().Add(time.Second * 10),
		},
	}
	_, err = q.AddJobWithOptions(MyTask, options, 5, "12")
	if err != nil {
		log.Fatalf("Error adding job with options: %v", err)
	}

	// Example adding a single job with schedule options
	options = &model.Options{
		Schedule: &model.Schedule{
			Start:    time.Now().Add(time.Second * 10),
			Interval: time.Second * 5,
			MaxCount: 10,
		},
	}
	_, err = q.AddJobWithOptions(MyTask, options, time.Now().Second(), "1")
	if err != nil {
		log.Fatalf("Error adding job with schedule options: %v", err)
	}

	// Wait for a while to let the jobs process
	<-ctx.Done()
	log.Println("Exiting...")
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
