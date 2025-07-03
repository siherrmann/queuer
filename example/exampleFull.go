package main

import (
	"context"
	"log"
	"queuer"
	"queuer/model"
	"time"
)

func ExampleFull() {
	// Example usage of the Queuer package
	q := queuer.NewQueuer(
		"exampleWorker",
		3,
		&model.OnError{
			Timeout:      5,
			RetryDelay:   1,
			RetryBackoff: model.RETRY_BACKOFF_EXPONENTIAL,
			MaxRetries:   3,
		},
	)
	// Manually set the polling interval for job processing if needed
	q.JobPollInterval = 5 * time.Second

	// Add the tasks to the queuer
	q.AddTask(ShortTask)
	q.AddTask(LongTask)

	// Start the queuer
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	q.Start(ctx, cancel)

	// Example adding a single job to the queue
	_, err := q.AddJob(ShortTask, 5, "12")
	if err != nil {
		log.Fatalf("Error adding job: %v", err)
	}

	// Example adding multiple jobs to the queue
	batchedJobs := make([]model.BatchJob, 0, 10)
	for i := 0; i < 10; i++ {
		batchedJobs = append(batchedJobs, model.BatchJob{
			Task:       ShortTask,
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
			Start:    time.Now().Add(time.Second * 10),
			Interval: time.Second * 5,
		},
	}
	_, err = q.AddJobWithOptions(options, ShortTask, 5, "12")
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
	job, err := q.AddJobWithOptions(options, LongTask, time.Now().Second(), "1")
	if err != nil {
		log.Fatalf("Error adding job with schedule options: %v", err)
	}

	// Cancel the job after 1 second
	time.AfterFunc(1*time.Second, func() {
		_, err := q.CancelJob(job.RID)
		if err != nil {
			log.Printf("Error canceling job: %v", err)
		}
	})

	// Wait for a while to let the jobs process
	time.Sleep(1 * time.Minute)

	// Stop the queuer gracefully
	err = q.Stop()
	if err != nil {
		log.Printf("Error stopping queuer: %v", err)
	}

	// Wait for a while to let the jobs process
	<-ctx.Done()
	log.Println("Exiting...")
}
