package main

import (
	"context"
	"log"
	"time"

	"github.com/siherrmann/queuer"
)

func ExampleEasy() {
	// Create a new queuer instance
	q := queuer.NewQueuer("exampleEasyWorker", 3)

	// Add a short task to the queuer
	q.AddTask(ShortTask)

	// Start the queuer
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	q.Start(ctx, cancel)

	// Add a job to the queue
	job, err := q.AddJob(ShortTask, 5, "12")
	if err != nil {
		log.Fatalf("Error adding job: %v", err)
	}

	job = q.WaitForJobFinished(job.RID, 30*time.Second)

	log.Printf("Job finished with status: %s", job.Status)

	// Stop the queuer gracefully
	err = q.Stop()
	if err != nil {
		log.Printf("Error stopping queuer: %v", err)
	}

	// Wait for a while to let the jobs process
	<-ctx.Done()
	log.Println("Exiting...")
}
