package main

import (
	"context"
	"log"
	"queuer"
)

func ExampleEasy() {
	// Create a new queuer instance
	q := queuer.NewQueuer("exampleWorker", 3, nil)

	// Add a short task to the queuer
	q.AddTask(ShortTask)

	// Start the queuer
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	q.Start(ctx, cancel)

	// Add a job to the queue
	_, err := q.AddJob(ShortTask, 5, "12")
	if err != nil {
		log.Fatalf("Error adding job: %v", err)
	}
}
