package main

import (
	"context"
	"log"
	"time"

	"github.com/siherrmann/queuer"
)

func ExampleTx() {
	// Create a new queuer instance
	q := queuer.NewQueuer("exampleTxWorker", 3)

	// Add a short task to the queuer
	q.AddTask(ShortTask)

	// Start the queuer
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	q.Start(ctx, cancel)

	// Add a job to the queue
	tx, err := q.DB.Begin()
	if err != nil {
		log.Fatalf("Error starting transaction: %v", err)
	}

	// Do something with the transaction
	// eg. `data, err := dataDB.InsertData(tx, "some data")`

	job, err := q.AddJobTx(tx, ShortTask, 5, "12")
	if err != nil {
		log.Printf("Error adding job: %v", err)
		err = tx.Rollback()
		if err != nil {
			log.Printf("Error rolling back transaction: %v", err)
		}
	}

	err = tx.Commit()
	if err != nil {
		log.Printf("Error committing transaction: %v", err)
		err = tx.Rollback()
		if err != nil {
			log.Printf("Error rolling back transaction: %v", err)
		}
	}

	// Wait for job to finish for stopping the queuer
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
