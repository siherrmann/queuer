package main

import (
	"context"
	"log"
	"queuer"
)

func ExampleTx() {
	// Create a new queuer instance
	q := queuer.NewQueuer("exampleWorker", 3)

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

	_, err = q.AddJobTx(tx, ShortTask, 5, "12")
	if err != nil {
		log.Printf("Error adding job: %v", err)
		tx.Rollback()
	}
}
