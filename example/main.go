package main

import (
	"context"
	"fmt"
	"log"
	"queuer"
	"strconv"
	"time"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Example usage of the Queuer package
	worker := queuer.NewQueuer("exampleQueue", "exampleWorker")

	// Add the task to the queuer
	worker.AddTask(myTask, MyTask)

	// Start the queuer
	worker.Start()

	// Example of how to add a task with parameters
	// IMPORTANT: This is not ensuring a correct order of execution,
	// normally you would use AddJob without concurrency.
	for i := 0; i < 10; i++ {
		go func() {
			_, err := worker.AddJob(myTask, 5, "10")
			if err != nil {
				log.Fatalf("Error adding job: %v", err)
			}
		}()
	}

	<-ctx.Done()
	fmt.Println("Example is done")
}

const (
	myTask = "myTask"
)

// Simple example task function
func MyTask(param1 int, param2 string) (int, error) {
	// Simulate some work
	time.Sleep(100 * time.Millisecond)

	// Example for some error handling
	param2Int, err := strconv.Atoi(param2)
	if err != nil {
		return 0, err
	}

	return param1 + param2Int, nil
}
