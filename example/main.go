package main

import (
	"context"
	"fmt"
	"log"
	"queuer"
	"strconv"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Example usage of the Queuer package
	queuer := queuer.NewQueuer("exampleQueue", "exampleWorker")

	// Add the task to the queuer
	queuer.AddTask("myTask", MyTask)

	// Start the queuer
	queuer.Start()

	// Example of how to add a task with parameters
	for i := 0; i < 10; i++ {
		go func() {
			_, err := queuer.AddJob("myTask", 5, "10")
			if err != nil {
				log.Fatalf("Error adding job: %v", err)
			}
		}()
	}

	<-ctx.Done()
	fmt.Println("Example is done")
}

// Simple example task function
func MyTask(param1 int, param2 string) (int, error) {
	// Simulate some work
	// time.Sleep(100 * time.Millisecond)

	// Example for some error handling
	param2Int, err := strconv.Atoi(param2)
	if err != nil {
		return 0, err
	}

	return param1 + param2Int, nil
}
