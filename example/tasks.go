package main

import (
	"strconv"
	"time"
)

// Short running example task function
func ShortTask(param1 int, param2 string) (int, error) {
	// Simulate some work
	time.Sleep(1 * time.Second)

	// Example for some error handling
	param2Int, err := strconv.Atoi(param2)
	if err != nil {
		return 0, err
	}

	return param1 + param2Int, nil
}

// Long running example task function
func LongTask(param1 int, param2 string) (int, error) {
	// Simulate some work
	time.Sleep(5 * time.Second)

	// Example for some error handling
	param2Int, err := strconv.Atoi(param2)
	if err != nil {
		return 0, err
	}

	return param1 + param2Int, nil
}
