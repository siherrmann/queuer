package main

import (
	"log"
	"sync"
)

func main() {
	var wg sync.WaitGroup
	wg.Add(3)

	go func() {
		defer wg.Done()
		ExampleFull()
	}()
	go func() {
		defer wg.Done()
		ExampleTx()
	}()
	go func() {
		defer wg.Done()
		ExampleEasy()
	}()

	wg.Wait()
	log.Println("All examples finished")
}
