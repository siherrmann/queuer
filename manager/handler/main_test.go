package handler

import (
	"context"
	"errors"
	"log"
	"testing"
	"time"

	"github.com/siherrmann/queuer"
	"github.com/siherrmann/queuer/helper"
	"github.com/testcontainers/testcontainers-go"
)

var dbPort string
var queue *queuer.Queuer

func testTask(timeSeconds int) error {
	time.Sleep(time.Duration(timeSeconds) * time.Second)
	return nil
}

func testTaskFailing() error {
	return errors.New("task failed")
}

func TestMain(m *testing.M) {
	var teardown func(ctx context.Context, opts ...testcontainers.TerminateOption) error
	var err error
	teardown, dbPort, err = helper.MustStartTimescaleContainer()
	if err != nil {
		log.Fatalf("error starting postgres container: %v", err)
	}

	dbConf := &helper.DatabaseConfiguration{
		Host:          "localhost",
		Port:          dbPort,
		Database:      "database",
		Username:      "user",
		Password:      "password",
		Schema:        "public",
		SSLMode:       "disable",
		WithTableDrop: true,
	}

	queue = queuer.NewQueuerWithDB("TestQueuer", 10, "", dbConf)
	queue.AddTaskWithName(testTask, "test-task")
	queue.AddTaskWithName(testTaskFailing, "test-task-failing")

	ctx, cancel := context.WithCancel(context.Background())
	queue.Start(ctx, cancel)

	// Give queuer time to start
	time.Sleep(500 * time.Millisecond)

	exitCode := m.Run()

	// Stop the queuer
	cancel()
	queue.Stop()

	if teardown != nil {
		if err := teardown(context.Background()); err != nil {
			log.Fatalf("error tearing down postgres container: %v", err)
		}
	}

	// Exit with the test exit code
	if exitCode != 0 {
		log.Fatalf("tests failed with exit code: %d", exitCode)
	}
}
