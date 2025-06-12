package queuer

import (
	"context"
	"log"
	"os"
	"queuer/database"
	"queuer/helper"
	"queuer/model"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
)

var dbPort string

func TestMain(m *testing.M) {
	var teardown func(ctx context.Context, opts ...testcontainers.TerminateOption) error
	var err error
	teardown, dbPort, err = helper.MustStartPostgresContainer()
	if err != nil {
		log.Fatalf("error starting postgres container: %v", err)
	}

	m.Run()

	if teardown != nil && teardown(context.Background()) != nil {
		log.Fatalf("error tearing down postgres container: %v", err)
	}
}

func newQueuerMock(name string, maxConcurrency int, options ...*model.OnError) *Queuer {
	// Logger
	logger := log.New(os.Stdout, "Queuer: ", log.Ltime)

	// Database
	dbConfig := helper.NewTestDatabaseConfig(dbPort)
	dbConnection := helper.NewTestDatabase(dbConfig)

	// DBs
	var dbJob database.JobDBHandlerFunctions
	var dbWorker database.WorkerDBHandlerFunctions
	dbJob, err := database.NewJobDBHandler(dbConnection)
	if err != nil {
		logger.Fatalf("failed to create job db handler: %v", err)
	}
	dbWorker, err = database.NewWorkerDBHandler(dbConnection)
	if err != nil {
		logger.Fatalf("failed to create worker db handler: %v", err)
	}

	// Job listeners
	jobInsertListener, err := database.NewQueuerListener(dbConfig, "job.INSERT")
	if err != nil {
		logger.Fatalf("failed to create job insert listener: %v", err)
	}
	jobUpdateListener, err := database.NewQueuerListener(dbConfig, "job.UPDATE")
	if err != nil {
		logger.Fatalf("failed to create job update listener: %v", err)
	}
	jobDeleteListener, err := database.NewQueuerListener(dbConfig, "job.DELETE")
	if err != nil {
		logger.Fatalf("failed to create job delete listener: %v", err)
	}

	// Inserting worker
	var newWorker *model.Worker
	if len(options) > 0 {
		newWorker, err = model.NewWorkerWithOptions(name, maxConcurrency, options[0])
		if err != nil {
			logger.Fatalf("error creating new worker with options: %v", err)
		}
	} else {
		newWorker, err = model.NewWorker(name, maxConcurrency)
		if err != nil {
			logger.Fatalf("error creating new worker: %v", err)
		}
	}

	worker, err := dbWorker.InsertWorker(newWorker)
	if err != nil {
		logger.Fatalf("error inserting worker: %v", err)
	}
	logger.Printf("Worker %s created with RID %s", worker.Name, worker.RID.String())

	return &Queuer{
		worker:            worker,
		dbJob:             dbJob,
		dbWorker:          dbWorker,
		jobInsertListener: jobInsertListener,
		jobUpdateListener: jobUpdateListener,
		jobDeleteListener: jobDeleteListener,
		JobPollInterval:   1 * time.Minute,
		tasks:             map[string]*model.Task{},
		log:               logger,
	}
}
