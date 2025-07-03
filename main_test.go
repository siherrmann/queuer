package queuer

import (
	"context"
	"log"
	"os"
	"queuer/core"
	"queuer/database"
	"queuer/helper"
	"queuer/model"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
)

const maxDeviation = 50 * time.Millisecond

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

// newQueuerMock creates a new Queuer instance for testing purposes.
// It initializes a test database connection, job listeners and worker.
func newQueuerMock(name string, maxConcurrency int, options ...*model.OnError) *Queuer {
	// Logger
	logger := log.New(os.Stdout, "Queuer: ", log.Ltime)

	// Database
	dbConfig := helper.NewTestDatabaseConfig(dbPort)
	dbConnection := helper.NewTestDatabase(dbConfig)

	// DBs
	var dbJob database.JobDBHandlerFunctions
	var dbWorker database.WorkerDBHandlerFunctions
	dbJob, err := database.NewJobDBHandler(dbConnection, true)
	if err != nil {
		logger.Fatalf("failed to create job db handler: %v", err)
	}
	dbWorker, err = database.NewWorkerDBHandler(dbConnection, true)
	if err != nil {
		logger.Fatalf("failed to create worker db handler: %v", err)
	}

	// Job listeners
	jobInsertListener, err := database.NewQueuerDBListener(dbConfig, "job.INSERT")
	if err != nil {
		logger.Fatalf("failed to create job insert listener: %v", err)
	}

	// Broadcasters for job updates and deletes
	broadcasterJobUpdate := core.NewBroadcaster[*model.Job]("job.UPDATE")
	jobUpdateListener, err := core.NewListener(broadcasterJobUpdate)
	if err != nil {
		logger.Panicf("failed to create job update listener: %v", err)
	}
	broadcasterJobDelete := core.NewBroadcaster[*model.Job]("job.DELETE")
	jobDeleteListener, err := core.NewListener(broadcasterJobDelete)
	if err != nil {
		logger.Panicf("failed to create job update listener: %v", err)
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
		DB:                dbConnection.Instance,
		dbJob:             dbJob,
		dbWorker:          dbWorker,
		jobInsertListener: jobInsertListener,
		jobUpdateListener: jobUpdateListener,
		jobDeleteListener: jobDeleteListener,
		JobPollInterval:   1 * time.Minute,
		tasks:             map[string]*model.Task{},
		nextIntervalFuncs: map[string]model.NextIntervalFunc{},
		log:               logger,
	}
}
