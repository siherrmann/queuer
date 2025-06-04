package queuer

import (
	"context"
	"fmt"
	"log"
	"os"
	"queuer/core"
	"queuer/database"
	"queuer/helper"
	"queuer/model"
	"sync"
	"time"
)

type Queuer struct {
	// Context
	ctx context.Context
	// Runners
	activeRunners sync.Map
	// Worker
	worker *model.Worker
	// DBs
	dbJob    database.JobDBHandlerFunctions
	dbWorker database.WorkerDBHandlerFunctions
	// Job listeners
	jobInsertListener *database.QueuerListener
	jobUpdateListener *database.QueuerListener
	jobDeleteListener *database.QueuerListener
	// Tasks
	tasks map[string]*model.Task
	// Logger
	log *log.Logger
}

func NewQueuer(name string, maxConcurrency int, options ...*model.OnError) *Queuer {
	// Logger
	logger := log.New(os.Stdout, "Queuer: ", log.Ltime)

	// Database
	dbConfig, err := helper.NewDatabaseConfiguration()
	if err != nil {
		logger.Fatalf("failed to create database configuration: %v", err)
	}
	dbConnection := helper.NewDatabase(
		"queuer",
		dbConfig,
	)

	// DBs
	var dbJob database.JobDBHandlerFunctions
	var dbWorker database.WorkerDBHandlerFunctions
	dbJob, err = database.NewJobDBHandler(dbConnection)
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
		tasks:             map[string]*model.Task{},
		log:               logger,
	}
}

func (q *Queuer) Start(ctx context.Context) {
	if q.dbJob == nil || q.dbWorker == nil || q.jobInsertListener == nil || q.jobUpdateListener == nil || q.jobDeleteListener == nil {
		q.log.Fatalln("worker is not initialized properly")
	}

	q.ctx = ctx

	go func() {
		ctx, cancel := context.WithCancel(q.ctx)
		defer cancel()

		go q.listen(ctx, cancel)

		err := q.pollJobTicker(ctx)
		if err != nil {
			q.log.Printf("error starting job poll ticker: %v", err)
			return
		}

		q.log.Println("Queuer started")

		<-ctx.Done()
		q.log.Println("Queuer stopped")
	}()
}

func (q *Queuer) Stop() {
	if q.jobInsertListener != nil {
		err := q.jobInsertListener.Listener.Close()
		if err != nil {
			q.log.Printf("error closing job insert listener: %v", err)
		}
	}
	if q.jobUpdateListener != nil {
		err := q.jobUpdateListener.Listener.Close()
		if err != nil {
			q.log.Printf("error closing job update listener: %v", err)
		}
	}
	if q.jobDeleteListener != nil {
		err := q.jobDeleteListener.Listener.Close()
		if err != nil {
			q.log.Printf("error closing job delete listener: %v", err)
		}
	}

	q.log.Println("Queuer stopped")
}

// Internal

// listen listens to job events and runs the initial job processing.
func (q *Queuer) listen(ctx context.Context, cancel context.CancelFunc) {
	go q.jobInsertListener.ListenToEvents(ctx, cancel, func(data string) {
		q.log.Printf("Job insert event received: %s", data)
		err := q.runJobInitial()
		if err != nil {
			q.log.Printf("error running job: %v", err)
		}
	})

	// go q.jobUpdateListener.ListenToEvents(ctx, cancel)
	// go q.jobDeleteListener.ListenToEvents(ctx, cancel)
}

func (q *Queuer) pollJobTicker(ctx context.Context) error {
	ticker, err := core.NewTicker(
		5*time.Minute,
		func() {
			log.Printf("Job poll ticker ticked at %s", time.Now().Format(time.RFC3339))
			err := q.runJobInitial()
			if err != nil {
				q.log.Printf("error running job: %v", err)
			}
		},
	)
	if err != nil {
		return fmt.Errorf("error creating ticker: %v", err)
	}

	q.log.Println("Starting job poll ticker...")
	err = ticker.Go(ctx)
	if err != nil {
		return fmt.Errorf("error starting ticker: %v", err)
	}
	return nil
}
