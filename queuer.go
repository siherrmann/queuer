package queuer

import (
	"context"
	"database/sql"
	"encoding/json"
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
	ctx    context.Context
	cancel context.CancelFunc
	// Runners
	activeRunners sync.Map
	// Worker
	worker *model.Worker
	// DBs
	DB       *sql.DB
	dbJob    database.JobDBHandlerFunctions
	dbWorker database.WorkerDBHandlerFunctions
	// Job DB listeners
	jobDbListener        *database.QueuerListener
	jobArchiveDbListener *database.QueuerListener
	// Job listeners
	jobInsertListener *core.Listener[*model.Job]
	jobUpdateListener *core.Listener[*model.Job]
	jobDeleteListener *core.Listener[*model.Job]
	JobPollInterval   time.Duration
	// Available functions
	tasks             map[string]*model.Task
	nextIntervalFuncs map[string]model.NextIntervalFunc
	// Logger
	log *log.Logger
}

// NewQueuer creates a new Queuer instance with the given name and max concurrency.
// It initializes the database connection, job listeners and worker.
// If options are provided, it creates a worker with those options.
// If any error occurs during initialization, it logs a panic error and exits the program.
// It returns a pointer to the newly created Queuer instance.
func NewQueuer(name string, maxConcurrency int, options ...*model.OnError) *Queuer {
	// Logger
	logger := log.New(os.Stdout, "Queuer: ", log.Ltime)

	// Database
	dbConfig, err := helper.NewDatabaseConfiguration()
	if err != nil {
		logger.Panicf("failed to create database configuration: %v", err)
	}
	dbConnection := helper.NewDatabase(
		"queuer",
		dbConfig,
	)

	// DBs
	var dbJob database.JobDBHandlerFunctions
	var dbWorker database.WorkerDBHandlerFunctions
	dbJob, err = database.NewJobDBHandler(dbConnection, false)
	if err != nil {
		logger.Panicf("failed to create job db handler: %v", err)
	}
	dbWorker, err = database.NewWorkerDBHandler(dbConnection, false)
	if err != nil {
		logger.Panicf("failed to create worker db handler: %v", err)
	}

	// Job listeners
	jobDbListener, err := database.NewQueuerDBListener(dbConfig, "job")
	if err != nil {
		logger.Panicf("failed to create job insert listener: %v", err)
	}
	jobArchiveDbListener, err := database.NewQueuerDBListener(dbConfig, "job_archive")
	if err != nil {
		logger.Panicf("failed to create job update listener: %v", err)
	}

	// Broadcasters for job updates and deletes
	broadcasterJobInsert := core.NewBroadcaster[*model.Job]("job.INSERT")
	jobInsertListener, err := core.NewListener(broadcasterJobInsert)
	if err != nil {
		logger.Panicf("failed to create job insert listener: %v", err)
	}
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
			logger.Panicf("error creating new worker with options: %v", err)
		}
	} else {
		newWorker, err = model.NewWorker(name, maxConcurrency)
		if err != nil {
			logger.Panicf("error creating new worker: %v", err)
		}
	}

	worker, err := dbWorker.InsertWorker(newWorker)
	if err != nil {
		logger.Panicf("error inserting worker: %v", err)
	}

	logger.Printf("Queuer %s created with worker RID %s", worker.Name, worker.RID.String())

	return &Queuer{
		worker:               worker,
		DB:                   dbConnection.Instance,
		dbJob:                dbJob,
		dbWorker:             dbWorker,
		jobDbListener:        jobDbListener,
		jobArchiveDbListener: jobArchiveDbListener,
		jobInsertListener:    jobInsertListener,
		jobUpdateListener:    jobUpdateListener,
		jobDeleteListener:    jobDeleteListener,
		JobPollInterval:      1 * time.Minute,
		tasks:                map[string]*model.Task{},
		nextIntervalFuncs:    map[string]model.NextIntervalFunc{},
		log:                  logger,
	}
}

// NewQueuerWithoutWorker creates a new Queuer instance without a worker.
// This is useful for scenarios where the queuer needs to be initialized without a worker,
// such as when a seperate service is responsible for job status endpoints without processing jobs.
// It initializes the database connection and job listeners.
// If any error occurs during initialization, it logs a panic error and exits the program.
// It returns a pointer to the newly created Queuer instance.
func NewQueuerWithoutWorker() *Queuer {
	// Logger
	logger := log.New(os.Stdout, "Queuer: ", log.Ltime)

	// Database
	dbConfig, err := helper.NewDatabaseConfiguration()
	if err != nil {
		logger.Panicf("failed to create database configuration: %v", err)
	}
	dbConnection := helper.NewDatabase(
		"queuer",
		dbConfig,
	)

	// DBs
	var dbJob database.JobDBHandlerFunctions
	var dbWorker database.WorkerDBHandlerFunctions
	dbJob, err = database.NewJobDBHandler(dbConnection, false)
	if err != nil {
		logger.Panicf("failed to create job db handler: %v", err)
	}
	dbWorker, err = database.NewWorkerDBHandler(dbConnection, false)
	if err != nil {
		logger.Panicf("failed to create worker db handler: %v", err)
	}

	// Job listeners
	jobDbListener, err := database.NewQueuerDBListener(dbConfig, "job")
	if err != nil {
		logger.Panicf("failed to create job insert listener: %v", err)
	}
	jobArchiveDbListener, err := database.NewQueuerDBListener(dbConfig, "job_archive")
	if err != nil {
		logger.Panicf("failed to create job update listener: %v", err)
	}

	// Broadcasters for job updates and deletes
	broadcasterJobInsert := core.NewBroadcaster[*model.Job]("job.INSERT")
	jobInsertListener, err := core.NewListener(broadcasterJobInsert)
	if err != nil {
		logger.Panicf("failed to create job insert listener: %v", err)
	}
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

	logger.Println("Queuer without worker created")

	return &Queuer{
		DB:                   dbConnection.Instance,
		dbJob:                dbJob,
		dbWorker:             dbWorker,
		jobDbListener:        jobDbListener,
		jobArchiveDbListener: jobArchiveDbListener,
		jobInsertListener:    jobInsertListener,
		jobUpdateListener:    jobUpdateListener,
		jobDeleteListener:    jobDeleteListener,
		JobPollInterval:      1 * time.Minute,
		tasks:                map[string]*model.Task{},
		nextIntervalFuncs:    map[string]model.NextIntervalFunc{},
		log:                  logger,
	}
}

// Start starts the queuer by initializing the job listeners and starting the job poll ticker.
// It checks if the queuer is initialized properly, and if not, it logs a panic error and exits the program.
// It runs the job processing in a separate goroutine and listens for job events.
func (q *Queuer) Start(ctx context.Context, cancel context.CancelFunc) {
	if q.dbJob == nil || q.dbWorker == nil || q.jobDbListener == nil || q.jobUpdateListener == nil || q.jobDeleteListener == nil {
		q.log.Panicln("worker is not initialized properly")
	}

	q.ctx = ctx
	q.cancel = cancel

	ready := make(chan struct{})
	go func() {
		ctx, cancel := context.WithCancel(q.ctx)
		defer cancel()

		q.listen(ctx, cancel)

		err := q.pollJobTicker(ctx)
		if err != nil && ctx.Err() == nil {
			q.log.Printf("Error starting job poll ticker: %v", err)
			return
		}

		q.log.Println("Queuer started")
		close(ready)

		<-ctx.Done()
		q.log.Println("Queuer stopped")
	}()

	select {
	case <-ready:
		return
	case <-time.After(5 * time.Second):
		q.log.Panicln("Queuer failed to start within 5 seconds")
	}
}

// Stop stops the queuer by closing the job listeners, cancelling all queued and running jobs,
// and cancelling the context to stop the queuer.
func (q *Queuer) Stop() error {
	// Close db listeners
	if q.jobDbListener != nil {
		err := q.jobDbListener.Listener.Close()
		if err != nil {
			return fmt.Errorf("error closing job insert listener: %v", err)
		}
	}
	if q.jobArchiveDbListener != nil {
		err := q.jobArchiveDbListener.Listener.Close()
		if err != nil {
			return fmt.Errorf("error closing job update listener: %v", err)
		}
	}

	// Cancel all queued and running jobs
	err := q.CancelAllJobsByWorker(q.worker.RID, 100)
	if err != nil {
		return fmt.Errorf("error cancelling all jobs by worker: %v", err)
	}

	// Cancel the context to stop the queuer
	if q.ctx != nil {
		q.cancel()
	}

	q.log.Println("Queuer stopped")

	return nil
}

// Internal

// listen listens to job events and runs the initial job processing.
func (q *Queuer) listen(ctx context.Context, cancel context.CancelFunc) {
	readyJob := make(chan struct{})
	readyJobArchive := make(chan struct{})

	go func() {
		close(readyJob)
		q.jobDbListener.Listen(ctx, cancel, func(data string) {
			job := &model.JobFromNotification{}
			err := json.Unmarshal([]byte(data), job)
			if err != nil {
				q.log.Printf("Error unmarshalling job data: %v", err)
				return
			}

			if job.Status == model.JobStatusQueued || job.Status == model.JobStatusScheduled {
				q.jobInsertListener.Notify(job.ToJob())
				err = q.runJobInitial()
				if err != nil {
					q.log.Printf("Error running job: %v", err)
					return
				}
			} else {
				q.jobUpdateListener.Notify(job.ToJob())
			}
		})
	}()

	<-readyJob
	go func() {
		close(readyJobArchive)
		q.jobArchiveDbListener.Listen(ctx, cancel, func(data string) {
			job := &model.JobFromNotification{}
			err := json.Unmarshal([]byte(data), job)
			if err != nil {
				q.log.Printf("Error unmarshalling job data: %v", err)
				return
			}

			q.jobDeleteListener.Notify(job.ToJob())
		})
	}()

	<-readyJobArchive
}

func (q *Queuer) pollJobTicker(ctx context.Context) error {
	ticker, err := core.NewTicker(
		q.JobPollInterval,
		func() {
			q.log.Println("Polling jobs...")
			err := q.runJobInitial()
			if err != nil {
				q.log.Printf("Error running job: %v", err)
			}
		},
	)
	if err != nil {
		return fmt.Errorf("error creating ticker: %v", err)
	}

	q.log.Println("Starting job poll ticker...")
	go ticker.Go(ctx)

	return nil
}
