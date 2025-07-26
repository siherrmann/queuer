package queuer

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/siherrmann/queuer/core"
	"github.com/siherrmann/queuer/database"
	"github.com/siherrmann/queuer/helper"
	"github.com/siherrmann/queuer/model"
)

// Queuer represents the main queuing system.
// It manages job scheduling, execution, and error handling.
// It provides methods to start, stop, and manage jobs and workers.
// It also handles database connections and listeners for job events.
type Queuer struct {
	DB               *sql.DB
	JobPollInterval  time.Duration
	RetentionArchive time.Duration
	// Context
	ctx    context.Context
	cancel context.CancelFunc
	// Runners
	activeRunners sync.Map
	// Logger
	log *log.Logger
	// Worker
	worker *model.Worker
	// DBs
	dbConfig *helper.DatabaseConfiguration
	dbJob    database.JobDBHandlerFunctions
	dbWorker database.WorkerDBHandlerFunctions
	dbMaster database.MasterDBHandlerFunctions
	// Job DB listeners
	jobDbListener        *database.QueuerListener
	jobArchiveDbListener *database.QueuerListener
	// Job listeners
	jobInsertListener *core.Listener[*model.Job]
	jobUpdateListener *core.Listener[*model.Job]
	jobDeleteListener *core.Listener[*model.Job]
	// Available functions
	tasks             map[string]*model.Task
	nextIntervalFuncs map[string]model.NextIntervalFunc
}

// NewQueuer creates a new Queuer instance with the given name and max concurrency.
// It wraps NewQueuerWithDB to initialize the queuer without an external db config.
func NewQueuer(name string, maxConcurrency int, options ...*model.OnError) *Queuer {
	return NewQueuerWithDB(name, maxConcurrency, nil, options...)
}

// NewQueuer creates a new Queuer instance with the given name and max concurrency.
// It initializes the database connection and worker.
// If options are provided, it creates a worker with those options.
// If any error occurs during initialization, it logs a panic error and exits the program.
// It returns a pointer to the newly created Queuer instance.
func NewQueuerWithDB(name string, maxConcurrency int, dbConfig *helper.DatabaseConfiguration, options ...*model.OnError) *Queuer {
	// Logger
	logger := log.New(os.Stdout, "Queuer: ", log.Ltime)

	// Database
	var err error
	var dbCon *helper.Database
	if dbConfig != nil {
		dbCon = helper.NewDatabase(
			"queuer",
			dbConfig,
		)
	} else {
		var err error
		dbConfig, err = helper.NewDatabaseConfiguration()
		if err != nil {
			logger.Panicf("failed to create database configuration: %v", err)
		}
		dbCon = helper.NewDatabase(
			"queuer",
			dbConfig,
		)
	}

	// DBs
	dbJob, err := database.NewJobDBHandler(dbCon, dbConfig.WithTableDrop)
	if err != nil {
		logger.Panicf("failed to create job db handler: %v", err)
	}
	dbWorker, err := database.NewWorkerDBHandler(dbCon, dbConfig.WithTableDrop)
	if err != nil {
		logger.Panicf("failed to create worker db handler: %v", err)
	}
	dbMaster, err := database.NewMasterDBHandler(dbCon, dbConfig.WithTableDrop)
	if err != nil {
		logger.Panicf("failed to create master db handler: %v", err)
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

	// Worker
	worker, err := dbWorker.InsertWorker(newWorker)
	if err != nil {
		logger.Panicf("error inserting worker: %v", err)
	}

	logger.Printf("Queuer %s created with worker RID: %v", newWorker.Name, worker.RID)

	return &Queuer{
		log:               logger,
		worker:            worker,
		DB:                dbCon.Instance,
		dbConfig:          dbConfig,
		dbJob:             dbJob,
		dbWorker:          dbWorker,
		dbMaster:          dbMaster,
		JobPollInterval:   1 * time.Minute,
		tasks:             map[string]*model.Task{},
		nextIntervalFuncs: map[string]model.NextIntervalFunc{},
	}
}

// Start starts the queuer by initializing the job listeners and starting the job poll ticker.
// It checks if the queuer is initialized properly, and if not, it logs a panic error and exits the program.
// It runs the job processing in a separate goroutine and listens for job events.
//
// Detailed steps include:
// 1. Create job and job archive database listeners.
// 2. Create broadcasters for job insert, update, and delete events.
// 3. Start the job listeners to listen for job events.
// 4. Start the job poll ticker to periodically check for new jobs.
// 5. Wait for the queuer to be ready or log a panic error if it fails to start within 5 seconds.
func (q *Queuer) Start(ctx context.Context, cancel context.CancelFunc, masterSettings ...*model.MasterSettings) {
	if ctx == nil || ctx == context.TODO() || cancel == nil {
		q.log.Panicln("ctx and cancel must be set")
	}

	q.ctx = ctx
	q.cancel = cancel

	// DB listeners
	var err error
	q.jobDbListener, err = database.NewQueuerDBListener(q.dbConfig, "job")
	if err != nil {
		q.log.Panicf("failed to create job insert listener: %v", err)
	}
	q.jobArchiveDbListener, err = database.NewQueuerDBListener(q.dbConfig, "job_archive")
	if err != nil {
		q.log.Panicf("failed to create job update listener: %v", err)
	}

	// Broadcasters for job updates and deletes
	broadcasterJobInsert := core.NewBroadcaster[*model.Job]("job.INSERT")
	q.jobInsertListener, err = core.NewListener(broadcasterJobInsert)
	if err != nil {
		q.log.Panicf("failed to create job insert listener: %v", err)
	}
	broadcasterJobUpdate := core.NewBroadcaster[*model.Job]("job.UPDATE")
	q.jobUpdateListener, err = core.NewListener(broadcasterJobUpdate)
	if err != nil {
		q.log.Panicf("failed to create job update listener: %v", err)
	}
	broadcasterJobDelete := core.NewBroadcaster[*model.Job]("job.DELETE")
	q.jobDeleteListener, err = core.NewListener(broadcasterJobDelete)
	if err != nil {
		q.log.Panicf("failed to create job update listener: %v", err)
	}

	// Start pollers
	ready := make(chan struct{})
	go func() {
		q.listen(ctx, cancel)

		err := q.pollJobTicker(ctx)
		if err != nil && ctx.Err() == nil {
			q.log.Printf("Error starting job poll ticker: %v", err)
			return
		}

		if len(masterSettings) > 0 && masterSettings[0] != nil {
			err = q.pollMasterTicker(ctx, masterSettings[0])
			if err != nil && ctx.Err() == nil {
				q.log.Printf("Error starting master poll ticker: %v", err)
				return
			}
		}

		close(ready)

		<-ctx.Done()
		q.log.Println("Queuer stopped")
	}()

	select {
	case <-ready:
		q.log.Println("Queuer started")
		return
	case <-time.After(5 * time.Second):
		q.log.Panicln("Queuer failed to start within 5 seconds")
	}
}

// Start starts the queuer by initializing the job listeners and starting the job poll ticker.
// It checks if the queuer is initialized properly, and if not, it logs a panic error and exits the program.
// It runs the job processing in a separate goroutine and listens for job events.
//
// This version does not run the job processing, allowing the queuer to be started without a worker.
// Is is useful if you want to run a queuer instance in a seperate service without a worker,
// for example to handle listening to job events and providing a central frontend.
func (q *Queuer) StartWithoutWorker(ctx context.Context, cancel context.CancelFunc, withoutListeners bool) {
	q.ctx = ctx
	q.cancel = cancel

	// Job listeners
	var err error
	if !withoutListeners {
		q.jobDbListener, err = database.NewQueuerDBListener(q.dbConfig, "job")
		if err != nil {
			q.log.Panicf("failed to create job insert listener: %v", err)
		}
		q.jobArchiveDbListener, err = database.NewQueuerDBListener(q.dbConfig, "job_archive")
		if err != nil {
			q.log.Panicf("failed to create job update listener: %v", err)
		}
	}

	// Broadcasters for job updates and deletes
	broadcasterJobInsert := core.NewBroadcaster[*model.Job]("job.INSERT")
	q.jobInsertListener, err = core.NewListener(broadcasterJobInsert)
	if err != nil {
		q.log.Panicf("failed to create job insert listener: %v", err)
	}
	broadcasterJobUpdate := core.NewBroadcaster[*model.Job]("job.UPDATE")
	q.jobUpdateListener, err = core.NewListener(broadcasterJobUpdate)
	if err != nil {
		q.log.Panicf("failed to create job update listener: %v", err)
	}
	broadcasterJobDelete := core.NewBroadcaster[*model.Job]("job.DELETE")
	q.jobDeleteListener, err = core.NewListener(broadcasterJobDelete)
	if err != nil {
		q.log.Panicf("failed to create job update listener: %v", err)
	}

	// Start job listeners
	ready := make(chan struct{})
	go func() {
		if !withoutListeners {
			q.listenWithoutRunning(ctx, cancel)
		}

		close(ready)

		<-ctx.Done()
		q.log.Println("Queuer stopped")
	}()

	select {
	case <-ready:
		q.log.Println("Queuer without worker started")
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

func (q *Queuer) listenWithoutRunning(ctx context.Context, cancel context.CancelFunc) {
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

func (q *Queuer) pollMasterTicker(ctx context.Context, masterSettings *model.MasterSettings) error {
	ctxInner, cancel := context.WithCancel(ctx)
	ticker, err := core.NewTicker(
		masterSettings.MasterPollInterval,
		func() {
			q.log.Println("Polling master...")
			master, err := q.dbMaster.UpdateMaster(q.worker, masterSettings)
			if err != nil {
				q.log.Printf("Error updating master: %v", err)
			}

			if master != nil {
				q.log.Printf("Worker %v is now the current master", q.worker.RID)
				err := q.masterTicker(ctx, master, masterSettings)
				if err != nil {
					q.log.Printf("Error starting master ticker: %v", err)
				} else {
					cancel()
				}
			}
		},
	)
	if err != nil {
		return fmt.Errorf("error creating ticker: %v", err)
	}

	q.log.Println("Starting master poll ticker...")
	go ticker.Go(ctxInner)

	return nil
}

func (q *Queuer) masterTicker(ctx context.Context, oldMaster *model.Master, masterSettings *model.MasterSettings) error {
	if oldMaster == nil {
		return fmt.Errorf("old master is nil")
	}

	if oldMaster.Settings.RetentionArchive == 0 {
		err := q.dbJob.AddRetentionArchive(masterSettings.RetentionArchive)
		if err != nil {
			return fmt.Errorf("error adding retention archive: %v", err)
		}
	} else if oldMaster.Settings.RetentionArchive != masterSettings.RetentionArchive {
		err := q.dbJob.RemoveRetentionArchive()
		if err != nil {
			return fmt.Errorf("error removing retention archive: %v", err)
		}

		err = q.dbJob.AddRetentionArchive(masterSettings.RetentionArchive)
		if err != nil {
			return fmt.Errorf("error adding retention archive: %v", err)
		}
	}

	ctxInner, cancel := context.WithCancel(ctx)
	ticker, err := core.NewTicker(
		masterSettings.MasterPollInterval,
		func() {
			master, err := q.dbMaster.UpdateMaster(q.worker, masterSettings)
			if err != nil {
				q.log.Printf("Error updating master: %v", err)
			}
			if master == nil {
				q.log.Printf("Worker %v is no longer the master", q.worker.RID)
				err := q.pollMasterTicker(ctx, masterSettings)
				if err != nil {
					q.log.Printf("Error restarting poll master ticker: %v", err)
				}
				cancel()
			}

			// Here we can add any additional logic that needs to run periodically while the worker is master.
			// This could include stale jobs, cleaning up the job database etc.
		},
	)
	if err != nil {
		return fmt.Errorf("error creating ticker: %v", err)
	}

	q.log.Println("Starting master poll ticker...")
	go ticker.Go(ctxInner)

	return nil
}
