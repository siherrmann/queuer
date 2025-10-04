package queuer

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"os"
	"strings"
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
	log *slog.Logger
	// Worker
	worker   *model.Worker
	workerMu sync.RWMutex
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
// It wraps NewQueuerWithDB to initialize the queuer without an external db config and encryption key.
// The encryption key for the database is taken from an environment variable (QUEUER_ENCRYPTION_KEY),
// if not provided, it defaults to unencrypted results.
func NewQueuer(name string, maxConcurrency int, options ...*model.OnError) *Queuer {
	return NewQueuerWithDB(name, maxConcurrency, os.Getenv("QUEUER_ENCRYPTION_KEY"), nil, options...)
}

// NewQueuerWithDB creates a new Queuer instance with the given name and max concurrency.
// It initializes the database connection and worker.
// If options are provided, it creates a worker with those options.
//
// It takes the db configuration from environment variables if dbConfig is nil.
// - QUEUER_DB_HOST (required)
// - QUEUER_DB_PORT (required)
// - QUEUER_DB_DATABASE (required)
// - QUEUER_DB_USERNAME (required)
// - QUEUER_DB_PASSWORD (required)
// - QUEUER_DB_SCHEMA (required)
// - QUEUER_DB_SSLMODE (optional, defaults to "require")
//
// If the encryption key is empty, it defaults to unencrypted results.
//
// If any error occurs during initialization, it logs a panic error and exits the program.
// It returns a pointer to the newly created Queuer instance.
func NewQueuerWithDB(name string, maxConcurrency int, encryptionKey string, dbConfig *helper.DatabaseConfiguration, options ...*model.OnError) *Queuer {
	// Logger
	opts := helper.PrettyHandlerOptions{
		SlogOpts: slog.HandlerOptions{
			Level: slog.LevelInfo,
		},
	}
	logger := slog.New(helper.NewPrettyHandler(os.Stdout, opts))

	// Database
	var err error
	var dbCon *helper.Database
	if dbConfig != nil {
		dbCon = helper.NewDatabase(
			"queuer",
			dbConfig,
			logger,
		)
	} else {
		var err error
		dbConfig, err = helper.NewDatabaseConfiguration()
		if err != nil {
			log.Panicf("error creating database configuration: %s", err.Error())
		}
		dbCon = helper.NewDatabase(
			"queuer",
			dbConfig,
			logger,
		)
	}

	// DBs
	dbJob, err := database.NewJobDBHandler(dbCon, dbConfig.WithTableDrop)
	if err != nil {
		log.Panicf("error creating job db handler: %s", err.Error())
	}
	dbWorker, err := database.NewWorkerDBHandler(dbCon, dbConfig.WithTableDrop)
	if err != nil {
		log.Panicf("error creating worker db handler: %s", err.Error())
	}
	dbMaster, err := database.NewMasterDBHandler(dbCon, dbConfig.WithTableDrop)
	if err != nil {
		log.Panicf("error creating master db handler: %s", err.Error())
	}

	// Inserting worker
	var newWorker *model.Worker
	if len(options) > 0 {
		newWorker, err = model.NewWorkerWithOptions(name, maxConcurrency, options[0])
		if err != nil {
			log.Panicf("error creating new worker with options: %s", err.Error())
		}
	} else {
		newWorker, err = model.NewWorker(name, maxConcurrency)
		if err != nil {
			log.Panicf("error creating new worker: %s", err.Error())
		}
	}

	// Worker
	worker, err := dbWorker.InsertWorker(newWorker)
	if err != nil {
		log.Panicf("error inserting worker: %s", err.Error())
	}

	logger.Info("Queuer with worker created", slog.String("worker_name", newWorker.Name), slog.String("worker_rid", worker.RID.String()))

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

// NewStaticQueuer creates a new Queuer instance without a worker.
// It initializes the database connection and other necessary components.
// If any error occurs during initialization, it logs a panic error and exits the program.
// It returns a pointer to the newly created Queuer instance.
// This queuer instance does not listen to the db nor does it run jobs.
// It is primarily used for static operations like adding jobs, getting job status etc.
func NewStaticQueuer(logLevel slog.Leveler, dbConfig *helper.DatabaseConfiguration) *Queuer {
	// Logger
	opts := helper.PrettyHandlerOptions{
		SlogOpts: slog.HandlerOptions{
			Level: logLevel,
		},
	}
	logger := slog.New(helper.NewPrettyHandler(os.Stdout, opts))

	// Database
	var err error
	var dbCon *helper.Database
	if dbConfig != nil {
		dbCon = helper.NewDatabase(
			"queuer",
			dbConfig,
			logger,
		)
	} else {
		var err error
		dbConfig, err = helper.NewDatabaseConfiguration()
		if err != nil {
			log.Panicf("error creating database configuration: %s", err.Error())
		}
		dbCon = helper.NewDatabase(
			"queuer",
			dbConfig,
			logger,
		)
	}

	// DBs
	dbJob, err := database.NewJobDBHandler(dbCon, dbConfig.WithTableDrop)
	if err != nil {
		log.Panicf("error creating job db handler: %s", err.Error())
	}
	dbWorker, err := database.NewWorkerDBHandler(dbCon, dbConfig.WithTableDrop)
	if err != nil {
		log.Panicf("error creating worker db handler: %s", err.Error())
	}
	dbMaster, err := database.NewMasterDBHandler(dbCon, dbConfig.WithTableDrop)
	if err != nil {
		log.Panicf("error creating master db handler: %s", err.Error())
	}

	return &Queuer{
		log:               logger,
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
		panic("ctx and cancel must be set")
	}

	q.ctx = ctx
	q.cancel = cancel

	// DB listeners
	var err error
	q.jobDbListener, err = database.NewQueuerDBListener(q.dbConfig, "job")
	if err != nil {
		log.Panicf("error creating job insert listener: %s", err.Error())
	}
	q.log.Info("Added listener", slog.String("channel", "job"))
	q.jobArchiveDbListener, err = database.NewQueuerDBListener(q.dbConfig, "job_archive")
	if err != nil {
		log.Panicf("error creating job archive listener: %s", err.Error())
	}
	q.log.Info("Added listener", slog.String("channel", "job_archive"))

	// Broadcasters for job updates and deletes
	broadcasterJobInsert := core.NewBroadcaster[*model.Job]("job.INSERT")
	q.jobInsertListener, err = core.NewListener(broadcasterJobInsert)
	if err != nil {
		log.Panicf("error creating job insert listener: %s", err.Error())
	}
	broadcasterJobUpdate := core.NewBroadcaster[*model.Job]("job.UPDATE")
	q.jobUpdateListener, err = core.NewListener(broadcasterJobUpdate)
	if err != nil {
		log.Panicf("error creating job update listener: %s", err.Error())
	}
	broadcasterJobDelete := core.NewBroadcaster[*model.Job]("job.DELETE")
	q.jobDeleteListener, err = core.NewListener(broadcasterJobDelete)
	if err != nil {
		log.Panicf("error creating job delete listener: %s", err.Error())
	}

	// Update worker to running
	q.workerMu.Lock()
	q.worker.Status = model.WorkerStatusRunning
	q.worker, err = q.dbWorker.UpdateWorker(q.worker)
	q.workerMu.Unlock()
	if err != nil {
		log.Panicf("error updating worker status to running: %s", err.Error())
	}

	// Start pollers
	ready := make(chan struct{})
	go func() {
		q.listen(ctx, cancel)

		err := q.pollJobTicker(ctx)
		if err != nil && ctx.Err() == nil {
			q.log.Error("Error starting job poll ticker", slog.String("error", err.Error()))
			return
		}

		if len(masterSettings) > 0 && masterSettings[0] != nil {
			err = q.pollMasterTicker(ctx, masterSettings[0])
			if err != nil && ctx.Err() == nil {
				q.log.Error("Error starting master poll ticker", slog.String("error", err.Error()))
				return
			}
		}

		close(ready)

		<-ctx.Done()
		err = q.Stop()
		if err != nil {
			q.log.Error("Error stopping queuer", slog.String("error", err.Error()))
		}
	}()

	select {
	case <-ready:
		q.log.Info("Queuer started")
		return
	case <-time.After(5 * time.Second):
		q.log.Error("Queuer failed to start within 5 seconds")
	}
}

// Start starts the queuer by initializing the job listeners and starting the job poll ticker.
// It checks if the queuer is initialized properly, and if not, it logs a panic error and exits the program.
// It runs the job processing in a separate goroutine and listens for job events.
//
// This version does not run the job processing, allowing the queuer to be started without a worker.
// Is is useful if you want to run a queuer instance in a seperate service without a worker,
// for example to handle listening to job events and providing a central frontend.
func (q *Queuer) StartWithoutWorker(ctx context.Context, cancel context.CancelFunc, withoutListeners bool, masterSettings ...*model.MasterSettings) {
	q.ctx = ctx
	q.cancel = cancel

	// Job listeners
	var err error
	if !withoutListeners {
		q.jobDbListener, err = database.NewQueuerDBListener(q.dbConfig, "job")
		if err != nil {
			log.Panicf("error creating job insert listener: %s", err.Error())
		}
		q.log.Info("Added listener", slog.String("channel", "job"))
		q.jobArchiveDbListener, err = database.NewQueuerDBListener(q.dbConfig, "job_archive")
		if err != nil {
			log.Panicf("error creating job archive listener: %s", err.Error())
		}
		q.log.Info("Added listener", slog.String("channel", "job_archive"))
	}

	// Broadcasters for job updates and deletes
	broadcasterJobInsert := core.NewBroadcaster[*model.Job]("job.INSERT")
	q.jobInsertListener, err = core.NewListener(broadcasterJobInsert)
	if err != nil {
		log.Panicf("error creating job insert listener: %s", err.Error())
	}
	broadcasterJobUpdate := core.NewBroadcaster[*model.Job]("job.UPDATE")
	q.jobUpdateListener, err = core.NewListener(broadcasterJobUpdate)
	if err != nil {
		log.Panicf("error creating job update listener: %s", err.Error())
	}
	broadcasterJobDelete := core.NewBroadcaster[*model.Job]("job.DELETE")
	q.jobDeleteListener, err = core.NewListener(broadcasterJobDelete)
	if err != nil {
		log.Panicf("error creating job delete listener: %s", err.Error())
	}

	// Start job listeners
	ready := make(chan struct{})
	go func() {
		if !withoutListeners {
			q.listenWithoutRunning(ctx, cancel)
		}

		if len(masterSettings) > 0 && masterSettings[0] != nil {
			err = q.pollMasterTicker(ctx, masterSettings[0])
			if err != nil && ctx.Err() == nil {
				q.log.Error("Error starting master poll ticker", slog.String("error", err.Error()))
				return
			}
		}

		close(ready)

		<-ctx.Done()
		err := q.Stop()
		if err != nil {
			q.log.Error("Error stopping queuer", slog.String("error", err.Error()))
		}
	}()

	select {
	case <-ready:
		q.log.Info("Queuer without worker started")
		return
	case <-time.After(5 * time.Second):
		q.log.Error("Queuer failed to start within 5 seconds")
	}
}

// Stop stops the queuer by closing the job listeners, cancelling all queued and running jobs,
// and cancelling the context to stop the queuer.
func (q *Queuer) Stop() error {
	// Close db listeners
	if q.jobDbListener != nil {
		err := q.jobDbListener.Listener.Close()
		if err != nil && !strings.Contains(err.Error(), "Listener has been closed") {
			return fmt.Errorf("error closing job insert listener: %v", err)
		}
	}
	if q.jobArchiveDbListener != nil {
		err := q.jobArchiveDbListener.Listener.Close()
		if err != nil && !strings.Contains(err.Error(), "Listener has been closed") {
			return fmt.Errorf("error closing job archive listener: %v", err)
		}
	}

	// Update worker status to stopped
	var err error
	q.workerMu.Lock()
	q.worker.Status = model.WorkerStatusStopped
	q.worker, err = q.dbWorker.UpdateWorker(q.worker)
	workerRID := q.worker.RID
	q.workerMu.Unlock()
	if err != nil {
		return fmt.Errorf("error updating worker status to stopped: %v", err)
	}

	// Cancel all queued and running jobs
	err = q.CancelAllJobsByWorker(workerRID, 100)
	if err != nil {
		return fmt.Errorf("error cancelling all jobs by worker: %v", err)
	}

	// Cancel the context to stop the queuer
	if q.ctx != nil {
		q.cancel()
	}

	// Wait a moment for background goroutines to finish gracefully
	time.Sleep(100 * time.Millisecond)

	// Close database connection
	if q.DB != nil {
		q.log.Info("Closing database connection")
		err = q.DB.Close()
		if err != nil {
			q.log.Error("error closing database connection", slog.String("error", err.Error()))
		}
	}

	q.log.Info("Queuer stopped")

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
				q.log.Error("Error unmarshalling job data", slog.String("error", err.Error()))
				return
			}

			switch job.Status {
			case model.JobStatusQueued, model.JobStatusScheduled:
				q.jobInsertListener.Notify(job.ToJob())
				err = q.runJobInitial()
				if err != nil {
					q.log.Error("Error running job", slog.String("error", err.Error()))
					return
				}
			default:
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
				q.log.Error("Error unmarshalling job data", slog.String("error", err.Error()))
				return
			}

			switch job.Status {
			case model.JobStatusCancelled:
				runner, ok := q.activeRunners.Load(job.RID)
				if ok {
					q.log.Info("Canceling running job", slog.String("job_id", job.RID.String()))
					runner.(*core.Runner).Cancel()
					q.activeRunners.Delete(job.RID)
				}
			default:
				q.jobDeleteListener.Notify(job.ToJob())
			}
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
				q.log.Error("Error unmarshalling job data", slog.String("error", err.Error()))
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
				q.log.Error("Error unmarshalling job data", slog.String("error", err.Error()))
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
			q.log.Info("Polling jobs...")
			err := q.runJobInitial()
			if err != nil {
				q.log.Error("Error running job", slog.String("error", err.Error()))
			}
		},
	)
	if err != nil {
		return fmt.Errorf("error creating ticker: %v", err)
	}

	q.log.Info("Starting job poll ticker...")
	go ticker.Go(ctx)

	return nil
}

func (q *Queuer) pollMasterTicker(ctx context.Context, masterSettings *model.MasterSettings) error {
	ctxInner, cancel := context.WithCancel(ctx)
	ticker, err := core.NewTicker(
		masterSettings.MasterPollInterval,
		func() {
			q.log.Info("Polling master...")
			q.workerMu.RLock()
			worker := q.worker
			workerRID := q.worker.RID
			q.workerMu.RUnlock()

			master, err := q.dbMaster.UpdateMaster(worker, masterSettings)
			if err != nil {
				q.log.Error("Error updating master", slog.String("error", err.Error()))
			}

			if master != nil {
				q.log.Info("New master", slog.String("worker_id", workerRID.String()))
				err := q.masterTicker(ctx, master, masterSettings)
				if err != nil {
					q.log.Error("Error starting master ticker", slog.String("error", err.Error()))
				} else {
					cancel()
				}
			}
		},
	)
	if err != nil {
		return fmt.Errorf("error creating ticker: %v", err)
	}

	q.log.Info("Starting master poll ticker...")
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
			q.workerMu.RLock()
			worker := q.worker
			q.workerMu.RUnlock()

			_, err := q.dbMaster.UpdateMaster(worker, masterSettings)
			if err != nil {
				err := q.pollMasterTicker(ctx, masterSettings)
				if err != nil {
					q.log.Error("Error restarting poll master ticker", slog.String("error", err.Error()))
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

	q.log.Info("Starting master poll ticker...")
	go ticker.Go(ctxInner)

	return nil
}
