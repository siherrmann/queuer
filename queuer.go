package queuer

import (
	"context"
	"log"
	"os"
	"queuer/database"
	"queuer/helper"
	"queuer/model"
)

type Queuer struct {
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

func NewQueuer(name string) *Queuer {
	// Logger
	logger := log.New(os.Stdout, "Queuer: ", log.Ltime)

	dbConfig := &helper.DatabaseConfiguration{
		Host:     helper.GetEnvVariableWithoutDelete("QUEUER_DB_HOST"),
		Port:     helper.GetEnvVariableWithoutDelete("QUEUER_DB_PORT"),
		Database: helper.GetEnvVariableWithoutDelete("QUEUER_DB_DATABASE"),
		Username: helper.GetEnvVariableWithoutDelete("QUEUER_DB_USERNAME"),
		Password: helper.GetEnvVariableWithoutDelete("QUEUER_DB_PASSWORD"),
		Schema:   helper.GetEnvVariableWithoutDelete("QUEUER_DB_SCHEMA"),
	}

	dbConnection := helper.NewDatabase(
		"queuer",
		dbConfig,
	)

	// DBs
	var err error
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
	newWorker, err := model.NewWorker(name, 10)
	if err != nil {
		logger.Fatalf("error creating new worker: %v", err)
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

func (q *Queuer) Start() {
	if q.dbJob == nil || q.dbWorker == nil || q.jobInsertListener == nil || q.jobUpdateListener == nil || q.jobDeleteListener == nil {
		q.log.Fatalln("worker is not initialized properly")
	}

	go func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go q.jobInsertListener.ListenToEvents(ctx, cancel, func(data string) {
			err := q.RunJob()
			if err != nil {
				q.log.Printf("error running job: %v", err)
			}
		})

		// go q.jobUpdateListener.ListenToEvents(ctx, cancel)
		// go q.jobDeleteListener.ListenToEvents(ctx, cancel)

		q.log.Println("Queuer started")

		<-ctx.Done()
		q.log.Println("Queuer stopped")
	}()
}

func (q *Queuer) AddTask(task interface{}) {
	newTask, err := model.NewTask(task)
	if err != nil {
		q.log.Fatalf("error creating new task: %v", err)
	}

	q.tasks[newTask.Name] = newTask
	q.worker.AvailableTasks = append(q.worker.AvailableTasks, newTask.Name)

	// Update worker in DB
	_, err = q.dbWorker.UpdateWorker(q.worker)
	if err != nil {
		q.log.Fatalf("error updating worker: %v", err)
	}

	q.log.Printf("Task added with name %v", newTask.Name)
}
