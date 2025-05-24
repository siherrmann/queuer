package queuer

import (
	"context"
	"fmt"
	"log"
	"queuer/database"
	"queuer/helper"
	"queuer/model"
	"reflect"
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
}

func NewQueuer(workerQueue string, workerName string) *Queuer {
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
		log.Fatalf("failed to create job db handler: %v", err)
	}
	dbWorker, err = database.NewWorkerDBHandler(dbConnection)
	if err != nil {
		log.Fatalf("failed to create worker db handler: %v", err)
	}

	// Job listeners
	jobInsertListener, err := database.NewQueuerListener(dbConfig, "job.INSERT")
	if err != nil {
		log.Fatalf("failed to create job insert listener: %v", err)
	}
	jobUpdateListener, err := database.NewQueuerListener(dbConfig, "job.UPDATE")
	if err != nil {
		log.Fatalf("failed to create job update listener: %v", err)
	}
	jobDeleteListener, err := database.NewQueuerListener(dbConfig, "job.DELETE")
	if err != nil {
		log.Fatalf("failed to create job delete listener: %v", err)
	}

	// Inserting worker
	worker, err := dbWorker.InsertWorker(&model.Worker{
		QueueName: workerQueue,
		Name:      workerName,
	})
	if err != nil {
		log.Fatalf("failed to insert worker: %v", err)
	}
	log.Printf("Worker %s created with RID %s", worker.Name, worker.RID.String())

	return &Queuer{
		worker:            worker,
		dbJob:             dbJob,
		dbWorker:          dbWorker,
		jobInsertListener: jobInsertListener,
		jobUpdateListener: jobUpdateListener,
		jobDeleteListener: jobDeleteListener,
		tasks:             map[string]*model.Task{},
	}
}

func (q *Queuer) Start() {
	if q.dbJob == nil || q.dbWorker == nil || q.jobInsertListener == nil || q.jobUpdateListener == nil || q.jobDeleteListener == nil {
		log.Fatal("worker is not initialized properly")
	}

	go func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go q.jobInsertListener.ListenToEvents(ctx, cancel, func(data string) error {
			err := q.RunJob()
			if err != nil {
				return fmt.Errorf("error running job: %v", err)
			}
			return nil
		})

		// go q.jobUpdateListener.ListenToEvents(ctx, cancel)
		// go q.jobDeleteListener.ListenToEvents(ctx, cancel)

		fmt.Println("Queuer started")

		<-ctx.Done()
		fmt.Println("Queuer stopped")
	}()
}

func (q *Queuer) AddTask(taskName string, task interface{}) {
	if reflect.ValueOf(task).Kind() != reflect.Func {
		log.Fatalf("task must be a function, got %s", reflect.TypeOf(task).Kind())
	}

	inputParameters := []reflect.Type{}
	inputCount := reflect.TypeOf(task).NumIn()
	for i := 0; i < inputCount; i++ {
		inputParameters = append(inputParameters, reflect.TypeOf(task).In(i))
	}

	outputParameters := []reflect.Type{}
	outputCount := reflect.TypeOf(task).NumOut()
	for i := 0; i < outputCount; i++ {
		outputParameters = append(outputParameters, reflect.TypeOf(task).Out(i))
	}

	q.tasks[taskName] = &model.Task{
		Task:             task,
		Name:             taskName,
		InputParameters:  inputParameters,
		OutputParameters: outputParameters,
	}
	q.worker.AvailableTasks = append(q.worker.AvailableTasks, taskName)

	// Update worker in DB
	_, err := q.dbWorker.UpdateWorker(q.worker)
	if err != nil {
		log.Fatalf("failed to update worker: %v", err)
	}
}

func (q *Queuer) AddJob(taskName string, parameters ...interface{}) (*model.Job, error) {
	newJob := &model.Job{
		TaskName:   taskName,
		Parameters: parameters,
	}
	job, err := q.dbJob.InsertJob(newJob)
	if err != nil {
		return nil, err
	}

	return job, nil
}

func (q *Queuer) RunJob() error {
	// Update job status to running with worker.
	job, err := q.dbJob.UpdateJobInitial(q.worker)
	if err != nil {
		return fmt.Errorf("error updating job status to running: %v", err)
	} else if job == nil {
		return nil
	}

	if len(job.Parameters) != len(q.tasks[job.TaskName].InputParameters) {
		return fmt.Errorf("task %s requires %d parameters, got %d", job.TaskName, len(q.tasks[job.TaskName].InputParameters), len(job.Parameters))
	}

	task := q.tasks[job.TaskName]
	taskFunc := reflect.ValueOf(task.Task)
	for i, param := range job.Parameters {
		// Convert json float to int if the parameter is int
		if task.InputParameters[i].Kind() == reflect.Int && reflect.TypeOf(param).Kind() == reflect.Float64 {
			job.Parameters[i] = int(param.(float64))
		} else if task.InputParameters[i].Kind() != reflect.TypeOf(param).Kind() {
			return fmt.Errorf("parameter %d of task %s must be of type %s, got %s", i, job.TaskName, task.InputParameters[i].Kind(), reflect.TypeOf(param).Kind())
		}
	}

	log.Printf("Job added with ID %v", job.ID)

	// Run the task function with the parameters
	results := taskFunc.Call(job.Parameters.ToReflectValues())
	resultValues := []interface{}{}
	for _, result := range results {
		resultValues = append(resultValues, result.Interface())
	}

	// Update job status to completed with results
	job.Status = model.JobStatusSucceeded
	job.Results = resultValues
	job, err = q.dbJob.UpdateJobFinal(job)
	if err != nil {
		return fmt.Errorf("error updating job status to completed: %v", err)
	}

	log.Printf("Job finished with ID %v", job.ID)

	return nil
}
