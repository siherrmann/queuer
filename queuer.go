package queuer

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"queuer/database"
	"queuer/helper"
	"queuer/model"
	"reflect"

	"github.com/google/uuid"
)

type Queuer struct {
	// Worker
	workerID  int
	workerRid uuid.UUID
	// DBs
	dbJob    *database.JobDBHandler
	dbWorker *database.WorkerDBHandler
	// Job listeners
	jobInsertListener *database.QueuerListener
	jobUpdateListener *database.QueuerListener
	jobDeleteListener *database.QueuerListener
	// Tasks
	tasks map[string]*model.Task
}

func NewQueuer(workerQueue string, workerName string) *Queuer {
	dbConfig := &helper.DatabaseConfiguration{
		Host:     helper.GetEnvVariable("QUEUER_DB_HOST"),
		Port:     helper.GetEnvVariable("QUEUER_DB_PORT"),
		Database: helper.GetEnvVariable("QUEUER_DB_DATABASE"),
		Username: helper.GetEnvVariable("QUEUER_DB_USERNAME"),
		Password: helper.GetEnvVariable("QUEUER_DB_PASSWORD"),
		Schema:   helper.GetEnvVariable("QUEUER_DB_SCHEMA"),
	}

	dbConnection := helper.NewDatabase(
		"queuer",
		dbConfig,
	)

	// DBs
	dbJob, err := database.NewJobDBHandler(dbConnection)
	if err != nil {
		log.Fatalf("failed to create job db handler: %v", err)
	}
	dbWorker, err := database.NewWorkerDBHandler(dbConnection)
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
		workerID:          worker.ID,
		workerRid:         worker.RID,
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

		count := 0
		go q.jobInsertListener.ListenToEvents(ctx, cancel, func(data string) error {
			var jobFromNotification *model.JobFromNotification
			err := json.Unmarshal([]byte(data), &jobFromNotification)
			if err != nil {
				return fmt.Errorf("error unmarshalling json: %v", err)
			}

			if jobFromNotification.Status == model.JobStatusQueued {
				count++
				err = q.RunJob(jobFromNotification.ToJob())
				if err != nil {
					return fmt.Errorf("error running job %v, count %v: %v", jobFromNotification.ID, count, err)
				}
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
}

func (q *Queuer) AddJob(taskName string, parameters ...interface{}) (*model.Job, error) {
	if _, ok := q.tasks[taskName]; !ok {
		return nil, fmt.Errorf("task %s not found", taskName)
	}

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

func (q *Queuer) RunJob(job *model.Job) error {
	if _, ok := q.tasks[job.TaskName]; !ok {
		return fmt.Errorf("task %s not found", job.TaskName)
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

	// Get the worker
	worker, err := q.dbWorker.SelectWorker(q.workerRid)
	if err != nil {
		return fmt.Errorf("error selecting worker: %v", err)
	}

	// Update job status to running with worker ID and RID
	job.WorkerID = worker.ID
	job.WorkerRID = worker.RID
	_, err = q.dbJob.UpdateJobInitial(job)
	if err != nil {
		return fmt.Errorf("error updating job status to running: %v", err)
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
	job, err = q.dbJob.UpdateJob(job)
	if err != nil {
		return fmt.Errorf("error updating job status to completed: %v", err)
	}

	log.Printf("Job finished with ID %v", job.ID)

	return nil
}
