package queuer

import (
	"log"
	"queue/database"
	"queue/helper"
	"queue/model"
	"reflect"
)

type Queuer struct {
	dbJob    *database.JobDBHandler
	dbWorker *database.WorkerDBHandler
	tasks    []*model.Task
}

func NewQueuer(workerQueue string, workerName string) *Queuer {
	dbConnection := helper.NewDatabase(
		"queuer",
		&helper.DatabaseConfiguration{
			Host:     helper.GetEnvVariable("DB_DATAMODEL_HOST"),
			Port:     helper.GetEnvVariable("DB_DATAMODEL_PORT"),
			Database: helper.GetEnvVariable("DB_DATAMODEL_DATABASE"),
			Username: helper.GetEnvVariable("DB_DATAMODEL_USERNAME"),
			Password: helper.GetEnvVariable("DB_DATAMODEL_PASSWORD"),
			Schema:   helper.GetEnvVariable("DB_DATAMODEL_SCHEMA"),
		},
	)

	dbJob := database.NewJobDBHandler(dbConnection)
	dbWorker := database.NewWorkerDBHandler(dbConnection)

	worker, err := dbWorker.InsertWorker(&model.Worker{
		QueueName: workerQueue,
		Name:      workerName,
	})
	if err != nil {
		log.Fatalf("failed to insert worker: %v", err)
	}
	log.Printf("worker %s created with RID %d", worker.Name, worker.RID)

	return &Queuer{
		dbJob:    dbJob,
		dbWorker: dbWorker,
	}
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

	newTask := &model.Task{
		Name:            taskName,
		InputParameters: inputParameters,
	}
	q.tasks = append(q.tasks, newTask)
}

func (q *Queuer) AddJob(parameters map[string]interface{}) error {
	newJob := &model.Job{
		Parameters: parameters,
	}
	job, err := q.dbJob.InsertJob(newJob)
	if err != nil {
		return err
	}
	log.Printf("job created with RID %d", job.RID)

	return nil
}
