package model

import (
	"fmt"
	"reflect"

	"github.com/siherrmann/queuer/helper"
)

// Task represents a job task with its function, name, input parameters, and output parameters.
// It is used to define a job that can be executed by the queuer system.
//
// Parameters:
// - Task is the function that will be executed as a job.
// - Name is the name of the task, which should be unique and descriptive.
// - InputParameters is a slice of reflect.Type representing the types of input parameters for the task.
type Task struct {
	Task             interface{}
	Name             string
	InputParameters  []reflect.Type
	OutputParameters []reflect.Type
}

func NewTask(task interface{}) (*Task, error) {
	taskName, err := helper.GetTaskNameFromFunction(task)
	if err != nil {
		return nil, helper.NewError("getting task name", err)
	}

	return NewTaskWithName(task, taskName)
}

func NewTaskWithName(task interface{}, taskName string) (*Task, error) {
	if len(taskName) == 0 || len(taskName) > 100 {
		return nil, helper.NewError("taskName check", fmt.Errorf("taskName must have a length between 1 and 100"))
	}

	err := helper.CheckValidTask(task)
	if err != nil {
		return nil, err
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

	return &Task{
		Task:             task,
		Name:             taskName,
		InputParameters:  inputParameters,
		OutputParameters: outputParameters,
	}, nil
}
