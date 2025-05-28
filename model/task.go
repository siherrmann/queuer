package model

import (
	"fmt"
	"queuer/helper"
	"reflect"
)

type Task struct {
	Task             interface{}
	Name             string
	InputParameters  []reflect.Type
	OutputParameters []reflect.Type
}

func NewTask(task interface{}) (*Task, error) {
	taskName, err := helper.GetFunctionName(task)
	if err != nil {
		return nil, fmt.Errorf("error getting task name: %v", err)
	}

	if len(taskName) == 0 || len(taskName) > 100 {
		return nil, fmt.Errorf("taskName must have a length between 1 and 100")
	}

	if reflect.ValueOf(task).Kind() != reflect.Func {
		return nil, fmt.Errorf("task must be a function, got %s", reflect.TypeOf(task).Kind())
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

func NewTaskWithName(task interface{}, taskName string) (*Task, error) {
	if len(taskName) == 0 || len(taskName) > 100 {
		return nil, fmt.Errorf("taskName must have a length between 1 and 100")
	}

	if reflect.ValueOf(task).Kind() != reflect.Func {
		return nil, fmt.Errorf("task must be a function, got %s", reflect.TypeOf(task).Kind())
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
