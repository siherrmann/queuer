package helper

import (
	"fmt"
	"reflect"
	"runtime"
)

func GetTaskNameFromFunction(f interface{}) (string, error) {
	if reflect.ValueOf(f).Kind() != reflect.Func {
		return "", fmt.Errorf("task must be a function, got %s", reflect.TypeOf(f).Kind())
	}

	return runtime.FuncForPC(reflect.ValueOf(f).Pointer()).Name(), nil
}

func GetTaskNameFromInterface(task interface{}) (string, error) {
	if taskNameString, ok := task.(string); ok {
		return taskNameString, nil
	}

	return GetTaskNameFromFunction(task)
}

func CheckValidTask(task interface{}) error {
	if reflect.ValueOf(task).Kind() != reflect.Func {
		return fmt.Errorf("task must be a function, got %s", reflect.TypeOf(task).Kind())
	}

	return nil
}

func CheckValidTaskWithParameters(task interface{}, parameters ...interface{}) error {
	err := CheckValidTask(task)
	if err != nil {
		return err
	}

	taskType := reflect.TypeOf(task)
	if taskType.NumIn() != len(parameters) {
		return fmt.Errorf("task expects %d parameters, got %d", taskType.NumIn(), len(parameters))
	}

	for i, param := range parameters {
		if !taskType.In(i).AssignableTo(reflect.TypeOf(param)) {
			return fmt.Errorf("parameter %d of task must be of type %s, got %s", i, taskType.In(i).Kind(), reflect.TypeOf(param).Kind())
		}
	}

	return nil
}

func GetInputParametersFromTask(task interface{}) ([]reflect.Type, error) {
	inputCount := reflect.TypeOf(task).NumIn()
	inputParameters := []reflect.Type{}
	for i := 0; i < inputCount; i++ {
		inputParameters = append(inputParameters, reflect.TypeOf(task).In(i))
	}

	return inputParameters, nil
}

func GetOutputParametersFromTask(task interface{}) ([]reflect.Type, error) {
	outputCount := reflect.TypeOf(task).NumOut()
	outputParameters := []reflect.Type{}
	for i := 0; i < outputCount; i++ {
		outputParameters = append(outputParameters, reflect.TypeOf(task).Out(i))
	}

	return outputParameters, nil
}
