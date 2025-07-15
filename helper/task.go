package helper

import (
	"fmt"
	"reflect"
	"runtime"
)

// CheckValidTask checks if the provided task is a valid function.
// It returns an error if the task is nil, not a function, or if its value is nil.
func CheckValidTask(task interface{}) error {
	if task == nil {
		return fmt.Errorf("task must not be nil")
	}
	if reflect.ValueOf(task).Kind() != reflect.Func {
		return fmt.Errorf("task must be a function, got %s", reflect.TypeOf(task).Kind())
	}
	if reflect.ValueOf(task).IsNil() {
		return fmt.Errorf("task value must not be nil")
	}

	return nil
}

// CheckValidTaskWithParameters checks if the provided task and parameters are valid.
// It checks if the task is a valid function and if the parameters match the task's input types.
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
		if !reflect.TypeOf(param).AssignableTo(taskType.In(i)) {
			return fmt.Errorf("parameter %d of task must be of type %s, got %s", i, taskType.In(i).Kind(), reflect.TypeOf(param).Kind())
		}
	}

	return nil
}

// GetTaskNameFromFunction retrieves the name of the function from the provided task.
// It checks if the task is a valid function and returns its name.
func GetTaskNameFromFunction(f interface{}) (string, error) {
	err := CheckValidTask(f)
	if err != nil {
		return "", err
	}

	return runtime.FuncForPC(reflect.ValueOf(f).Pointer()).Name(), nil
}

// GetTaskNameFromInterface retrieves the name of the task from the provided interface.
// It checks if the task is a string or a function and returns its name accordingly.
func GetTaskNameFromInterface(task interface{}) (string, error) {
	if taskNameString, ok := task.(string); ok {
		return taskNameString, nil
	}

	return GetTaskNameFromFunction(task)
}

// GetInputParametersFromTask retrieves the input parameters of the provided task.
// It checks if the task is a valid function and returns its input parameter types.
func GetInputParametersFromTask(task interface{}) ([]reflect.Type, error) {
	inputCount := reflect.TypeOf(task).NumIn()
	inputParameters := []reflect.Type{}
	for i := 0; i < inputCount; i++ {
		inputParameters = append(inputParameters, reflect.TypeOf(task).In(i))
	}

	return inputParameters, nil
}

// GetOutputParametersFromTask retrieves the output parameters of the provided task.
// It checks if the task is a valid function and returns its output parameter types.
func GetOutputParametersFromTask(task interface{}) ([]reflect.Type, error) {
	outputCount := reflect.TypeOf(task).NumOut()
	outputParameters := []reflect.Type{}
	for i := 0; i < outputCount; i++ {
		outputParameters = append(outputParameters, reflect.TypeOf(task).Out(i))
	}

	return outputParameters, nil
}
