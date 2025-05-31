package helper

import "reflect"

func GetTaskNameFromInterface(task interface{}) (string, error) {
	if taskNameString, ok := task.(string); ok {
		return taskNameString, nil
	}

	return GetFunctionName(task)
}

func IsValidTask(task interface{}) bool {
	if reflect.ValueOf(task).Kind() != reflect.Func {
		return false
	}

	return true
}

func IsValidTaskWithParameters(task interface{}, parameters ...interface{}) bool {
	if !IsValidTask(task) {
		return false
	}

	taskType := reflect.TypeOf(task)
	if taskType.NumIn() != len(parameters) {
		return false
	}

	for i, param := range parameters {
		if !taskType.In(i).AssignableTo(reflect.TypeOf(param)) {
			return false
		}
	}

	return true
}
