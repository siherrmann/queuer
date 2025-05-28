package helper

func GetTaskNameFromInterface(task interface{}) (string, error) {
	if taskNameString, ok := task.(string); ok {
		return taskNameString, nil
	}
	return GetFunctionName(task)
}
