package helper

import (
	"fmt"
	"reflect"
	"runtime"
)

func GetFunctionName(f interface{}) (string, error) {
	if reflect.ValueOf(f).Kind() != reflect.Func {
		return "", fmt.Errorf("task must be a function, got %s", reflect.TypeOf(f).Kind())
	}
	return runtime.FuncForPC(reflect.ValueOf(f).Pointer()).Name(), nil
}
