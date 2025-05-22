package model

import "reflect"

type Task struct {
	Task             interface{}
	Name             string
	InputParameters  []reflect.Type
	OutputParameters []reflect.Type
}
