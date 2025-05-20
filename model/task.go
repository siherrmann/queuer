package model

import "reflect"

type Task struct {
	Name            string
	InputParameters []reflect.Type
}
