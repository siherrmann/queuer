package helper

import (
	"fmt"
	"path"
	"runtime"
	"strings"
)

type Error struct {
	Original error
	Trace    []string
}

func (e Error) Error() string {
	return e.Original.Error() + " | Trace: " + fmt.Sprint(strings.Join(e.Trace, ", "))
}

func NewError(trace string, original error) Error {
	pc, _, _, ok := runtime.Caller(1)
	details := runtime.FuncForPC(pc)
	if ok && details != nil {
		functionName := path.Base(details.Name())
		trace = functionName + " - " + trace
	}

	if v, ok := original.(Error); ok {
		return Error{
			Original: v.Original,
			Trace:    append(v.Trace, trace),
		}
	}
	return Error{
		Original: original,
		Trace:    []string{trace},
	}
}
