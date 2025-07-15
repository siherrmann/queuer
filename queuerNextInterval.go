package queuer

import (
	"slices"

	"github.com/siherrmann/queuer/helper"
	"github.com/siherrmann/queuer/model"
)

// AddNextIntervalFunc adds a NextIntervalFunc to the worker's available next interval functions.
// It takes a NextIntervalFunc and adds it to the worker's AvailableNextIntervalFuncs.
// The function name is derived from the NextIntervalFunc interface using helper.GetTaskNameFromInterface.
// The function is meant to be used before starting the Queuer to ensure that the worker has access to the function.
// It checks if the function is nil or already exists in the worker's available next interval functions.
//
// If the function is nil or already exists, it panics.
// It returns the updated worker after adding the function.
func (q *Queuer) AddNextIntervalFunc(nif model.NextIntervalFunc) *model.Worker {
	if nif == nil {
		q.log.Panicf("error adding next interval: NextIntervalFunc cannot be nil")
	}

	nifName, err := helper.GetTaskNameFromInterface(nif)
	if err != nil {
		q.log.Panicf("error getting function name: %v", err)
	}
	if slices.Contains(q.worker.AvailableNextIntervalFuncs, nifName) {
		q.log.Panicf("NextIntervalFunc with name %v already exists", nifName)
	}

	q.nextIntervalFuncs[nifName] = nif
	q.worker.AvailableNextIntervalFuncs = append(q.worker.AvailableNextIntervalFuncs, nifName)

	worker, err := q.dbWorker.UpdateWorker(q.worker)
	if err != nil {
		q.log.Panicf("error updating worker: %v", err)
	}

	q.log.Printf("NextInterval function added with name %v", nifName)

	return worker
}

// AddNextIntervalFuncWithName adds a NextIntervalFunc to
// the worker's available next interval functions with a specific name.
// It takes a NextIntervalFunc and a name, checks if the function is nil
// or already exists in the worker's available next interval functions,
// and adds it to the worker's AvailableNextIntervalFuncs.
//
// This function is useful when you want to add a NextIntervalFunc
// with a specific name that you control, rather than deriving it from the function itself.
// It ensures that the function is not nil and that the name does not already exist
// in the worker's available next interval functions.
//
// If the function is nil or already exists, it panics.
// It returns the updated worker after adding the function with the specified name.
func (q *Queuer) AddNextIntervalFuncWithName(nif model.NextIntervalFunc, name string) *model.Worker {
	if nif == nil {
		q.log.Panicf("NextIntervalFunc cannot be nil")
	}
	if slices.Contains(q.worker.AvailableNextIntervalFuncs, name) {
		q.log.Panicf("NextIntervalFunc with name %v already exists", name)
	}

	q.nextIntervalFuncs[name] = nif
	q.worker.AvailableNextIntervalFuncs = append(q.worker.AvailableNextIntervalFuncs, name)

	worker, err := q.dbWorker.UpdateWorker(q.worker)
	if err != nil {
		q.log.Panicf("error updating worker: %v", err)
	}

	q.log.Printf("NextInterval function added with name %v", name)

	return worker
}
