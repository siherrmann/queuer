package queuer

import (
	"slices"

	"github.com/siherrmann/queuer/helper"
	"github.com/siherrmann/queuer/model"
)

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
