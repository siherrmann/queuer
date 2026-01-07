package queuer

import (
	"fmt"

	"github.com/google/uuid"
	"github.com/siherrmann/queuer/helper"
	"github.com/siherrmann/queuer/model"
)

// StopWorkerGracefully sets the status of the specified worker to 'STOPPING'
// to cancel running jobs when stopping.
func (q *Queuer) StopWorker(workerRid uuid.UUID) error {
	worker, err := q.GetWorker(workerRid)
	if err != nil {
		return helper.NewError("getting worker", err)
	}
	worker.Status = model.WorkerStatusStopped

	worker, err = q.dbWorker.UpdateWorker(worker)
	if err != nil {
		return helper.NewError("updating worker status to stopped", err)
	}

	// Update local worker object if this is the current queuer's worker
	if q.worker != nil && q.worker.RID == workerRid {
		q.workerMu.Lock()
		q.worker = worker
		q.workerMu.Unlock()
	}

	return nil
}

// StopWorkerGracefully sets the worker's status to STOPPING
// to allow it to finish current tasks before stopping.
func (q *Queuer) StopWorkerGracefully(workerRid uuid.UUID) error {
	worker, err := q.GetWorker(workerRid)
	if err != nil {
		return helper.NewError("getting worker", err)
	}
	worker.Status = model.WorkerStatusStopping

	worker, err = q.dbWorker.UpdateWorker(worker)
	if err != nil {
		return helper.NewError("updating worker status to stopping", err)
	}

	// Update local worker object if this is the current queuer's worker
	if q.worker != nil && q.worker.RID == workerRid {
		q.workerMu.Lock()
		q.worker = worker
		q.workerMu.Unlock()
	}

	return nil
}

// GetWorker retrieves a worker by its RID (Resource Identifier).
func (q *Queuer) GetWorker(workerRid uuid.UUID) (*model.Worker, error) {
	worker, err := q.dbWorker.SelectWorker(workerRid)
	if err != nil {
		return nil, helper.NewError("selecting worker", err)
	}

	return worker, nil
}

// GetWorkers retrieves a list of workers starting from the lastId and returning the specified number of entries.
func (q *Queuer) GetWorkers(lastId int, entries int) ([]*model.Worker, error) {
	if lastId < 0 {
		return nil, helper.NewError("lastId check", fmt.Errorf("lastId cannot be negative"))
	}
	if entries <= 0 {
		return nil, helper.NewError("entries check", fmt.Errorf("entries must be greater than zero"))
	}

	workers, err := q.dbWorker.SelectAllWorkers(lastId, entries)
	if err != nil {
		return nil, helper.NewError("selecting workers", err)
	}

	return workers, nil
}

// GetWorkersBySearch retrieves workers that match the given search term.
func (q *Queuer) GetWorkersBySearch(search string, lastId int, entries int) ([]*model.Worker, error) {
	workers, err := q.dbWorker.SelectAllWorkersBySearch(search, lastId, entries)
	if err != nil {
		return nil, helper.NewError("selecting workers by search", err)
	}

	return workers, nil
}

// GetAllConnections retrieves all connections from the database.
func (q *Queuer) GetConnections() ([]*model.Connection, error) {
	connections, err := q.dbWorker.SelectAllConnections()
	if err != nil {
		return nil, helper.NewError("selecting connections", err)
	}

	return connections, nil
}
