package queuer

import (
	"fmt"

	"github.com/google/uuid"
	"github.com/siherrmann/queuer/helper"
	"github.com/siherrmann/queuer/model"
)

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
