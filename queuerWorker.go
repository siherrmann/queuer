package queuer

import (
	"fmt"

	"github.com/google/uuid"
	"github.com/siherrmann/queuer/model"
)

// GetWorker retrieves a worker by its RID (Resource Identifier).
// It returns the worker if found, or an error if not.
func (q *Queuer) GetWorker(workerRid uuid.UUID) (*model.Worker, error) {
	worker, err := q.dbWorker.SelectWorker(workerRid)
	if err != nil {
		return nil, err
	}

	return worker, nil
}

// GetWorkers retrieves a list of workers starting from the lastId and returning the specified number of entries.
// It returns a slice of workers and an error if any occurs.
func (q *Queuer) GetWorkers(lastId int, entries int) ([]*model.Worker, error) {
	if lastId < 0 {
		return nil, fmt.Errorf("lastId cannot be negative, got %d", lastId)
	}
	if entries <= 0 {
		return nil, fmt.Errorf("entries must be greater than zero, got %d", entries)
	}

	workers, err := q.dbWorker.SelectAllWorkers(lastId, entries)
	if err != nil {
		return nil, err
	}

	return workers, nil
}

// GetAllConnections retrieves all connections from the database.
// It returns a slice of connections and an error if any occurs.
func (q *Queuer) GetConnections() ([]*model.Connection, error) {
	connections, err := q.dbWorker.SelectAllConnections()
	if err != nil {
		return nil, err
	}

	return connections, nil
}
