package queuer

import (
	"fmt"

	"github.com/siherrmann/queuer/helper"
	"github.com/siherrmann/queuer/model"
)

// ListenForJobInsert listens for job insert events and notifies the provided function when a job is added.
func (q *Queuer) ListenForJobUpdate(notifyFunction func(data *model.Job)) error {
	if q == nil || q.ctx == nil {
		return helper.NewError("queuer check", fmt.Errorf("cannot listen with uninitialized or not running Queuer"))
	}

	outerReady := make(chan struct{})
	ready := make(chan struct{})
	go func() {
		close(outerReady)
		q.jobUpdateListener.Listen(q.ctx, ready, notifyFunction)
	}()

	<-ready
	<-outerReady
	return nil
}

// ListenForJobInsert listens for job insert events and notifies the provided function when a job is added.
func (q *Queuer) ListenForJobDelete(notifyFunction func(data *model.Job)) error {
	if q == nil || q.ctx == nil {
		return helper.NewError("queuer check", fmt.Errorf("cannot listen with uninitialized or not running Queuer"))
	}

	outerReady := make(chan struct{})
	ready := make(chan struct{})
	go func() {
		close(outerReady)
		q.jobDeleteListener.Listen(q.ctx, ready, notifyFunction)
	}()

	<-ready
	<-outerReady
	return nil
}
