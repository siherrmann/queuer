package queuer

import (
	"fmt"
	"queuer/model"
)

func (q *Queuer) ListenForJobUpdate(notifyFunction func(data *model.Job)) error {
	if q == nil || q.ctx == nil {
		return fmt.Errorf("cannot listen with uninitialized or not running Queuer")
	}
	ready := make(chan struct{})
	go q.jobUpdateListener.Listen(q.ctx, ready, notifyFunction)
	<-ready
	return nil
}

func (q *Queuer) ListenForJobDelete(notifyFunction func(data *model.Job)) error {
	if q == nil || q.ctx == nil {
		return fmt.Errorf("cannot listen with uninitialized or not running Queuer")
	}
	ready := make(chan struct{})
	go q.jobDeleteListener.Listen(q.ctx, ready, notifyFunction)
	<-ready
	return nil
}
