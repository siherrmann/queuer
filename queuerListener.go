package queuer

import (
	"fmt"
	"queuer/model"
)

func (q *Queuer) ListenForJobUpdate(notifyFunction func(data *model.Job)) error {
	if q == nil || q.ctx == nil {
		return fmt.Errorf("cannot listen with uninitialized or not running Queuer")
	}
	go q.jobUpdateListener.Listen(q.ctx, notifyFunction)
	return nil
}

func (q *Queuer) ListenForJobDelete(notifyFunction func(data *model.Job)) error {
	if q == nil || q.ctx == nil {
		return fmt.Errorf("cannot listen with uninitialized or not running Queuer")
	}
	go q.jobDeleteListener.Listen(q.ctx, notifyFunction)
	return nil
}
