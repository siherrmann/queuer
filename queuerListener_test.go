package queuer

import (
	"context"
	"queuer/helper"
	"queuer/model"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestListenForJobUpdate(t *testing.T) {
	helper.SetTestDatabaseConfigEnvs(t, dbPort)
	q := NewQueuer("testQueuer", 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	q.Start(ctx, cancel)

	data := &model.Job{RID: uuid.New()}
	notifyChannel := make(chan *model.Job, 1)
	err := q.ListenForJobUpdate(func(d *model.Job) {
		notifyChannel <- d
	})
	require.NoError(t, err, "expected to successfully listen for job updates")

	for i := 0; i < 100; i++ {
		// Notify the listener manually
		q.jobUpdateListener.Notify(data)

		q.jobUpdateListener.WaitForNotificationsProcessed()

		select {
		case receivedData := <-notifyChannel:
			assert.NotNil(t, receivedData, "expected to receive job data")
			assert.Equal(t, data.RID, receivedData.RID, "expected to receive the same job RID")
		case <-time.After(1 * time.Second):
			t.Error("timed out after 1s waiting for job data")
		}
	}
}

func TestListenForJobDelete(t *testing.T) {
	helper.SetTestDatabaseConfigEnvs(t, dbPort)
	q := NewQueuer("testQueuer", 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	q.Start(ctx, cancel)

	data := &model.Job{RID: uuid.New()}
	notifyChannel := make(chan *model.Job, 1)
	err := q.ListenForJobDelete(func(d *model.Job) {
		notifyChannel <- d
	})
	require.NoError(t, err, "expected to successfully listen for job deletions")

	for i := 0; i < 100; i++ {
		// Notify the listener manually
		q.jobDeleteListener.Notify(data)

		q.jobDeleteListener.WaitForNotificationsProcessed()

		select {
		case receivedData := <-notifyChannel:
			assert.NotNil(t, receivedData, "expected to receive job data")
			assert.Equal(t, data.RID, receivedData.RID, "expected to receive the same job RID")
		case <-time.After(1 * time.Second):
			t.Error("timed out after 5s waiting for job data")
		}
	}
}
