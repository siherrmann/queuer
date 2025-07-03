package queuer

import (
	"context"
	"queuer/model"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestListenForJobUpdate(t *testing.T) {
	q := newQueuerMock("testQueuer", 1)
	require.NotNil(t, q, "expected queuer to be initialized")
	require.NotNil(t, q.jobUpdateListener, "expected jobUpdateListener to be initialized")

	// Start the queuer
	ctx, cancel := context.WithCancel(context.Background())
	q.Start(ctx, cancel)

	// Use a channel to capture the notification
	data := &model.Job{RID: uuid.New()}
	notifyChannel := make(chan *model.Job)
	err := q.ListenForJobUpdate(func(d *model.Job) {
		notifyChannel <- d
	})
	require.NoError(t, err, "expected to successfully listen for job updates")

	// Notify the listener manually
	q.jobUpdateListener.Notify(data)

	// Check if the data was received
	receivedData := <-notifyChannel
	assert.NotNil(t, receivedData, "expected to receive job data")
	assert.Equal(t, data.RID, receivedData.RID, "expected to receive the same job RID")
}

func TestListenForJobDelete(t *testing.T) {
	q := newQueuerMock("testQueuer", 1)
	require.NotNil(t, q, "expected queuer to be initialized")
	require.NotNil(t, q.jobUpdateListener, "expected jobUpdateListener to be initialized")

	// Start the queuer
	ctx, cancel := context.WithCancel(context.Background())
	q.Start(ctx, cancel)

	// Use a channel to capture the notification
	data := &model.Job{RID: uuid.New()}
	notifyChannel := make(chan *model.Job)
	err := q.ListenForJobDelete(func(d *model.Job) {
		notifyChannel <- d
	})
	require.NoError(t, err, "expected to successfully listen for job deletions")

	// Notify the listener manually
	q.jobDeleteListener.Notify(data)

	// Check if the data was received
	receivedData := <-notifyChannel
	assert.NotNil(t, receivedData, "expected to receive job data")
	assert.Equal(t, data.RID, receivedData.RID, "expected to receive the same job RID")
}
