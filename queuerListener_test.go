package queuer

import (
	"context"
	"queuer/model"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestListenForJobUpdate(t *testing.T) {
	q := newQueuerMock("testQueuer", 1)
	ctx, cancel := context.WithCancel(context.Background())
	q.Start(ctx, cancel)

	data := &model.Job{RID: uuid.New()}
	notifyChannel := make(chan *model.Job)
	err := q.ListenForJobUpdate(func(d *model.Job) {
		notifyChannel <- d
	})
	require.NoError(t, err, "expected to successfully listen for job updates")

	time.Sleep(1 * time.Second)

	// Notify the listener manually
	q.jobUpdateListener.Notify(data)

	select {
	case receivedData := <-notifyChannel:
		assert.NotNil(t, receivedData, "expected to receive job data")
		assert.Equal(t, data.RID, receivedData.RID, "expected to receive the same job RID")
	default:
		t.Error("expected to receive job data, but got nothing")
	}
}

func TestListenForJobDelete(t *testing.T) {
	q := newQueuerMock("testQueuer", 1)
	ctx, cancel := context.WithCancel(context.Background())
	q.Start(ctx, cancel)

	data := &model.Job{RID: uuid.New()}
	notifyChannel := make(chan *model.Job)
	err := q.ListenForJobDelete(func(d *model.Job) {
		notifyChannel <- d
	})
	require.NoError(t, err, "expected to successfully listen for job deletions")

	time.Sleep(1 * time.Second)

	// Notify the listener manually
	q.jobDeleteListener.Notify(data)

	select {
	case receivedData := <-notifyChannel:
		assert.NotNil(t, receivedData, "expected to receive job data")
		assert.Equal(t, data.RID, receivedData.RID, "expected to receive the same job RID")
	default:
		t.Error("expected to receive job data, but got nothing")
	}
}
