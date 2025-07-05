package database

import (
	"context"
	"queuer/helper"
	"testing"
	"time"

	"github.com/lib/pq"
	"github.com/stretchr/testify/assert"
)

func TestNewQueuerDBListener(t *testing.T) {
	dbConfig := helper.NewTestDatabaseConfig(dbPort)
	listener, err := NewQueuerDBListener(dbConfig, "test_channel")
	assert.NoError(t, err, "Expected NewQueuerDBListener to not return an error")
	assert.NotNil(t, listener, "Expected listener to be created")
	assert.NotNil(t, listener.Listener, "Expected listener.Listener to be initialized")
	assert.Equal(t, "test_channel", listener.Channel, "Expected listener.Channel to match the provided channel name")

	err = listener.Listener.Close()
	assert.NoError(t, err, "Expected listener.Listener to close without error")
}

func TestListen(t *testing.T) {
	dbConfig := helper.NewTestDatabaseConfig(dbPort)
	listener, err := NewQueuerDBListener(dbConfig, "test_channel")
	assert.NoError(t, err, "Expected NewQueuerDBListener to not return an error")
	assert.NotNil(t, listener, "Expected listener to be created")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	notifyData := make(chan string)
	notifyFunction := func(data string) {
		notifyData <- data
	}

	go listener.Listen(ctx, cancel, notifyFunction)

	// Simulate a notification
	listener.Listener.Notify <- &pq.Notification{Extra: "test_data"}

	for {
		select {
		case data := <-notifyData:
			assert.Equal(t, "test_data", data, "Expected notification data to match")
			return
		case <-time.After(5 * time.Second):
			t.Fatal("Timeout waiting for notification")
		}
	}
}
