package core

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewListener(t *testing.T) {
	listener := NewListener[string]()
	require.NotNil(t, listener, "expected non-nil listener")
	assert.NotNil(t, listener.Channel, "expected non-nil channel")
}

func TestNotify(t *testing.T) {
	listener := NewListener[string]()
	data := "test data"

	// Use a channel to capture the notification
	notifyChannel := make(chan string)
	go func() {
		for d := range listener.Channel {
			notifyChannel <- d
		}
	}()

	listener.Notify(data)

	// Check if the data was received
	receivedData := <-notifyChannel
	assert.Equal(t, data, receivedData, "expected to receive the same data")
}

func TestListen(t *testing.T) {
	listener := NewListener[string]()
	data := "test data"

	t.Run("Successfully listen with valid function", func(t *testing.T) {
		notifyChannel := make(chan string)
		go listener.Listen(context.Background(), func(d string) {
			notifyChannel <- d
		})

		listener.Notify(data)

		// Check if the data was received
		receivedData := <-notifyChannel
		assert.Equal(t, data, receivedData, "expected to receive the same data")
	})

	t.Run("Context cancellation stops listening", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		notifyChannel := make(chan string)
		go listener.Listen(ctx, func(d string) {
			notifyChannel <- d
		})

		// Cancel the context to stop listening
		cancel()

		select {
		case <-notifyChannel:
			t.Error("expected no data to be received after context cancellation")
		default:
			// No data should be received, which is the expected behavior
		}
	})
}
