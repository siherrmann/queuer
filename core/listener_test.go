package core

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewListener(t *testing.T) {
	broadcaster := NewBroadcaster[string]("testBroadcaster")
	listener, err := NewListener(broadcaster)
	require.NoError(t, err, "expected no error when creating a new listener")
	require.NotNil(t, listener, "expected non-nil listener")
}

func TestNotifyAndListen(t *testing.T) {
	broadcaster := NewBroadcaster[string]("testBroadcaster")
	listener, err := NewListener(broadcaster)
	require.NoError(t, err, "expected no error when creating a new listener")
	require.NotNil(t, listener, "expected non-nil listener")

	data := "test data"
	notifyChannel := make(chan string, 1) // Buffer the channel to prevent blocking
	ready := make(chan struct{})

	for i := 0; i < 100; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			listener.Listen(ctx, ready, func(data string) {
				notifyChannel <- data
			})
		}()

		// Wait for the listener to be ready and notify
		<-ready
		listener.Notify(data)

		// Check if the data was received
		receivedData := <-notifyChannel
		assert.Equal(t, data, receivedData, "expected to receive the same data")

		cancel()
		ready = make(chan struct{})
	}
}

func TestListen(t *testing.T) {
	broadcaster := NewBroadcaster[string]("testBroadcaster")
	listener, err := NewListener(broadcaster)
	require.NoError(t, err, "expected no error when creating a new listener")
	require.NotNil(t, listener, "expected non-nil listener")

	data := "test data"

	t.Run("Successfully listen with valid function", func(t *testing.T) {
		notifyChannel := make(chan string)
		ready := make(chan struct{})
		go listener.Listen(context.Background(), ready, func(d string) {
			notifyChannel <- d
		})

		// Wait for the listener to be ready and notify
		<-ready
		listener.Notify(data)

		// Check if the data was received
		receivedData := <-notifyChannel
		assert.Equal(t, data, receivedData, "expected to receive the same data")
	})

	t.Run("Context cancellation stops listening", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		notifyChannel := make(chan string)
		ready := make(chan struct{})
		go listener.Listen(ctx, ready, func(d string) {
			notifyChannel <- d
		})

		// Wait for the listener to be ready and cancel
		<-ready
		cancel()

		select {
		case <-notifyChannel:
			t.Error("expected no data to be received after context cancellation")
		default:
			// No data should be received, which is the expected behavior
		}
	})
}
