package core

import (
	"context"
	"fmt"
	"sync"
)

type Listener[T any] struct {
	broadcaster *Broadcaster[T]
	waitgroup   *sync.WaitGroup
}

// NewListener creates a new Listener instance for the specified type T.
// It initializes a broadcaster and a waitgroup to manage concurrent processing of notifications.
// It returns a pointer to the new Listener instance.
func NewListener[T any](broadcaster *Broadcaster[T]) (*Listener[T], error) {
	if broadcaster == nil {
		return nil, fmt.Errorf("broadcaster cannot be nil")
	}

	return &Listener[T]{
		broadcaster: broadcaster,
		waitgroup:   &sync.WaitGroup{},
	}, nil
}

// Listen listens for notifications on the broadcaster's channel.
// It takes a context for cancellation, a ready channel to signal readiness,
// and a notifyFunction that will be called with the data received.
// The listener will process notifications in a separate goroutine to avoid blocking.
// If the context is done, it will stop listening and return.
// The ready channel is closed in the first for loop iteration to signal that the listener is ready.
func (l *Listener[T]) Listen(ctx context.Context, ready chan struct{}, notifyFunction func(data T)) {
	if notifyFunction == nil {
		return
	}

	channel := l.broadcaster.Subscribe()

	readySignaled := false
	for {
		select {
		case <-ctx.Done():
			if !readySignaled {
				close(ready)
			}
			return
		case data := <-channel:
			l.waitgroup.Add(1)

			if !readySignaled {
				readySignaled = true
				close(ready)
			}

			go func(d T) {
				defer l.waitgroup.Done()
				notifyFunction(d)
			}(data)
		default:
			if !readySignaled {
				readySignaled = true
				close(ready)
			}
		}
	}
}

// Notify sends a notification with the provided data to all listeners.
// It uses the broadcaster to broadcast the data to all subscribed channels.
// This method is typically called when an event occurs that needs to be communicated to all listeners.
// As Broadcast is not blocking it does not block and will not wait for listeners to process the notification.
func (l *Listener[T]) Notify(data T) {
	l.broadcaster.Broadcast(data)
}

// WaitForNotificationsProcessed waits for all notifications to be processed.
// It blocks until all goroutines that were started to process notifications have completed.
// This is useful to ensure that all notifications have been handled before proceeding with further operations.
// It is typically called after calling Listen and Notify to ensure that all processing is complete.
func (l *Listener[T]) WaitForNotificationsProcessed() {
	l.waitgroup.Wait()
}
