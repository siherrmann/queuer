package database

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/lib/pq"
	"github.com/siherrmann/queuer/helper"
)

type QueuerListener struct {
	Listener *pq.Listener
	Channel  string
}

// NewQueuerDBListener creates a new QueuerListener instance.
// It initializes a PostgreSQL listener for the specified channel using the provided database configuration.
// The listener will automatically reconnect if the connection is lost, with a 10-second timeout and a 1-minute interval for reconnection attempts.
// If an error occurs during the creation of the listener, it returns an error.
// The listener will log any errors encountered during listening.
func NewQueuerDBListener(dbConfig *helper.DatabaseConfiguration, channel string) (*QueuerListener, error) {
	listener := pq.NewListener(dbConfig.DatabaseConnectionString(), 10*time.Second, time.Minute, func(ev pq.ListenerEventType, err error) {
		if err != nil {
			fmt.Printf("error creating postgres listener: %v", err)
		}
	})

	err := listener.Listen(channel)
	if err != nil {
		return nil, fmt.Errorf("error listening to channel %v: %v", channel, err)
	}

	return &QueuerListener{
		Listener: listener,
		Channel:  channel,
	}, nil
}

// Listen listens for events on the specified channel and processes them.
// It takes a context for cancellation, a cancel function to stop listening,
// and a notifyFunction that will be called with the event data when an event is received.
// The listener will check the connection every 90 seconds and will cancel the context if an error occurs during the ping.
// The notifyFunction will be called in a separate goroutine to avoid blocking the listener.
// If the context is done, the listener will stop listening and returns.
// It will log any errors encountered during the ping operation.
func (l *QueuerListener) Listen(ctx context.Context, cancel context.CancelFunc, notifyFunction func(data string)) {
	for {
		select {
		case <-ctx.Done():
			return
		case n := <-l.Listener.Notify:
			if n != nil {
				go notifyFunction(n.Extra)
			}
		case <-time.After(90 * time.Second):
			// Checking connection all 90 seconds
			err := l.Listener.Ping()
			if err != nil {
				log.Printf("Error pinging listener: %v", err)
				cancel()
				return
			}
		}
	}
}
