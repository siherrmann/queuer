package database

import (
	"context"
	"fmt"
	"log"
	"queuer/helper"
	"time"

	"github.com/lib/pq"
)

type QueuerListener struct {
	Listener *pq.Listener
	Channel  string
}

// NewQueuerDBListener creates a new QueuerListener instance.
func NewQueuerDBListener(dbConfig *helper.DatabaseConfiguration, channel string) (*QueuerListener, error) {
	listener := pq.NewListener(dbConfig.DatabaseConnectionString(), 10*time.Second, time.Minute, func(ev pq.ListenerEventType, err error) {
		if err != nil {
			fmt.Printf("error creating postgres listener: %v", err)
		}
	})

	if err := listener.Listen(channel); err != nil {
		return nil, fmt.Errorf("error listening to channel %v: %v", channel, err)
	}

	log.Println("Added listener to channel: ", channel)

	return &QueuerListener{
		Listener: listener,
		Channel:  channel,
	}, nil
}

// Listen listens for events on the specified channel and processes them.
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
