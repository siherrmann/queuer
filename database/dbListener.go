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

// NewQueuerListener creates a new QueuerListener instance.
func NewQueuerListener(dbConfig *helper.DatabaseConfiguration, channel string) (*QueuerListener, error) {
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

// ListenToEvents listens for events on the specified channel and processes them.
func (l *QueuerListener) ListenToEvents(ctx context.Context, cancel context.CancelFunc, notifyFunction func(data string) error) {
	for {
		select {
		case <-ctx.Done():
			log.Println("Context done, stopping listener.")
			l.Listener.Close()
			return
		case n := <-l.Listener.Notify:
			err := notifyFunction(n.Extra)
			if err != nil {
				log.Printf("Error processing %v: %v", n.Channel, err)
			}
		case <-time.After(90 * time.Second):
			// Checking connection all 90 seconds
			err := l.Listener.Ping()
			if err != nil {
				fmt.Println("Error pinging listener: ", err)
				cancel()
				return
			}
		}
	}
}
