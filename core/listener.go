package core

import (
	"context"
	"log"
	"sync"
	"time"
)

type Listener[T any] struct {
	name      string
	channel   chan T
	waitgroup *sync.WaitGroup
}

func NewListener[T any](name string) *Listener[T] {
	return &Listener[T]{
		name:      name,
		channel:   make(chan T, 100),
		waitgroup: &sync.WaitGroup{},
	}
}

func (l *Listener[T]) Listen(ctx context.Context, ready chan struct{}, notifyFunction func(data T)) {
	if notifyFunction == nil {
		return
	}

	readySignaled := false
	for {
		select {
		case <-ctx.Done():
			if !readySignaled {
				close(ready)
			}
			return
		case data := <-l.channel:
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

func (l *Listener[T]) Notify(data T) {
	select {
	case l.channel <- data:
		log.Println("Listener notified")
	case <-time.After(1 * time.Second):
		panic("Listener timeout: probably the listener is not running or the channel is full")
	}
}

func (l *Listener[T]) WaitForNotificationsProcessed() {
	l.waitgroup.Wait()
}
