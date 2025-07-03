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

func NewListener[T any](broadcaster *Broadcaster[T]) (*Listener[T], error) {
	if broadcaster == nil {
		return nil, fmt.Errorf("broadcaster cannot be nil")
	}

	return &Listener[T]{
		broadcaster: broadcaster,
		waitgroup:   &sync.WaitGroup{},
	}, nil
}

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

func (l *Listener[T]) Notify(data T) {
	l.broadcaster.Broadcast(data)
}

func (l *Listener[T]) WaitForNotificationsProcessed() {
	l.waitgroup.Wait()
}
