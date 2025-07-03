package core

import (
	"context"
	"log"
)

type Listener[T any] struct {
	Channel chan T
}

func NewListener[T any]() *Listener[T] {
	return &Listener[T]{
		Channel: make(chan T, 1),
	}
}

func (l *Listener[T]) Listen(ctx context.Context, notifyFunction func(data T)) {
	for {
		log.Printf("Context: %v, Channel: %v", ctx, l.Channel)
		select {
		case <-ctx.Done():
			return
		case data := <-l.Channel:
			if notifyFunction != nil {
				go notifyFunction(data)
			}
		}
	}
}

func (l *Listener[T]) Notify(data T) {
	log.Printf("Listener notified with data: %v", data)
	l.Channel <- data
}
