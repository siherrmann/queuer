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
		Channel: make(chan T),
	}
}

func (l *Listener[T]) Listen(ctx context.Context, notifyFunction func(data T)) {
	log.Printf("Context: %v, Channel: %v", ctx, l.Channel)
	for {
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
	select {
	case l.Channel <- data:
		log.Printf("Listener notified with data: %v", data)
	default:
		log.Printf("No listener for data: %v", data)
	}
}
