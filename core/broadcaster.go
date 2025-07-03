package core

import "log"

type Broadcaster[T any] struct {
	name      string
	listeners map[chan T]bool
}

func NewBroadcaster[T any](name string) *Broadcaster[T] {
	return &Broadcaster[T]{
		listeners: make(map[chan T]bool),
	}
}

func (b *Broadcaster[T]) Subscribe() chan T {
	ch := make(chan T, 100)
	b.listeners[ch] = true
	return ch
}

func (b *Broadcaster[T]) Unsubscribe(ch chan T) {
	if _, ok := b.listeners[ch]; ok {
		delete(b.listeners, ch)
		close(ch)
	}
}

func (b *Broadcaster[T]) Broadcast(msg T) {
	log.Printf("Broadcasting message to %d listeners for %v", len(b.listeners), b.name)
	for ch := range b.listeners {
		select {
		case ch <- msg:
		default:
			// If the channel is full, we skip sending the message
			// to avoid blocking the broadcaster.
			// This is a non-blocking send.
		}
	}
}
