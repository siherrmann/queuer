package core

import "log"

type Broadcaster[T any] struct {
	name      string
	listeners map[chan T]bool
}

// NewBroadcaster creates a new Broadcaster instance for the specified type T.
// It initializes a map to hold the listeners.
// The name parameter is used to identify the broadcaster.
// It returns a pointer to the new Broadcaster instance.
func NewBroadcaster[T any](name string) *Broadcaster[T] {
	return &Broadcaster[T]{
		name:      name,
		listeners: make(map[chan T]bool),
	}
}

// Subscribe adds a new listener channel to the broadcaster.
// It returns a channel of type T that can be used to receive messages.
// The channel is buffered (100) to allow for non-blocking sends.
func (b *Broadcaster[T]) Subscribe() chan T {
	ch := make(chan T, 100)
	b.listeners[ch] = true
	return ch
}

// Unsubscribe removes a listener channel from the broadcaster.
// It closes the channel to signal that no more messages will be sent.
// If the channel does not exist in the listeners map, it does nothing.
func (b *Broadcaster[T]) Unsubscribe(ch chan T) {
	if _, ok := b.listeners[ch]; ok {
		delete(b.listeners, ch)
		close(ch)
	}
}

// Broadcast sends a message of type T to all subscribed listeners.
// It logs the number of listeners and the name of the broadcaster.
// If a listener's channel is full, it skips sending the message to avoid blocking.
// This is a non-blocking send operation.
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
