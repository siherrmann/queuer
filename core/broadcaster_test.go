package core

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewBroadcaster(t *testing.T) {
	b := NewBroadcaster[int]("testBroadcaster")
	assert.NotNil(t, b, "Broadcaster should not be nil")
	assert.Len(t, b.listeners, 0, "NewBroadcaster should initialize with an empty listeners map")
}

func TestSubscribe(t *testing.T) {
	b := NewBroadcaster[int]("testBroadcaster")
	ch := b.Subscribe()
	assert.NotNil(t, ch, "Subscribe should return a valid channel")
	assert.Len(t, b.listeners, 1, "Subscribe should add the channel to listeners")
	assert.Contains(t, b.listeners, ch, "Subscribe should add the channel to listeners map")
}

func TestUnsubscribe(t *testing.T) {
	b := NewBroadcaster[int]("testBroadcaster")
	ch := b.Subscribe()
	assert.NotNil(t, ch, "Subscribe should return a valid channel")
	assert.Len(t, b.listeners, 1, "Subscribe should add the channel to listeners")

	b.Unsubscribe(ch)
	assert.Len(t, b.listeners, 0, "Unsubscribe should remove the channel from listeners")
	assert.NotContains(t, b.listeners, ch, "Unsubscribe should remove the channel from listeners map")
}

func TestBroadcast(t *testing.T) {
	b := NewBroadcaster[int]("testBroadcaster")
	ch1 := b.Subscribe()
	ch2 := b.Subscribe()

	// Test broadcasting to multiple channels
	for i := 0; i < 50; i++ {
		go b.Broadcast(i)
	}

	messagesReceived := 0

outerloop:
	for {
		select {
		case msg := <-ch1:
			assert.Less(t, msg, 50, "Channel 1 should receive the broadcasted message")
			messagesReceived++
			if messagesReceived == 100 {
				break outerloop
			}
		case msg := <-ch2:
			assert.Less(t, msg, 50, "Channel 2 should receive the broadcasted message")
			messagesReceived++
			if messagesReceived == 100 {
				break outerloop
			}
		case <-time.After(10 * time.Second):
			t.Error("Timed out waiting for messages")
			break outerloop
		}
	}
}
