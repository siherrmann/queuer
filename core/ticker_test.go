package core

import (
	"context"
	"log"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// funcTicker is a mock function type that can be passed to Ticker.
type funcTicker struct {
	callChannel  chan bool     // Channel to count how many times the function has been called
	workDuration time.Duration // How long each call should simulate work
}

// newFuncTicker creates a new instance of funcTicker.
func newFuncTicker(workDuration time.Duration) *funcTicker {
	m := &funcTicker{
		callChannel:  make(chan bool),
		workDuration: workDuration,
	}
	return m
}

// Call simulates the execution of the function.
func (m *funcTicker) Call() {
	m.callChannel <- true
	if m.workDuration > 0 {
		time.Sleep(m.workDuration) // Simulate work
	}
	log.Println("Mock function called")
}

// TestNewTicker tests the NewTicker constructor with valid inputs.
func TestNewTicker(t *testing.T) {
	mockFn := func() {}
	mockFnWithParameters := func(v1 string, v2 string) {}

	tests := []struct {
		name       string
		interval   time.Duration
		task       interface{}
		parameters []interface{}
		wantErr    bool
	}{
		{
			name:     "With normal interval",
			interval: 1 * time.Second,
			task:     mockFn,
			wantErr:  false,
		},
		{
			name:       "With normal interval and parameters",
			interval:   1 * time.Second,
			task:       mockFnWithParameters,
			parameters: []interface{}{"param1", "param2"},
			wantErr:    false,
		},
		{
			name:     "With zero interval",
			interval: 0,
			task:     mockFn,
			wantErr:  true,
		},
		{
			name:     "With negative interval",
			interval: -1 * time.Second,
			task:     mockFn,
			wantErr:  true,
		},
		{
			name:       "With normal interval and wrong parameters",
			interval:   1 * time.Second,
			task:       mockFnWithParameters,
			parameters: []interface{}{"param1", 1},
			wantErr:    true,
		},
		{
			name:     "With task of invalid type",
			interval: 1 * time.Second,
			task:     "invalidTask",
			wantErr:  true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var err error
			var scheduler *Ticker
			scheduler, err = NewTicker(test.interval, test.task, test.parameters...)

			if test.wantErr {
				assert.Error(t, err, "Expected error for interval, task, or parameters")
				assert.Nil(t, scheduler, "Ticker should be nil for invalid task")
			} else {
				require.NoError(t, err, "NewTicker should not return an error for valid parameters")
				require.NotNil(t, scheduler, "Ticker should not be nil for valid parameters")
			}
		})
	}
}

// TestTickerWithTwoIntervals tests the Go method of Ticker to ensure it runs the task at the specified interval.
func TestTickerWithTwoIntervals(t *testing.T) {
	callCounter := int32(0)
	waitingForCount := 2
	mockFn := newFuncTicker(0)
	interval := 100 * time.Millisecond

	scheduler, err := NewTicker(interval, mockFn.Call)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go scheduler.Go(ctx)

outerLoop:
	for {
		select {
		case <-mockFn.callChannel:
			callCount := atomic.AddInt32(&callCounter, 1)
			log.Printf("Mock function called %d times", callCount)
			if callCount >= int32(waitingForCount) {
				cancel()
				break outerLoop
			}
		case <-time.After((interval * time.Duration(waitingForCount)) + 50*time.Millisecond):
			cancel()
			t.Error("Task did not execute within the expected time frame")
		}
	}
}

// TestTickerWithWorkDuration tests the Go method of Ticker with a work duration.
func TestTickerWithWorkDuration(t *testing.T) {
	callCounter := int32(0)
	waitingForCount := 2
	workDuration := 1 * time.Second
	mockFn := newFuncTicker(workDuration)
	interval := 100 * time.Millisecond

	scheduler, err := NewTicker(interval, mockFn.Call)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go scheduler.Go(ctx)

outerLoop:
	for {
		select {
		case <-mockFn.callChannel:
			callCount := atomic.AddInt32(&callCounter, 1)
			log.Printf("Mock function called %d times", callCount)
			if callCount >= int32(waitingForCount) {
				cancel()
				break outerLoop
			}
		case <-time.After((interval * time.Duration(waitingForCount)) + 50*time.Millisecond):
			cancel()
			t.Error("Task did not execute within the expected time frame")
		}
	}
}
