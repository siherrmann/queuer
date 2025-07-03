package core

import (
	"context"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const maxDeviation = 50 * time.Millisecond

// funcScheduler is a mock function type that can be passed to Scheduler.
type funcScheduler struct {
	wg           sync.WaitGroup // WaitGroup to signal when the function has been called
	workDuration time.Duration  // How long each call should simulate work
}

// newFuncScheduler creates a new instance of funcScheduler.
func newFuncScheduler(workDuration time.Duration) *funcScheduler {
	m := &funcScheduler{
		wg:           sync.WaitGroup{},
		workDuration: workDuration,
	}
	return m
}

// Call simulates the execution of the function.
func (m *funcScheduler) Call() {
	if m.workDuration > 0 {
		time.Sleep(m.workDuration) // Simulate work
	}
	log.Println("Mock function called")
	m.wg.Done()
}

// TestNewScheduler tests the NewScheduler constructor with valid inputs.
func TestNewScheduler(t *testing.T) {
	mockFn := func() {}
	now := time.Now()

	tests := []struct {
		name      string
		task      interface{}
		startTime time.Time
		expected  time.Duration // Expected 'After' duration
		wantErr   bool
	}{
		{
			name:      "With Nil StartTime",
			task:      mockFn,
			startTime: time.Time{},
			expected:  0,
			wantErr:   false,
		},
		{
			name:      "With Future StartTime",
			task:      mockFn,
			startTime: now.Add(5 * time.Second),
			expected:  5 * time.Second,
			wantErr:   false,
		},
		{
			name:      "With Past StartTime",
			task:      mockFn,
			startTime: now.Add(-5 * time.Second),
			expected:  -5 * time.Second,
			wantErr:   false,
		},
		{
			name:      "With invalid task type",
			task:      "mockFn",
			startTime: now.Add(5 * time.Second),
			expected:  5 * time.Second,
			wantErr:   true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var err error
			var scheduler *Scheduler
			if test.startTime.IsZero() {
				scheduler, err = NewScheduler(nil, test.task)
			} else {
				scheduler, err = NewScheduler(&test.startTime, test.task)
			}

			if test.wantErr {
				assert.Error(t, err, "Expected error for invalid task or startTime")
				assert.Nil(t, scheduler, "Scheduler should be nil for invalid task")
			} else {
				require.NoError(t, err, "NewScheduler should not return an error for valid task")
				require.NotNil(t, scheduler, "Scheduler should not be nil for valid task")
			}
		})
	}
}

// TestScheduleWithDelay tests if the task executes after the specified delay.
func TestScheduleWithDelay(t *testing.T) {
	mockFn := newFuncScheduler(0)
	delay := 1 * time.Second
	startTime := time.Now().Add(delay) // Time in the future

	mockFn.wg.Add(1)
	scheduler, err := NewScheduler(&startTime, mockFn.Call)
	require.NoError(t, err)

	start := time.Now()
	scheduler.Go(context.Background())

	done := make(chan bool)
	go func() {
		mockFn.wg.Wait()
		done <- true
	}()

	select {
	case <-done:
		duration := time.Since(start)
		assert.GreaterOrEqual(t, duration, delay, "Task should execute after the specified delay")
		assert.Less(t, duration, delay+maxDeviation, "Task should execute reasonably close to the delay") // Add a small buffer
	case <-time.After(delay + maxDeviation):
		t.Error("Task did not execute within the expected time frame")
	}
}

// TestScheduleTimeInPast tests if the task executes immediately if startTime is in the past.
func TestScheduleTimeInPast(t *testing.T) {
	mockFn := newFuncScheduler(0)
	startTime := time.Now().Add(-5 * time.Second) // Time in the past

	mockFn.wg.Add(1)
	scheduler, err := NewScheduler(&startTime, mockFn.Call)
	require.NoError(t, err)

	start := time.Now()
	scheduler.Go(context.Background())

	done := make(chan bool)
	go func() {
		mockFn.wg.Wait()
		done <- true
	}()

	select {
	case <-done:
		duration := time.Since(start)
		assert.Less(t, duration, maxDeviation, "Task should execute almost immediately if start time is in the past")
	case <-time.After(maxDeviation):
		t.Error("Task did not execute immediately as expected")
	}
}

// TestScheduleNoDelay tests if the task executes immediately if delay is zero.
func TestScheduleNoDelay(t *testing.T) {
	mockFn := newFuncScheduler(0)
	now := time.Now() // Current time

	mockFn.wg.Add(1)
	scheduler, err := NewScheduler(&now, mockFn.Call)
	require.NoError(t, err)

	start := time.Now()
	scheduler.Go(context.Background())

	done := make(chan bool)
	go func() {
		mockFn.wg.Wait()
		done <- true
	}()

	select {
	case <-done:
		duration := time.Since(start)
		assert.Less(t, duration, 50*time.Millisecond, "Task should execute almost immediately if delay is zero")
	case <-time.After(maxDeviation):
		t.Error("Task did not execute immediately as expected")
	}
}

// TestScheduleWithWorkDuration tests a task that simulates work.
func TestScheduleWithWorkDuration(t *testing.T) {
	workDuration := 1 * time.Second
	mockFn := newFuncScheduler(workDuration)
	delay := 1 * time.Second
	startTime := time.Now().Add(delay) // Time in the future

	mockFn.wg.Add(1)
	scheduler, err := NewScheduler(&startTime, mockFn.Call)
	require.NoError(t, err)

	start := time.Now()
	scheduler.Go(context.Background())

	done := make(chan bool)
	go func() {
		mockFn.wg.Wait()
		done <- true
	}()

	select {
	case <-done:
		duration := time.Since(start)
		assert.GreaterOrEqual(t, duration, delay+workDuration, "Total duration should be delay + work duration")
		assert.Less(t, duration, delay+workDuration+maxDeviation, "Total duration should be reasonable")
	case <-time.After(delay + workDuration + maxDeviation):
		t.Error("Task did not execute within the expected time frame")
	}
}
