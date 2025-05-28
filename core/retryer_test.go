package core

import (
	"errors"
	"queuer/model"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// Mock function to simulate a task that might fail
type funcRetryer struct {
	failCount   *atomic.Int32 // How many times it should fail before succeeding
	callCount   *atomic.Int32 // Tracks how many times it has been called
	errExpected error         // The error it returns when failing
}

func newFuncRetryer(failBeforeSuccess int32, errToReturn error) *funcRetryer {
	m := &funcRetryer{
		errExpected: errToReturn,
		failCount:   new(atomic.Int32),
		callCount:   new(atomic.Int32),
	}
	m.failCount.Store(failBeforeSuccess)
	m.callCount.Store(0)
	return m
}

func (m *funcRetryer) Call() error {
	m.callCount.Add(1)
	if m.callCount.Load() <= m.failCount.Load() {
		return m.errExpected
	}
	return nil
}

// TestNewRetryer tests the constructor's validation
func TestNewRetryer(t *testing.T) {
	tests := []struct {
		name    string
		options *model.Options
		wantErr bool
	}{
		{
			name: "Valid Options",
			options: &model.Options{
				MaxRetries:   3,
				RetryDelay:   1,
				RetryBackoff: model.RETRY_BACKOFF_NONE,
			},
			wantErr: false,
		},
		{
			name: "MaxRetries Zero",
			options: &model.Options{
				MaxRetries:   0,
				RetryDelay:   1,
				RetryBackoff: model.RETRY_BACKOFF_NONE,
			},
			wantErr: true,
		},
		{
			name: "MaxRetries Negative",
			options: &model.Options{
				MaxRetries:   -1,
				RetryDelay:   1,
				RetryBackoff: model.RETRY_BACKOFF_NONE,
			},
			wantErr: true,
		},
		{
			name: "RetryDelay Negative",
			options: &model.Options{
				MaxRetries:   3,
				RetryDelay:   -1,
				RetryBackoff: model.RETRY_BACKOFF_NONE,
			},
			wantErr: true,
		},
		{
			name:    "Nil Options",
			options: nil,
			wantErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := NewRetryer(func() error { return nil }, test.options)
			if test.wantErr {
				assert.Error(t, err, "NewRetryer should return an error for invalid options")
			} else {
				assert.NoError(t, err, "NewRetryer should not return an error for valid options")
			}
		})
	}
}

// TestRetrySuccessFirstAttempt tests a function that succeeds immediately
func TestRetrySuccessFirstAttempt(t *testing.T) {
	mockFn := newFuncRetryer(0, errors.New("initial error")) // Succeeds on 1st call

	options := &model.Options{
		MaxRetries:   3,
		RetryDelay:   1,
		RetryBackoff: model.RETRY_BACKOFF_NONE,
	}
	retryer, err := NewRetryer(mockFn.Call, options)
	assert.NoError(t, err, "NewRetryer should not return an error for valid options")

	err = retryer.Retry()
	assert.NoError(t, err, "Retry() should not return an error on success")
	assert.Equal(t, int32(1), mockFn.callCount.Load(), "Retry() should call mock function 1 time")
}

// TestRetrySuccessAfterRetries tests a function that succeeds after some failures
func TestRetrySuccessAfterRetries(t *testing.T) {
	mockFn := newFuncRetryer(2, errors.New("temporary error")) // Fails 2 times, succeeds on 3rd

	options := &model.Options{
		MaxRetries:   5,
		RetryDelay:   1,
		RetryBackoff: model.RETRY_BACKOFF_NONE,
	}
	retryer, err := NewRetryer(mockFn.Call, options)
	assert.NoError(t, err, "NewRetryer should not return an error for valid options")

	err = retryer.Retry()
	assert.NoError(t, err, "Retry() should not return an error on success after retries")
	assert.Equal(t, int32(3), mockFn.callCount.Load(), "Retry() should call mock function 3 times")
}

// TestRetryFailureMaxRetries tests a function that always fails
func TestRetryFailureMaxRetries(t *testing.T) {
	expectedErr := errors.New("permanent error")
	mockFn := newFuncRetryer(5, expectedErr) // Fails 5 times

	options := &model.Options{
		MaxRetries:   5,
		RetryDelay:   1,
		RetryBackoff: model.RETRY_BACKOFF_NONE,
	}
	retryer, err := NewRetryer(mockFn.Call, options)
	assert.NoError(t, err, "NewRetryer should not return an error for valid options")

	err = retryer.Retry()
	assert.Error(t, err, "Retry() should return an error after max retries")
	assert.Equal(t, expectedErr, err, "Retry() should return the expected error after max retries")
	assert.Equal(t, int32(5), mockFn.callCount.Load(), "Retry() should call mock function 5 times")
}

// TestRetryLinearBackoff tests linear backoff
func TestRetryLinearBackoff(t *testing.T) {
	expectedErr := errors.New("test error")
	mockFn := newFuncRetryer(3, expectedErr) // Fails 3 times

	options := &model.Options{
		MaxRetries:   3,
		RetryDelay:   1,
		RetryBackoff: model.RETRY_BACKOFF_LINEAR,
	}
	retryer, err := NewRetryer(mockFn.Call, options)
	assert.NoError(t, err, "NewRetryer should not return an error for valid options")

	start := time.Now()
	err = retryer.Retry()
	duration := time.Since(start)

	assert.Error(t, err, "Retry() should return an error after retries")
	assert.Equal(t, expectedErr, err, "Retry() should return the expected error after retries")
	assert.Equal(t, int32(3), mockFn.callCount.Load(), "Retry() should call mock function 3 times")
	assert.GreaterOrEqual(t, duration, 6*time.Second, "Retry() duration should be at least 6 seconds")
}

// TestRetryExponentialBackoff tests exponential backoff
func TestRetryExponentialBackoff(t *testing.T) {
	expectedErr := errors.New("test error")
	mockFn := newFuncRetryer(3, expectedErr) // Fails 3 times

	options := &model.Options{
		MaxRetries:   3,
		RetryDelay:   1,
		RetryBackoff: model.RETRY_BACKOFF_EXPONENTIAL,
	}
	retryer, err := NewRetryer(mockFn.Call, options)
	assert.NoError(t, err, "NewRetryer should not return an error for valid options")

	start := time.Now()
	err = retryer.Retry()
	duration := time.Since(start)

	assert.Error(t, err, "Retry() should return an error after retries")
	assert.Equal(t, expectedErr, err, "Retry() should return the expected error after retries")
	assert.Equal(t, int32(3), mockFn.callCount.Load(), "Retry() should call mock function 3 times")
	assert.GreaterOrEqual(t, duration, 7*time.Second, "Retry() duration should be at least 7 seconds")
}
