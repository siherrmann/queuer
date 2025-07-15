package core

import (
	"fmt"
	"time"

	"github.com/siherrmann/queuer/model"
)

type Retryer struct {
	function func() error
	sleep    time.Duration
	options  *model.OnError
}

// NewRetryer creates a new Retryer instance.
// It initializes the retryer with a function to execute, a sleep duration for retries,
// and options for retry behavior.
// It returns a pointer to the new Retryer instance or an error if the options are invalid.
func NewRetryer(function func() error, options *model.OnError) (*Retryer, error) {
	if options == nil || options.MaxRetries <= 0 || options.RetryDelay < 0 {
		return nil, fmt.Errorf("no valid retry options provided")
	}

	return &Retryer{
		function: function,
		sleep:    time.Duration(options.RetryDelay) * time.Second,
		options:  options,
	}, nil
}

// Retry attempts to execute the function up to MaxRetries times.
// It sleeps for the specified duration between retries.
// The retry behavior is determined by the RetryBackoff option.
// If the function returns an error, it will retry according to the specified backoff strategy.
// If all retries fail, it returns the last error encountered.
//
// The function is executed in a loop until it succeeds or the maximum number of retries is reached
// If the function succeeds, it returns nil.
//
// The backoff strategies are:
// - RETRY_BACKOFF_NONE: No backoff, retries immediately.
// - RETRY_BACKOFF_LINEAR: Increases the sleep duration linearly by the initial delay.
// - RETRY_BACKOFF_EXPONENTIAL: Doubles the sleep duration after each retry.
func (r *Retryer) Retry() (err error) {
	for i := 0; i < r.options.MaxRetries; i++ {
		err = r.function()
		if err != nil {
			time.Sleep(r.sleep)
			if r.options.RetryBackoff == model.RETRY_BACKOFF_NONE {
				continue
			} else if r.options.RetryBackoff == model.RETRY_BACKOFF_LINEAR {
				r.sleep += time.Duration(r.options.RetryDelay) * time.Second
				continue
			} else if r.options.RetryBackoff == model.RETRY_BACKOFF_EXPONENTIAL {
				r.sleep *= 2
				continue
			}
		}
		break
	}
	return err
}
