package core

import (
	"fmt"
	"queuer/model"
	"time"
)

type Retryer struct {
	function func() error
	sleep    time.Duration
	options  *model.OnError
}

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
