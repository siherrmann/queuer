package core

import (
	"context"
	"fmt"
)

func CancelableGo(ctx context.Context, task func(), errorChan chan error) {
	ctxGo, cancel := context.WithCancel(ctx)
	defer cancel()

	done := make(chan struct{})
	panicChan := make(chan interface{})

	go func() {
		defer close(done)
		defer func() {
			if p := recover(); p != nil {
				panicChan <- p
			}
		}()
		task()
	}()

	select {
	case p := <-panicChan:
		cancel()
		errorChan <- fmt.Errorf("cancelable task panicked: %v", p)
	case <-ctxGo.Done():
		errorChan <- fmt.Errorf("cancelable task cancelled: %v", ctxGo.Err())
	case <-done:
		errorChan <- nil
	}
}
