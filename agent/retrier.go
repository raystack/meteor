package agent

import (
	"errors"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/odpf/meteor/plugins"
)

const (
	defaultMaxRetries      = 5
	defaultInitialInterval = 5 * time.Second
)

type retrier struct {
	maxRetries      int
	initialInterval time.Duration
}

func newRetrier(maxRetries int, initialInterval time.Duration) *retrier {
	r := new(retrier)

	r.maxRetries = maxRetries
	if r.maxRetries == 0 {
		r.maxRetries = defaultMaxRetries
	}
	r.initialInterval = initialInterval
	if r.initialInterval == 0 {
		r.initialInterval = defaultInitialInterval
	}

	return r
}

func (r *retrier) retry(operation func() error, notify func(e error, d time.Duration)) error {
	bo := backoff.WithMaxRetries(r.createExponentialBackoff(r.initialInterval), uint64(r.maxRetries))
	return backoff.RetryNotify(func() error {
		err := operation()
		if err == nil {
			return err
		}
		// if err is RetryError, returns err directly to retry
		if errors.Is(err, plugins.RetryError{}) {
			return err
		}
		// if err is not RetryError, wraps error to prevent retrying
		return backoff.Permanent(err)
	}, bo, notify)
}

func (r *retrier) createExponentialBackoff(initialInterval time.Duration) backoff.BackOff {
	ebo := backoff.NewExponentialBackOff()
	ebo.InitialInterval = initialInterval // first interval duration to be used
	ebo.RandomizationFactor = 0           // to make sure we get constant increment in interval instead of random
	ebo.Multiplier = 5                    // interval multiplier e.g. 5s -> 25s -> 125s -> 625s

	return ebo
}
