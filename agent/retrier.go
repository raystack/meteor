package agent

import (
	"context"
	"errors"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/goto/meteor/plugins"
)

const (
	defaultInitialInterval = 5 * time.Second
)

type retrier struct {
	maxRetries      int
	initialInterval time.Duration
}

func newRetrier(maxRetries int, initialInterval time.Duration) *retrier {
	r := retrier{
		maxRetries:      maxRetries,
		initialInterval: initialInterval,
	}

	if r.initialInterval == 0 {
		r.initialInterval = defaultInitialInterval
	}

	return &r
}

func (r *retrier) retry(ctx context.Context, operation func() error, notify func(e error, d time.Duration)) error {
	bo := backoff.WithMaxRetries(r.createExponentialBackoff(r.initialInterval), uint64(r.maxRetries))
	bo = backoff.WithContext(bo, ctx)
	return backoff.RetryNotify(func() error {
		err := operation()
		if err == nil {
			return err
		}
		// if err is RetryError, returns err directly to retry
		if errors.As(err, &plugins.RetryError{}) {
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
