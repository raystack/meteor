package agent

import (
	"errors"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/odpf/meteor/plugins"
)

const (
	defaultRetryTimes      = 5
	defaultInitialInterval = 5 * time.Second
)

func retryIfNeeded(operation func() error, retryTimes int, initialInterval time.Duration, notify func(e error, d time.Duration)) error {
	if retryTimes == 0 {
		retryTimes = defaultRetryTimes
	}
	if initialInterval == 0 {
		initialInterval = defaultInitialInterval
	}

	bo := backoff.WithMaxRetries(createExponentialBackoff(initialInterval), uint64(retryTimes))
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

func createExponentialBackoff(initialInterval time.Duration) backoff.BackOff {
	ebo := backoff.NewExponentialBackOff()
	ebo.InitialInterval = initialInterval // first interval duration to be used
	ebo.RandomizationFactor = 0           // to make sure we get constant increment in interval instead of random
	ebo.Multiplier = 5                    // interval multiplier e.g. 5s -> 25s -> 125s -> 625s

	return ebo
}
