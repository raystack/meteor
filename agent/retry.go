package agent

import (
	"errors"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/odpf/meteor/plugins"
)

const (
	defaultRetryTimes    = 5
	defaultRetryInterval = 10 * time.Second
)

func retryIfNeeded(operation func() error, retryTimes int, retryInterval time.Duration, notify func(e error, d time.Duration)) error {
	if retryTimes == 0 {
		retryTimes = defaultRetryTimes
	}
	if retryInterval == 0 {
		retryInterval = defaultRetryInterval
	}

	bo := backoff.WithMaxRetries(backoff.NewConstantBackOff(retryInterval), uint64(retryTimes))
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
