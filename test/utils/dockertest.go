package utils

import (
	"fmt"
	"time"

	"github.com/ory/dockertest/v3"
)

// CreateContainer will create a docker container using the RunOptions given
//
// "opts" is the configuration for docker
//
// "retryOp" is an exponential backoff-retry, because the application in the container might not be ready to accept connections yet
func CreateContainer(opts dockertest.RunOptions, retryOp func(r *dockertest.Resource) error) (func() error, error) {
	pool, err := dockertest.NewPool("")
	// exponential backoff-retry, because the application in the container might not be ready to accept connections yet
	pool.MaxWait = 300 * time.Second
	if err != nil {
		return nil, fmt.Errorf("create dockertest pool: %w", err)
	}
	resource, err := pool.RunWithOptions(&opts)
	if err != nil {
		return nil, fmt.Errorf("start resource: %w", err)
	}
	purgeFn := func() error {
		if err := pool.Purge(resource); err != nil {
			return fmt.Errorf("purge resource: %w", err)
		}
		return nil
	}

	if err := pool.Retry(func() error {
		if err := retryOp(resource); err != nil {
			fmt.Printf("retrying: %s\n", err)
			return err
		}

		return nil
	}); err != nil {
		if err := purgeFn(); err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("connect to docker: %w", err)
	}

	return purgeFn, nil
}
