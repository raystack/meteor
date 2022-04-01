package utils

import (
	"fmt"
	"time"

	"github.com/ory/dockertest/v3"
)

//CreateContainer will create a docker container using the RunOptions given
//
//"opts" is the configuration for docker
//
//"retryOp" is an exponential backoff-retry, because the application in the container might not be ready to accept connections yet
func CreateContainer(opts dockertest.RunOptions, retryOp func(r *dockertest.Resource) error) (purgeFn func() error, err error) {
	pool, err := dockertest.NewPool("")
	// exponential backoff-retry, because the application in the container might not be ready to accept connections yet
	pool.MaxWait = 300 * time.Second
	if err != nil {
		return purgeFn, fmt.Errorf("could not create dockertest pool: %s", err)
	}
	resource, err := pool.RunWithOptions(&opts)
	if err != nil {
		return purgeFn, fmt.Errorf("could not start resource: %s", err.Error())
	}
	purgeFn = func() error {
		if err := pool.Purge(resource); err != nil {
			return fmt.Errorf("could not purge resource: %s", err)
		}

		return nil
	}

	if err = pool.Retry(func() (err error) {
		err = retryOp(resource)
		if err != nil {
			fmt.Println(fmt.Errorf("retrying: %s", err))
		}

		return
	}); err != nil {
		if err := purgeFn(); err != nil {
			return nil, err
		}
		return purgeFn, fmt.Errorf("could not connect to docker: %s", err.Error())
	}
	return
}
