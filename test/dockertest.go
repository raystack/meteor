package test

import (
	"fmt"

	"github.com/ory/dockertest/v3"
)

//This will create a docker container using the RunOptions given
//
//"opts" is the configuration for docker
//
//"retryOp" is an exponential backoff-retry, because the application in the container might not be ready to accept connections yet
func CreateContainer(opts dockertest.RunOptions, retryOp func(r *dockertest.Resource) error) (err error, purgeFn func() error) {
	pool, err := dockertest.NewPool("")
	if err != nil {
		return fmt.Errorf("Could not create dockertest pool: %s", err), purgeFn
	}
	resource, err := pool.RunWithOptions(&opts)
	if err != nil {
		return fmt.Errorf("Could not start resource: %s", err.Error()), purgeFn
	}
	purgeFn = func() error {
		if err := pool.Purge(resource); err != nil {
			return fmt.Errorf("Could not purge resource: %s", err)
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
		purgeFn()
		return fmt.Errorf("Could not connect to docker: %s", err.Error()), purgeFn
	}
	return
}
