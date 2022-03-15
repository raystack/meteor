//go:build plugins
// +build plugins

package bigtable_test

import (
	"context"
	"log"
	"os"
	"testing"

	"github.com/odpf/meteor/test/utils"

	"cloud.google.com/go/bigtable"
	"github.com/odpf/meteor/plugins"
	bt "github.com/odpf/meteor/plugins/extractors/bigtable"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/assert"
)

func TestMain(m *testing.M) {
	// setup test
	opts := dockertest.RunOptions{
		Repository: "shopify/bigtable-emulator",
		Env: []string{
			"BIGTABLE_EMULATOR_HOST=localhost:9035",
		},
		ExposedPorts: []string{"9035"},
		PortBindings: map[docker.Port][]docker.PortBinding{
			"9035": {
				{HostIP: "0.0.0.0", HostPort: "9035"},
			},
		},
		Cmd: []string{"-cf", "dev.records.data,dev.records.metadata"},
	}
	// exponential backoff-retry, because the application in the container might not be ready to accept connections yet
	retryFn := func(resource *dockertest.Resource) (err error) {
		_, err = bigtable.NewAdminClient(context.Background(), "dev", "dev")
		return
	}
	purgeFn, err := utils.CreateContainer(opts, retryFn)
	if err != nil {
		log.Fatal("", err)
	}

	// run tests
	code := m.Run()

	if err := purgeFn(); err != nil {
		log.Fatal("", err)
	}
	os.Exit(code)
}

func TestInit(t *testing.T) {
	t.Run("should return error if no project_id in config", func(t *testing.T) {
		err := bt.New(utils.Logger).Init(context.TODO(), map[string]interface{}{
			"wrong-config": "sample-project",
		})

		assert.Equal(t, plugins.InvalidConfigError{}, err)
	})

	t.Run("should return error if project_id is empty", func(t *testing.T) {
		err := bt.New(utils.Logger).Init(context.TODO(), map[string]interface{}{
			"project_id": "",
		})

		assert.EqualError(t, err, "invalid extractor config")
	})
}
