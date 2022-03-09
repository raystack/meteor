//go:build plugins
// +build plugins

package presto_test

import (
	"context"
	"database/sql"
	"fmt"
	assetsv1beta1 "github.com/odpf/meteor/models/odpf/assets/v1beta1"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/plugins/extractors/presto"
	"github.com/odpf/meteor/test/mocks"
	"github.com/odpf/meteor/test/utils"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	_ "github.com/prestodb/presto-go-client/presto"
	"github.com/stretchr/testify/assert"
	"log"
	"os"
	"testing"
	"time"
)

const (
	user = "presto"
	port = "8080"
)

var (
	host = "localhost:" + port
	db   *sql.DB
)

func TestMain(m *testing.M) {
	// setup test
	opts := dockertest.RunOptions{
		Repository:   "ahanaio/prestodb-sandbox",
		Tag:          "0.270",
		ExposedPorts: []string{"8080"},
		PortBindings: map[docker.Port][]docker.PortBinding{
			"8080": {
				{HostIP: "0.0.0.0", HostPort: "8080"},
			},
		},
	}

	// Exponential backoff-retry for container to accept connections
	// dsn format - http[s]://user[:pass]@host[:port][?parameters]
	retryFn := func(r *dockertest.Resource) (err error) {
		dsn := "http://presto@localhost:8080"
		db, err = sql.Open("presto", dsn)
		if err != nil {
			return err
		}

		// wait until presto ready, might want to call SELECT 1 and retry if failed and give timeout
		time.Sleep(1 * time.Minute)

		return db.Ping()
	}
	purgeFn, err := utils.CreateContainer(opts, retryFn)
	if err != nil {
		log.Fatal(err)
	}

	// Run tests
	code := m.Run()

	// Clean tests
	if err = db.Close(); err != nil {
		log.Fatal(err)
	}
	if err = purgeFn(); err != nil {
		log.Fatal(err)
	}
	os.Exit(code)
}

// TestInit tests the configs
func TestInit(t *testing.T) {
	t.Run("should return error for invalid config", func(t *testing.T) {
		err := presto.New(utils.Logger).Init(context.TODO(), map[string]interface{}{
			"invalid_config": "invalid_config_value",
		})
		assert.Equal(t, plugins.InvalidConfigError{}, err)
	})
}

// TestExtract tests that the extractor returns the expected result
func TestExtract(t *testing.T) {
	t.Run("should return mock data we generated with presto", func(t *testing.T) {
		ctx := context.TODO()
		newExtractor := presto.New(utils.Logger)

		if err := newExtractor.Init(ctx, map[string]interface{}{
			"connection_url":  fmt.Sprintf("http://%s@%s", user, host),
			"exclude_catalog": "memory,system,tpcds,tpch", // only jmx catalog is not excluded
		}); err != nil {
			t.Fatal(err)
		}

		emitter := mocks.NewEmitter()
		err := newExtractor.Extract(ctx, emitter.Push)
		assert.NoError(t, err)

		var urns []string
		for _, record := range emitter.Get() {
			table := record.Data().(*assetsv1beta1.Table)
			urns = append(urns, table.Resource.Urn)

		}
		assert.Equal(t, 242, len(urns))
	})
}
