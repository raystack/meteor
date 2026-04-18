//go:build plugins
// +build plugins

package couchdb_test

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"testing"

	"github.com/raystack/meteor/test/utils"

	_ "github.com/go-kivik/couchdb"
	"github.com/go-kivik/kivik"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/raystack/meteor/plugins"
	"github.com/raystack/meteor/plugins/extractors/couchdb"
	"github.com/raystack/meteor/test/mocks"
	"github.com/stretchr/testify/assert"
)

const (
	user     = "meteor_test_user"
	pass     = "couchdb"
	testDB   = "mockdata_meteor_metadata_test"
	urnScope = "test-couchdb"
)

var (
	host            string
	client          *kivik.Client
	dbs             = []string{"applicant", "jobs"}
	docCount        = 3
	dockerAvailable bool
)

func TestMain(m *testing.M) {
	dockerAvailable = utils.CheckDockerAvailability()
	if !dockerAvailable {
		os.Exit(m.Run())
	}

	pwd, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}
	// setup test
	opts := dockertest.RunOptions{
		Repository: "couchdb",
		Tag:        "3",
		Env: []string{
			"COUCHDB_USER=" + user,
			"COUCHDB_PASSWORD=" + pass,
		},
		Mounts: []string{
			fmt.Sprintf("%s/localConfig:/opt/couchdb/etc/local.d:rw", pwd),
		},
		ExposedPorts: []string{"5984"},
		PortBindings: map[docker.Port][]docker.PortBinding{"5984": {{HostPort: "0"}}},
	}
	// exponential backoff-retry, because the application in the container might not be ready to accept connections yet
	retryFn := func(resource *dockertest.Resource) (err error) {
		host = resource.GetHostPort("5984/tcp")
		client, err = kivik.New("couch", fmt.Sprintf("http://%s:%s@%s/", user, pass, host))
		if err != nil {
			return
		}
		err = setup()
		return
	}
	purgeFn, err := utils.CreateContainer(opts, retryFn)
	if err != nil {
		log.Fatal(err)
	}

	// run tests
	code := m.Run()

	// clean tests
	client.Close(context.TODO())
	if err := purgeFn(); err != nil {
		log.Fatal(err)
	}
	os.Exit(code)
}

func TestInit(t *testing.T) {
	utils.SkipIfNoDocker(t, dockerAvailable)
	t.Run("should return error for invalid configs", func(t *testing.T) {
		err := couchdb.New(utils.Logger).Init(context.TODO(), plugins.Config{
			URNScope: urnScope,
			RawConfig: map[string]any{
				"invalid_config": "invalid_config_value",
			},
		})

		assert.ErrorAs(t, err, &plugins.InvalidConfigError{})
	})
}

func TestExtract(t *testing.T) {
	utils.SkipIfNoDocker(t, dockerAvailable)
	t.Run("should extract and output tables metadata along with its columns", func(t *testing.T) {
		ctx := context.TODO()
		extr := couchdb.New(utils.Logger)

		err := extr.Init(ctx, plugins.Config{
			URNScope: urnScope,
			RawConfig: map[string]any{
				"connection_url": fmt.Sprintf("http://%s:%s@%s/", user, pass, host),
			},
		})
		if err != nil {
			t.Fatal(err)
		}

		emitter := mocks.NewEmitter()
		err = extr.Extract(ctx, emitter.Push)

		assert.NoError(t, err)
		assert.Equal(t, docCount*len(dbs), len(emitter.Get()))
	})
}

func setup() (err error) {
	for _, database := range dbs {
		// create database
		err = client.CreateDB(context.TODO(), database)
		// DB already created
		if kivik.StatusCode(err) == http.StatusPreconditionFailed {
			err = nil
		}
		if err != nil {
			return
		}
		db := client.DB(context.TODO(), database)
		// create and populate tables
		err = execute(mockdata(database), db)
		if err != nil {
			return
		}
	}
	return
}

func execute(queries []map[string]any, db *kivik.DB) (err error) {
	for _, query := range queries {
		_, err := db.Put(context.TODO(), query["_id"].(string), query)
		if kivik.StatusCode(err) == http.StatusConflict {
			err = nil
		}
		if err != nil {
			return err
		}
	}
	return
}

func mockdata(dbName string) (mockSetupData []map[string]any) {
	for i := 0; i < docCount; i++ {
		doc := map[string]any{
			"_id":    kivik.UserPrefix + dbName + strconv.Itoa(i),
			"field1": 1,
			"field2": "data",
		}
		mockSetupData = append(mockSetupData, doc)
	}
	return
}
