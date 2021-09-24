//+build integration

package couchdb_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"testing"

	_ "github.com/go-kivik/couchdb"
	"github.com/go-kivik/kivik"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/plugins/extractors/couchdb"
	"github.com/odpf/meteor/test"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/assert"
)

const (
	user   = "meteor_test_user"
	pass   = "couchdb"
	port   = "5984"
	testDB = "mockdata_meteor_metadata_test"
)

var (
	host   = "localhost:" + port
	client *kivik.Client
)

func TestMain(m *testing.M) {
	// setup test
	opts := dockertest.RunOptions{
		Repository: "docker.io/bitnami/couchdb",
		Tag:        "3",
		Env: []string{
			"COUCHDB_USER=" + user,
			"COUCHDB_PASSWORD=" + pass,
		},
		ExposedPorts: []string{"4369", "5984", port},
		PortBindings: map[docker.Port][]docker.PortBinding{
			"5984": {
				{HostIP: "0.0.0.0", HostPort: "5984"},
			},
		},
	}
	// exponential backoff-retry, because the application in the container might not be ready to accept connections yet
	retryFn := func(resource *dockertest.Resource) (err error) {
		client, err = kivik.New("couch", fmt.Sprintf("http://%s:%s@%s/", user, pass, host))
		if err != nil {
			return err
		}
		_, err = client.Ping(context.TODO())
		return
	}
	purgeFn, err := test.CreateContainer(opts, retryFn)
	if err != nil {
		log.Fatal(err)
	}
	if err := setup(); err != nil {
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
	t.Run("should return error for invalid configs", func(t *testing.T) {
		err := couchdb.New(test.Logger).Init(context.TODO(), map[string]interface{}{
			"password": "pass",
			"host":     host,
		})

		assert.Equal(t, plugins.InvalidConfigError{}, err)
	})
}

func TestExtract(t *testing.T) {
	t.Run("should extract and output tables metadata along with its columns", func(t *testing.T) {
		ctx := context.TODO()
		extr := couchdb.New(test.Logger)

		err := extr.Init(ctx, map[string]interface{}{
			"user_id":  user,
			"password": pass,
			"host":     host,
		})
		if err != nil {
			t.Fatal(err)
		}

		// emitter := mocks.NewEmitter()
		// err = extr.Extract(ctx, emitter.Push)

		// assert.NoError(t, err)
		// assert.Equal(t, getExpected(), emitter.Get())
	})
}

func setup() (err error) {
	dbs := []string{"applicant", "jobs"}
	for _, database := range dbs {
		// create database
		err = client.CreateDB(context.TODO(), database)
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

func execute(queries []map[string]interface{}, db *kivik.DB) (err error) {
	for _, query := range queries {
		fmt.Println(query)
		rev, err := db.Put(context.TODO(), query["_id"].(string), query)
		if err != nil {
			return err
		}
		fmt.Println(rev)
	}
	return
}

func mockdata(dbName string) (mockSetupData []map[string]interface{}) {
	for i := 0; i < 3; i++ {
		doc := map[string]interface{}{
			"_id":    kivik.UserPrefix + dbName + strconv.Itoa(i),
			"field1": "data",
			"field2": "data",
		}
		mockSetupData = append(mockSetupData, doc)
	}
	return
}

// func getExpected() []models.Record {
// return []models.Record{
// models.NewRecord(&assets.Table{
// Resource: &common.Resource{
// Urn:  "mockdata_meteor_metadata_test.applicant",
// Name: "applicant",
// },
// Schema: &facets.Columns{
// Columns: []*facets.Column{
// {
// Name:        "applicant_id",
// DataType:    "int",
// Description: "",
// IsNullable:  true,
// Length:      0,
// },
// {
// Name:        "first_name",
// DataType:    "varchar",
// Description: "",
// IsNullable:  true,
// Length:      255,
// },
// {
// Name:        "last_name",
// DataType:    "varchar",
// Description: "",
// IsNullable:  true,
// Length:      255,
// },
// },
// },
// }),
// models.NewRecord(&assets.Table{
// Resource: &common.Resource{
// Urn:  "mockdata_meteor_metadata_test.jobs",
// Name: "jobs",
// },
// Schema: &facets.Columns{
// Columns: []*facets.Column{
// {
// Name:        "department",
// DataType:    "varchar",
// Description: "",
// IsNullable:  true,
// Length:      255,
// },
// {
// Name:        "job",
// DataType:    "varchar",
// Description: "",
// IsNullable:  true,
// Length:      255,
// },
// {
// Name:        "job_id",
// DataType:    "int",
// Description: "",
// IsNullable:  true,
// Length:      0,
// },
// },
// },
// }),
// }
// }
//
