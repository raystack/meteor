//+build integration

package clickhouse_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"testing"

	"database/sql"

	_ "github.com/ClickHouse/clickhouse-go"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/plugins/extractors/clickhouse"
	"github.com/odpf/meteor/plugins/testutils"
	"github.com/odpf/meteor/proto/odpf/assets"
	"github.com/odpf/meteor/proto/odpf/assets/common"
	"github.com/odpf/meteor/proto/odpf/assets/facets"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/assert"
)

const (
	testDB     = "mockdata_meteor_metadata_test"
	user       = "meteor_test_user"
	pass       = "pass"
	globalhost = "%"
	port       = "9000"
)

var (
	db   *sql.DB
	host = "127.0.0.1:" + port
)

func TestMain(m *testing.M) {
	pwd, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}
	// setup test
	opts := dockertest.RunOptions{
		Repository:   "yandex/clickhouse-server",
		Tag:          "21.7.4-alpine",
		ExposedPorts: []string{"9000", port},
		Mounts: []string{
			fmt.Sprintf("%s/localConfig/users.xml:/etc/clickhouse-server/users.xml:rw", pwd),
		},
		PortBindings: map[docker.Port][]docker.PortBinding{
			"9000": {
				{HostIP: "0.0.0.0", HostPort: port},
			},
		},
	}
	// exponential backoff-retry, because the application in the container might not be ready to accept connections yet
	retryFn := func(resource *dockertest.Resource) (err error) {
		db, err = sql.Open("clickhouse", fmt.Sprintf("tcp://%s?username=default&password=pass&debug=true", host))
		if err != nil {
			return err
		}
		return db.Ping()
	}
	err, purgeFn := testutils.CreateContainer(opts, retryFn)
	if err != nil {
		log.Fatal(err)
	}
	if err := setup(); err != nil {
		log.Fatal(err)
	}

	// run tests
	code := m.Run()

	// clean tests
	db.Close()
	if err := purgeFn(); err != nil {
		log.Fatal(err)
	}
	os.Exit(code)
}

func TestExtract(t *testing.T) {
	t.Run("should return error for invalid configuration", func(t *testing.T) {
		err := newExtractor().Extract(context.TODO(), map[string]interface{}{
			"password": "pass",
			"host":     host,
		}, make(chan<- interface{}))

		assert.Equal(t, plugins.InvalidConfigError{}, err)
	})

	t.Run("should return mockdata we generated with clickhouse running on localhost", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		extractOut := make(chan interface{})

		go func() {
			err := newExtractor().Extract(ctx, map[string]interface{}{
				"user_id":  "default",
				"password": pass,
				"host":     host,
			}, extractOut)

			close(extractOut)

			assert.Nil(t, err)
		}()

		var results []assets.Table
		for d := range extractOut {
			table, ok := d.(assets.Table)
			if !ok {
				t.Fatal(errors.New("invalid table format"))
			}
			results = append(results, table)
		}
		assert.Equal(t, getExpected(), results)
	})
}

func getExpected() []assets.Table {
	return []assets.Table{
		{
			Resource: &common.Resource{
				Urn:  "mockdata_meteor_metadata_test.applicant",
				Name: "applicant",
			},
			Schema: &facets.Columns{
				Columns: []*facets.Column{
					{
						Name:        "applicant_id",
						DataType:    "Int32",
						Description: "",
					},
					{
						Name:        "last_name",
						DataType:    "String",
						Description: "",
					},
					{
						Name:        "first_name",
						DataType:    "String",
						Description: "",
					},
				},
			},
		},
		{
			Resource: &common.Resource{
				Urn:  "mockdata_meteor_metadata_test.jobs",
				Name: "jobs",
			},
			Schema: &facets.Columns{
				Columns: []*facets.Column{
					{
						Name:        "job_id",
						DataType:    "Int32",
						Description: "",
					},
					{
						Name:        "job",
						DataType:    "String",
						Description: "",
					},
					{
						Name:        "department",
						DataType:    "String",
						Description: "",
					},
				},
			},
		},
	}
}

func setup() (err error) {
	// create database, user and grant access
	err = execute(db, []string{
		fmt.Sprintf("DROP DATABASE IF EXISTS %s", testDB),
		fmt.Sprintf("CREATE DATABASE %s", testDB),
		fmt.Sprintf("USE %s;", testDB),
	})
	if err != nil {
		return
	}

	// create and populate tables
	err = execute(db, []string{
		"CREATE TABLE IF NOT EXISTS applicant (applicant_id int, last_name varchar(255), first_name varchar(255))  engine=Memory",
		"CREATE TABLE jobs (job_id int, job varchar(255), department varchar(255))  engine=Memory",
	})
	if err != nil {
		return
	}
	return
}

func execute(db *sql.DB, queries []string) (err error) {
	for _, query := range queries {
		_, err = db.Exec(query)
		if err != nil {
			return
		}
	}
	return
}

func newExtractor() *clickhouse.Extractor {
	return clickhouse.New(testutils.Logger)
}
