//go:build plugins
// +build plugins

package clickhouse_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/odpf/meteor/test/utils"
	"google.golang.org/protobuf/types/known/anypb"

	"database/sql"

	_ "github.com/ClickHouse/clickhouse-go"
	"github.com/odpf/meteor/models"
	v1beta2 "github.com/odpf/meteor/models/odpf/assets/v1beta2"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/plugins/extractors/clickhouse"
	"github.com/odpf/meteor/test/mocks"
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
	urnScope   = "test-clickhouse"
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
			fmt.Sprintf("%s/localConfig/users.xml:/etc/clickhouse-server/users.d/user.xml:rw", pwd),
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
	purgeFn, err := utils.CreateContainer(opts, retryFn)
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

func TestInit(t *testing.T) {
	t.Run("should return error for invalid configuration", func(t *testing.T) {
		err := newExtractor().Init(context.TODO(), plugins.Config{
			URNScope: urnScope,
			RawConfig: map[string]interface{}{
				"invalid_config": "invalid_config_value",
			}})

		assert.ErrorAs(t, err, &plugins.InvalidConfigError{})
	})
}

func TestExtract(t *testing.T) {
	t.Run("should return mockdata we generated with clickhouse running on localhost", func(t *testing.T) {
		ctx := context.TODO()
		extr := newExtractor()
		err := extr.Init(ctx, plugins.Config{
			URNScope: urnScope,
			RawConfig: map[string]interface{}{
				"connection_url": fmt.Sprintf("tcp://%s?username=default&password=%s&debug=true", host, pass),
			},
		})
		if err != nil {
			t.Fatal(err)
		}

		emitter := mocks.NewEmitter()
		err = extr.Extract(ctx, emitter.Push)
		if err != nil {
			t.Fatal(err)
		}

		assert.Equal(t, getExpected(), emitter.Get())
	})
}

func getExpected() []models.Record {
	table1, err := anypb.New(&v1beta2.Table{
		Columns: []*v1beta2.Column{
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
	})
	if err != nil {
		err = fmt.Errorf("error creating Any struct for test: %w", err)
		log.Fatal(err)
	}
	table2, err := anypb.New(&v1beta2.Table{
		Columns: []*v1beta2.Column{
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
	})
	if err != nil {
		err = fmt.Errorf("error creating Any struct for test: %w", err)
		log.Fatal(err)
	}
	return []models.Record{
		models.NewRecord(&v1beta2.Asset{
			Urn:     "urn:clickhouse:test-clickhouse:table:mockdata_meteor_metadata_test.applicant",
			Name: "applicant",
			Type: "table",
			Service: "clickhouse",
			Data: table1,
		}),
		models.NewRecord(&v1beta2.Asset{
			Urn:     "urn:clickhouse:test-clickhouse:table:mockdata_meteor_metadata_test.jobs",
			Name: "jobs",
			Type: "table",
			Service: "clickhouse",
			Data: table2,
		}),
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
	return clickhouse.New(utils.Logger)
}
