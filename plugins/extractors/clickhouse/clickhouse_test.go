//go:build plugins
// +build plugins

package clickhouse_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"

	"database/sql"

	_ "github.com/ClickHouse/clickhouse-go"
	"github.com/ory/dockertest/v3"
	"github.com/raystack/meteor/models"
	meteorv1beta1 "github.com/raystack/meteor/models/raystack/meteor/v1beta1"
	"github.com/raystack/meteor/plugins"
	"github.com/raystack/meteor/plugins/extractors/clickhouse"
	"github.com/raystack/meteor/test/mocks"
	"github.com/raystack/meteor/test/utils"
	"github.com/stretchr/testify/assert"
)

const (
	testDB     = "mockdata_meteor_metadata_test"
	user       = "meteor_test_user"
	pass       = "pass"
	globalhost = "%"
	urnScope   = "test-clickhouse"
)

var (
	db              *sql.DB
	host            string
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
		Repository:   "yandex/clickhouse-server",
		Tag:          "21.7.4-alpine",
		ExposedPorts: []string{"9000"},
		Mounts: []string{
			fmt.Sprintf("%s/localConfig/users.xml:/etc/clickhouse-server/users.d/user.xml:rw", pwd),
		},
	}
	// exponential backoff-retry, because the application in the container might not be ready to accept connections yet
	retryFn := func(resource *dockertest.Resource) (err error) {
		host = resource.GetHostPort("9000/tcp")
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
	utils.SkipIfNoDocker(t, dockerAvailable)
	t.Run("should return error for invalid configuration", func(t *testing.T) {
		err := newExtractor().Init(context.TODO(), plugins.Config{
			URNScope: urnScope,
			RawConfig: map[string]any{
				"invalid_config": "invalid_config_value",
			}})

		assert.ErrorAs(t, err, &plugins.InvalidConfigError{})
	})
}

func TestExtract(t *testing.T) {
	utils.SkipIfNoDocker(t, dockerAvailable)
	t.Run("should return mockdata we generated with clickhouse running on localhost", func(t *testing.T) {
		ctx := context.TODO()
		extr := newExtractor()
		err := extr.Init(ctx, plugins.Config{
			URNScope: urnScope,
			RawConfig: map[string]any{
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

		utils.AssertEqualProtos(t, getExpected(), emitter.GetAllEntities())
	})
}

func getExpected() []*meteorv1beta1.Entity {
	return []*meteorv1beta1.Entity{
		models.NewEntity("urn:clickhouse:test-clickhouse:table:mockdata_meteor_metadata_test.applicant", "table", "applicant", "clickhouse", map[string]any{
			"columns": []any{
				map[string]any{"name": "applicant_id", "data_type": "Int32"},
				map[string]any{"name": "last_name", "data_type": "String"},
				map[string]any{"name": "first_name", "data_type": "String"},
			},
		}),
		models.NewEntity("urn:clickhouse:test-clickhouse:table:mockdata_meteor_metadata_test.jobs", "table", "jobs", "clickhouse", map[string]any{
			"columns": []any{
				map[string]any{"name": "job_id", "data_type": "Int32"},
				map[string]any{"name": "job", "data_type": "String"},
				map[string]any{"name": "department", "data_type": "String"},
			},
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
