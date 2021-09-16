//+build integration

package mssql_test

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"testing"

	_ "github.com/denisenkom/go-mssqldb"
	"github.com/odpf/meteor/models"
	"github.com/odpf/meteor/models/odpf/assets"
	"github.com/odpf/meteor/models/odpf/assets/common"
	"github.com/odpf/meteor/models/odpf/assets/facets"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/plugins/extractors/mssql"
	"github.com/odpf/meteor/test"
	"github.com/odpf/meteor/test/mocks"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/assert"
)

const (
	testDB = "mockdata_meteor_metadata_test"
	user   = "sa"
	pass   = "P@ssword1234"
	port   = "1433"
)

var host = "localhost:" + port

var db *sql.DB

func TestMain(m *testing.M) {
	// setup test
	opts := dockertest.RunOptions{
		Repository: "mcr.microsoft.com/mssql/server",
		Tag:        "2019-latest",
		Env: []string{
			"SA_PASSWORD=" + pass,
			"ACCEPT_EULA=Y",
		},
		ExposedPorts: []string{port},
		PortBindings: map[docker.Port][]docker.PortBinding{
			port: {
				{HostIP: "0.0.0.0", HostPort: port},
			},
		},
	}
	retryFn := func(resource *dockertest.Resource) (err error) {
		db, err = sql.Open("mssql", fmt.Sprintf("sqlserver://%s:%s@%s/", user, pass, host))
		if err != nil {
			return err
		}
		return db.Ping()
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
	db.Close()
	if err := purgeFn(); err != nil {
		log.Fatal(err)
	}
	os.Exit(code)
}

func TestInit(t *testing.T) {
	t.Run("should error for invalid configurations", func(t *testing.T) {
		err := mssql.New(test.Logger).Init(context.TODO(), map[string]interface{}{
			"password": "pass",
			"host":     host,
		})

		assert.Equal(t, plugins.InvalidConfigError{}, err)
	})
}

func TestExtract(t *testing.T) {
	t.Run("should extract and output tables metadata along with its columns", func(t *testing.T) {
		ctx := context.TODO()
		extr := mssql.New(test.Logger)

		err := extr.Init(ctx, map[string]interface{}{
			"user_id":  user,
			"password": pass,
			"host":     host,
		})
		if err != nil {
			t.Fatal(err)
		}

		emitter := mocks.NewEmitter()
		err = extr.Extract(ctx, emitter.Push)

		assert.NoError(t, err)
		assert.Equal(t, getExpected(), emitter.Get())
	})
}

func setup() (err error) {
	err = execute(db, []string{
		fmt.Sprintf("DROP DATABASE IF EXISTS %s;", testDB),
		fmt.Sprintf("CREATE DATABASE %s;", testDB),
		fmt.Sprintf("USE %s;", testDB),
	})
	if err != nil {
		return
	}

	err = execute(db, []string{
		fmt.Sprintf("CREATE TABLE %s.dbo.applicant (applicant_id int, last_name varchar(255), first_name varchar(255));", testDB),
		fmt.Sprintf("INSERT INTO %s.dbo.applicant VALUES (1, 'test1', 'test11');", testDB),
		fmt.Sprintf("CREATE TABLE %s.dbo.jobs (job_id int, job varchar(255), department varchar(255));", testDB),
		fmt.Sprintf("INSERT INTO %s.dbo.jobs VALUES (2, 'test2', 'test22');", testDB),
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

func getExpected() []models.Record {
	return []models.Record{
		models.NewRecord(&assets.Table{
			Resource: &common.Resource{
				Urn:  "mockdata_meteor_metadata_test.applicant",
				Name: "applicant",
			},
			Schema: &facets.Columns{
				Columns: []*facets.Column{
					{
						DataType:   "int",
						Name:       "applicant_id",
						IsNullable: true,
						Length:     0,
					},
					{
						DataType:   "varchar",
						Name:       "first_name",
						IsNullable: true,
						Length:     255,
					},
					{
						DataType:   "varchar",
						Name:       "last_name",
						IsNullable: true,
						Length:     255,
					},
				},
			},
		}),
		models.NewRecord(&assets.Table{
			Resource: &common.Resource{
				Urn:  "mockdata_meteor_metadata_test.jobs",
				Name: "jobs",
			},
			Schema: &facets.Columns{
				Columns: []*facets.Column{
					{
						DataType:   "varchar",
						Name:       "department",
						IsNullable: true,
						Length:     255,
					},
					{
						DataType:   "varchar",
						Name:       "job",
						IsNullable: true,
						Length:     255,
					},
					{
						DataType:   "int",
						Name:       "job_id",
						IsNullable: true,
						Length:     0,
					},
				},
			},
		}),
	}
}
