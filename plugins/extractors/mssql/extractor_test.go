//+build integration

package mssql_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"testing"

	_ "github.com/denisenkom/go-mssqldb"
	"github.com/odpf/meteor/core/extractor"
	"github.com/odpf/meteor/logger"
	"github.com/odpf/meteor/plugins/extractors/mssql"
	"github.com/odpf/meteor/plugins/testutils"
	"github.com/odpf/meteor/proto/odpf/meta"
	"github.com/odpf/meteor/proto/odpf/meta/facets"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/assert"
)

const (
	testDB = "mockdata_meteor_metadata_test"
	user   = "sa"
	pass   = "P@ssword1234"
)

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
		ExposedPorts: []string{"1433"},
		PortBindings: map[docker.Port][]docker.PortBinding{
			"1433": {
				{HostIP: "0.0.0.0", HostPort: "1433"},
			},
		},
	}
	retryFn := func(resource *dockertest.Resource) (err error) {
		db, err = sql.Open("mssql", fmt.Sprintf("sqlserver://%s:%s@localhost:1433/", user, pass))
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
	t.Run("should return error if no user_id in config", func(t *testing.T) {
		err := newExtractor().Extract(context.TODO(), map[string]interface{}{
			"password": "pass",
			"host":     "localhost:1433",
		}, make(chan<- interface{}))

		assert.Equal(t, extractor.InvalidConfigError{}, err)
	})
	t.Run("should return error if no password in config", func(t *testing.T) {
		err := newExtractor().Extract(context.TODO(), map[string]interface{}{
			"user_id": user,
			"host":    "localhost:1433",
		}, make(chan<- interface{}))

		assert.Equal(t, extractor.InvalidConfigError{}, err)
	})
	t.Run("should return error if no host in config", func(t *testing.T) {
		err := newExtractor().Extract(context.TODO(), map[string]interface{}{
			"user_id":  user,
			"password": pass,
		}, make(chan<- interface{}))

		assert.Equal(t, extractor.InvalidConfigError{}, err)
	})
	t.Run("should extract and output tables metadata along with its columns", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		out := make(chan interface{})

		go func() {
			err := newExtractor().Extract(ctx, map[string]interface{}{
				"user_id":  user,
				"password": pass,
				"host":     "localhost:1433",
			}, out)
			close(out)

			assert.Nil(t, err)
		}()

		var results []meta.Table
		for d := range out {
			table, ok := d.(meta.Table)
			if !ok {
				t.Fatal(errors.New("invalid table format"))
			}

			results = append(results, table)
		}

		assert.Equal(t, len(getExpected()), len(results))
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

func newExtractor() *mssql.Extractor {
	return mssql.New(
		logger.NewWithWriter("info", ioutil.Discard),
	)
}

func getExpected() (expected []meta.Table) {
	return []meta.Table{
		{
			Urn:  "mockdata_meteor_metadata_test.applicant",
			Name: "applicant",
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
		},
		{
			Urn:  "mockdata_meteor_metadata_test.jobs",
			Name: "jobs",
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
		},
	}
}
