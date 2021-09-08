//+build integration

package mysql_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"testing"

	"database/sql"

	_ "github.com/go-sql-driver/mysql"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/plugins/extractors/mysql"
	"github.com/odpf/meteor/proto/odpf/assets"
	"github.com/odpf/meteor/proto/odpf/assets/common"
	"github.com/odpf/meteor/proto/odpf/assets/facets"
	"github.com/odpf/meteor/test"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/assert"
)

var db *sql.DB

const (
	user = "meteor_test_user"
	pass = "pass"
	port = "3310"
)

var host = "localhost:" + port

func TestMain(m *testing.M) {
	// setup test
	opts := dockertest.RunOptions{
		Repository: "mysql",
		Tag:        "8.0.25",
		Env: []string{
			"MYSQL_ALLOW_EMPTY_PASSWORD=true",
		},
		ExposedPorts: []string{"3306", port},
		PortBindings: map[docker.Port][]docker.PortBinding{
			"3306": {
				{HostIP: "0.0.0.0", HostPort: port},
			},
		},
	}
	// exponential backoff-retry, because the application in the container might not be ready to accept connections yet
	retryFn := func(resource *dockertest.Resource) (err error) {
		db, err = sql.Open("mysql", "root@tcp("+host+")/")
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

func TestExtract(t *testing.T) {
	t.Run("should return error for invalid configs", func(t *testing.T) {
		err := newExtractor().Extract(context.TODO(), map[string]interface{}{
			"password": "pass",
			"host":     host,
		}, make(chan<- interface{}))

		assert.Equal(t, plugins.InvalidConfigError{}, err)
	})

	t.Run("should extract and output tables metadata along with its columns", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		out := make(chan interface{})

		go func() {
			err := newExtractor().Extract(ctx, map[string]interface{}{
				"user_id":  user,
				"password": pass,
				"host":     host,
			}, out)
			close(out)

			assert.Nil(t, err)
		}()

		var results []assets.Table
		for d := range out {
			table, ok := d.(assets.Table)
			if !ok {
				t.Fatal(errors.New("invalid table format"))
			}

			results = append(results, table)
		}

		assert.Equal(t, getExpected(), results)
	})
}

func setup() (err error) {
	testDB := "mockdata_meteor_metadata_test"

	// create database, user and grant access
	err = execute(db, []string{
		fmt.Sprintf("DROP DATABASE IF EXISTS %s", testDB),
		fmt.Sprintf("CREATE DATABASE %s", testDB),
		fmt.Sprintf("USE %s;", testDB),
		fmt.Sprintf(`CREATE USER IF NOT EXISTS '%s'@'%%' IDENTIFIED BY '%s';`, user, pass),
		fmt.Sprintf(`GRANT ALL PRIVILEGES ON *.* TO '%s'@'%%';`, user),
	})
	if err != nil {
		return
	}

	// create and populate tables
	err = execute(db, []string{
		"CREATE TABLE applicant (applicant_id int, last_name varchar(255), first_name varchar(255));",
		"INSERT INTO applicant VALUES (1, 'test1', 'test11');",
		"CREATE TABLE jobs (job_id int, job varchar(255), department varchar(255));",
		"INSERT INTO jobs VALUES (2, 'test2', 'test22');",
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

func newExtractor() *mysql.Extractor {
	return mysql.New(test.Logger)
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
						DataType:    "int",
						Description: "",
						IsNullable:  true,
						Length:      0,
					},
					{
						Name:        "first_name",
						DataType:    "varchar",
						Description: "",
						IsNullable:  true,
						Length:      255,
					},
					{
						Name:        "last_name",
						DataType:    "varchar",
						Description: "",
						IsNullable:  true,
						Length:      255,
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
						Name:        "department",
						DataType:    "varchar",
						Description: "",
						IsNullable:  true,
						Length:      255,
					},
					{
						Name:        "job",
						DataType:    "varchar",
						Description: "",
						IsNullable:  true,
						Length:      255,
					},
					{
						Name:        "job_id",
						DataType:    "int",
						Description: "",
						IsNullable:  true,
						Length:      0,
					},
				},
			},
		},
	}
}
