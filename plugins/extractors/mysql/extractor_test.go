//+build integration

package mysql_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"

	"database/sql"

	_ "github.com/go-sql-driver/mysql"
	"github.com/odpf/meteor/core/extractor"
	"github.com/odpf/meteor/plugins/testutils"
	"github.com/odpf/meteor/proto/odpf/meta"
	"github.com/odpf/meteor/proto/odpf/meta/facets"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/assert"
)

var db *sql.DB

const (
	testDB     = "mockdata_meteor_metadata_test"
	user       = "meteor_test_user"
	pass       = "pass"
	globalhost = "%"
)

func TestMain(m *testing.M) {
	// setup test
	opts := dockertest.RunOptions{
		Repository: "mysql",
		Tag:        "8.0.25",
		Env: []string{
			"MYSQL_ALLOW_EMPTY_PASSWORD=true",
		},
		ExposedPorts: []string{"3306"},
		PortBindings: map[docker.Port][]docker.PortBinding{
			"3306": {
				{HostIP: "0.0.0.0", HostPort: "3306"},
			},
		},
	}
	// exponential backoff-retry, because the application in the container might not be ready to accept connections yet
	retryFn := func(resource *dockertest.Resource) (err error) {
		db, err = sql.Open("mysql", "root@tcp(localhost:3306)/")
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
		extr, _ := extractor.Catalog.Get("postgres")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		err := extr.Extract(ctx, map[string]interface{}{
			"password": "pass",
			"host":     "localhost:3306",
		}, make(chan<- interface{}))

		assert.Equal(t, extractor.InvalidConfigError{}, err)
	})

	t.Run("should return error if no password in config", func(t *testing.T) {
		extr, _ := extractor.Catalog.Get("postgres")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		err := extr.Extract(ctx, map[string]interface{}{
			"user_id": user,
			"host":    "localhost:3306",
		}, make(chan<- interface{}))

		assert.Equal(t, extractor.InvalidConfigError{}, err)
	})

	t.Run("should return error if no host in config", func(t *testing.T) {
		extr, _ := extractor.Catalog.Get("postgres")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		err := extr.Extract(ctx, map[string]interface{}{
			"user_id":  user,
			"password": pass,
		}, make(chan<- interface{}))

		assert.Equal(t, extractor.InvalidConfigError{}, err)
	})

	t.Run("should return mockdata we generated with mysql running on localhost", func(t *testing.T) {
		extr, _ := extractor.Catalog.Get("postgres")
		ctx, cancel := context.WithCancel(context.Background())

		defer cancel()
		extractOut := make(chan interface{})

		go func() {
			err := extr.Extract(ctx, map[string]interface{}{
				"user_id":  user,
				"password": pass,
				"host":     "localhost:3306",
			}, extractOut)
			close(extractOut)
			assert.Nil(t, err)
		}()

		for val := range extractOut {
			expected := getExpectedVal()
			assert.Equal(t, expected, val)
		}

	})
}

func getExpectedVal() []meta.Table {
	return []meta.Table{
		{
			Urn:  "mockdata_meteor_metadata_test.applicant",
			Name: "applicant",
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
			Urn:  "mockdata_meteor_metadata_test.jobs",
			Name: "jobs",
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

func setup() (err error) {
	_, err = db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", testDB))
	if err != nil {
		return
	}
	_, err = db.Exec(fmt.Sprintf("CREATE DATABASE %s", testDB))
	if err != nil {
		return
	}
	_, err = db.Exec(fmt.Sprintf("USE %s;", testDB))
	if err != nil {
		return
	}
	table1 := "applicant"
	columns1 := "(applicant_id int, last_name varchar(255), first_name varchar(255))"
	table2 := "jobs"
	columns2 := "(job_id int, job varchar(255), department varchar(255))"
	err = createTable(table1, columns1)
	if err != nil {
		return
	}
	err = createTable(table2, columns2)
	if err != nil {
		return
	}
	_, err = db.Exec(fmt.Sprintf(`CREATE USER IF NOT EXISTS '%s'@'%%' IDENTIFIED BY '%s';`, user, pass))
	if err != nil {
		return
	}
	_, err = db.Exec(fmt.Sprintf(`GRANT ALL PRIVILEGES ON *.* TO '%s'@'%%';`, user))
	if err != nil {
		return
	}

	return
}

func createTable(table string, columns string) (err error) {
	query := "CREATE TABLE "
	_, err = db.Exec(query + table + columns + ";")
	if err != nil {
		return
	}
	values1 := "(1, 'test1', 'test11');"
	values2 := "(2, 'test2', 'test22');"
	err = populateTable(table, values1, db)
	if err != nil {
		return
	}
	err = populateTable(table, values2, db)
	if err != nil {
		return
	}

	return
}

func populateTable(table string, values string, db *sql.DB) (err error) {
	query := " INSERT INTO "
	completeQuery := query + table + " VALUES " + values
	_, err = db.Exec(completeQuery)
	if err != nil {
		return
	}
	return
}
