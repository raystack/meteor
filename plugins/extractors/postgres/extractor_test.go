//+build integration

package postgres_test

import (
	"fmt"
	"log"
	"os"
	"testing"

	"database/sql"

	_ "github.com/lib/pq"
	"github.com/odpf/meteor/plugins/extractors/postgres"
	"github.com/odpf/meteor/plugins/testutils"
	"github.com/odpf/meteor/proto/odpf/meta"
	"github.com/odpf/meteor/proto/odpf/meta/facets"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/assert"
)

var db *sql.DB

const (
	testDB = "mockdata_meteor_metadata_test"
	user   = "meteor_test_user"
	pass   = "pass"
	port   = "5432"
)

func TestMain(m *testing.M) {
	// setup test
	opts := dockertest.RunOptions{
		Repository: "postgres",
		Tag:        "12.3",
		Env: []string{
			"POSTGRES_USER=root",
			"POSTGRES_PASSWORD=pass",
			"POSTGRES_DB=postgres",
		},
		ExposedPorts: []string{"5432"},
		PortBindings: map[docker.Port][]docker.PortBinding{
			"5432": {
				{HostIP: "0.0.0.0", HostPort: port},
			},
		},
	}
	// exponential backoff-retry, because the application in the container might not be ready to accept connections yet
	retryFn := func() (err error) {
		db, err = sql.Open("postgres", "postgres://root:pass@localhost:5432/postgres?sslmode=disable")
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
	if err := purgeFn(); err != nil {
		log.Fatal(err)
	}
	os.Exit(code)
}

func TestExtract(t *testing.T) {
	t.Run("should return error if no user_id in config", func(t *testing.T) {
		extractor := new(postgres.Extractor)
		_, err := extractor.Extract(map[string]interface{}{
			"password": "pass",
			"host":     "localhost:5432",
		})

		assert.NotNil(t, err)
	})

	t.Run("should return error if no password in config", func(t *testing.T) {
		extractor := new(postgres.Extractor)
		_, err := extractor.Extract(map[string]interface{}{
			"user_id": user,
			"host":    "localhost:5432",
		})

		assert.NotNil(t, err)
	})

	t.Run("should return error if no host in config", func(t *testing.T) {
		extractor := new(postgres.Extractor)
		_, err := extractor.Extract(map[string]interface{}{
			"user_id":  user,
			"password": pass,
		})

		assert.NotNil(t, err)
	})

	t.Run("should not return error for root user without DB Name", func(t *testing.T) {
		extractor := new(postgres.Extractor)
		_, err := extractor.Extract(map[string]interface{}{
			"user_id":  "root",
			"password": "pass",
			"host":     "localhost:5432",
		})
		assert.Nil(t, err)
	})

	t.Run("should return mockdata we generated with postgres running on localhost", func(t *testing.T) {
		extractor := new(postgres.Extractor)
		result, err := extractor.Extract(map[string]interface{}{
			"user_id":       user,
			"password":      pass,
			"host":          "localhost:5432",
			"database_name": testDB,
		})
		if err != nil {
			t.Fatal(err)
		}
		expected := getExpectedVal()
		assert.Equal(t, expected, result)
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
						DataType:   "integer",
						Name:       "applicant_id",
						IsNullable: true,
						Length:     int64(0),
					},
					{
						DataType:   "character varying",
						Name:       "first_name",
						IsNullable: true,
						Length:     int64(255),
					},
					{
						DataType:   "character varying",
						Name:       "last_name",
						IsNullable: true,
						Length:     int64(255),
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
						DataType:   "character varying",
						Name:       "department",
						IsNullable: true,
						Length:     int64(255),
					},
					{
						DataType:   "character varying",
						Name:       "job",
						IsNullable: true,
						Length:     int64(255),
					},
					{
						DataType:   "integer",
						Name:       "job_id",
						IsNullable: true,
						Length:     int64(0),
					},
				},
			},
		},
	}
}

func setup() (err error) {
	err = setupDatabaseAndUser()
	if err != nil {
		return
	}
	err = populateMockData()
	if err != nil {
		return
	}

	return
}

func populateMockData() (err error) {
	userDB, err := sql.Open("postgres", fmt.Sprintf("postgres://%s:%s@localhost:5432/%s?sslmode=disable", user, pass, testDB))
	if err != nil {
		return
	}
	defer userDB.Close()

	table1 := "applicant"
	columns1 := "(applicant_id int, last_name varchar(255), first_name varchar(255))"
	table2 := "jobs"
	columns2 := "(job_id int, job varchar(255), department varchar(255))"
	err = createTable(userDB, table1, columns1)
	if err != nil {
		return
	}
	err = createTable(userDB, table2, columns2)
	if err != nil {
		return
	}
	return
}

func setupDatabaseAndUser() (err error) {
	_, err = db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", testDB))
	if err != nil {
		return
	}
	_, err = db.Exec(fmt.Sprintf("CREATE DATABASE %s", testDB))
	if err != nil {
		return
	}
	_, err = db.Exec(fmt.Sprintf(`DROP ROLE IF EXISTS "%s";`, user))
	if err != nil {
		return
	}
	_, err = db.Exec(fmt.Sprintf(`CREATE ROLE "%s" WITH SUPERUSER LOGIN PASSWORD '%s';`, user, pass))
	if err != nil {
		return
	}
	_, err = db.Exec(fmt.Sprintf(`SET ROLE "%s";`, user))
	if err != nil {
		return
	}

	return
}

func createTable(db *sql.DB, table string, columns string) (err error) {
	query := "CREATE TABLE "
	_, err = db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", table))
	if err != nil {
		return
	}
	_, err = db.Exec(query + table + columns + ";")
	if err != nil {
		return
	}
	values1 := "(1, 'test1', 'test11');"
	values2 := "(2, 'test2', 'test22');"
	err = populateTable(db, table, values1)
	if err != nil {
		return
	}
	err = populateTable(db, table, values2)
	if err != nil {
		return
	}

	return
}

func populateTable(db *sql.DB, table string, values string) (err error) {
	query := " INSERT INTO "
	completeQuery := query + table + " VALUES " + values
	_, err = db.Exec(completeQuery)
	if err != nil {
		return
	}
	return
}
