package clickhouse_test

import (
	"fmt"
	"log"
	"os"
	"testing"

	"database/sql"

	_ "github.com/ClickHouse/clickhouse-go"
	"github.com/odpf/meteor/core/extractor"
	"github.com/odpf/meteor/plugins/extractors/clickhouse"
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
	port       = "9000"
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
		ExposedPorts: []string{"9000"},
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
		db, err = sql.Open("clickhouse", fmt.Sprintf("tcp://127.0.0.1:%s?username=default&password=pass&debug=true", port))
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
		extr := new(clickhouse.Extractor)
		_, err := extr.Extract(map[string]interface{}{
			"password": pass,
			"host":     "127.0.0.1:9000",
		})

		assert.Equal(t, extractor.InvalidConfigError{}, err)
	})

	t.Run("should return error if no password in config", func(t *testing.T) {
		extr := new(clickhouse.Extractor)
		_, err := extr.Extract(map[string]interface{}{
			"user_id": user,
			"host":    "127.0.0.1:9000",
		})

		assert.Equal(t, extractor.InvalidConfigError{}, err)
	})

	t.Run("should return error if no host in config", func(t *testing.T) {
		extr := new(clickhouse.Extractor)
		_, err := extr.Extract(map[string]interface{}{
			"user_id":  user,
			"password": pass,
		})

		assert.Equal(t, extractor.InvalidConfigError{}, err)
	})

	t.Run("should return mockdata we generated with clickhouse running on localhost", func(t *testing.T) {
		extractor := new(clickhouse.Extractor)
		result, err := extractor.Extract(map[string]interface{}{
			"user_id":  "default",
			"password": pass,
			"host":     "127.0.0.1:9000",
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
			Urn:  "mockdata_meteor_metadata_test.jobs",
			Name: "jobs",
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
	return
}

func createTable(table string, columns string) (err error) {
	query := "CREATE TABLE IF NOT EXISTS "
	_, err = db.Exec(query + table + columns + " engine=Memory")
	if err != nil {
		return
	}
	return
}
