package snowflake_test

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"testing"

	assetsv1beta1 "github.com/odpf/meteor/models/odpf/assets/v1beta1"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/plugins/extractors/mariadb"
	"github.com/odpf/meteor/test/mocks"
	"github.com/odpf/meteor/test/utils"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	_ "github.com/snowflakedb/gosnowflake" // used to register the snowflake driver
	"github.com/stretchr/testify/assert"
)

const (
	testDB = "test_db"
	user   = "test_user"
	pass   = "pass"
	port   = "8888"
)

var (
	host = "127.0.0.1:" + port
	db   *sql.DB
)

func TestMain(m *testing.M) {
	// setup test
	opts := dockertest.RunOptions{
		Repository: "codexmojo/bitbucket-snowsql", // zhoussen/snowtire-v2 or bbtv/snowsql
		Tag:        "latest",                      // snowpark-accelerator
		//Env:          []string{},
		ExposedPorts: []string{"8888"},
		PortBindings: map[docker.Port][]docker.PortBinding{
			"8888": {
				{HostIP: "0.0.0.0", HostPort: "8888"},
			},
		},
	}

	// Exponential backoff-retry for container to accept connections
	retryFn := func(r *dockertest.Resource) (err error) {
		// https://github.com/snowflakedb/gosnowflake/blob/e24bda449ced75324e8ce61377c88e4cea9c1efa/driver_test.go#L79
		// required : account, user, password
		dsn := fmt.Sprintf("snowflake@%s", host)
		db, err = sql.Open("snowflake", dsn)
		if err != nil {
			fmt.Println("1")
			return err
		}
		return db.Ping()
	}
	purgeFn, err := utils.CreateContainer(opts, retryFn)
	if err != nil {
		fmt.Println("2")
		log.Fatal(err)
	}
	if err := setup(); err != nil {
		fmt.Println("3")
		log.Fatal(err)
	}

	// Run tests
	code := m.Run()

	// Clean tests
	db.Close()
	if err := purgeFn(); err != nil {
		fmt.Println("4")
		log.Fatal(err)
	}
	os.Exit(code)
}

// TestInit tests the configs
func TestInit(t *testing.T) {
	t.Run("should return error for invalid config", func(t *testing.T) {
		err := mariadb.New(utils.Logger).Init(context.TODO(), map[string]interface{}{
			"invalid_config": "invalid_config_value",
		})
		assert.Equal(t, plugins.InvalidConfigError{}, err)
	})
}

// TestExtract tests that the extractor returns the expected result
func TestExtract(t *testing.T) {
	t.Run("should return mockdata we generated with mariadb", func(t *testing.T) {
		ctx := context.TODO()
		newExtractor := mariadb.New(utils.Logger)

		err := newExtractor.Init(ctx, map[string]interface{}{
			"connection_url": fmt.Sprintf("%s:%s@%s/", user, pass, host),
		})
		if err != nil {

			t.Fatal(err)
		}

		emitter := mocks.NewEmitter()
		err = newExtractor.Extract(ctx, emitter.Push)
		assert.NoError(t, err)

		var urns []string
		for _, record := range emitter.Get() {
			table := record.Data().(*assetsv1beta1.Table)
			urns = append(urns, table.Resource.Urn)

		}
		assert.Equal(t, []string{"test_db.applicant", "test_db.jobs"}, urns)
	})
}

// setup is a helper function to set up the test database
func setup() (err error) {
	var queries = []string{
		fmt.Sprintf("DROP DATABASE IF EXISTS %s", testDB),
		fmt.Sprintf("CREATE DATABASE %s", testDB),
		fmt.Sprintf("USE %s;", testDB),
		fmt.Sprintf(`CREATE USER IF NOT EXISTS '%s'@'%%' IDENTIFIED BY '%s';`, user, pass),
		fmt.Sprintf(`GRANT ALL PRIVILEGES ON *.* TO '%s'@'%%';`, user),
	}
	if err = execute(db, queries); err != nil {
		return
	}

	// create and populate tables
	var populate = []string{
		"CREATE TABLE applicant (applicant_id int, last_name varchar(255), first_name varchar(255));",
		"INSERT INTO applicant VALUES (1, 'test1', 'test11');",
		"CREATE TABLE jobs (job_id int, job varchar(255), department varchar(255));",
		"INSERT INTO jobs VALUES (2, 'test2', 'test22');",
	}
	if err = execute(db, populate); err != nil {
		return
	}
	return
}

// execute is a helper function to execute a list of queries
func execute(db *sql.DB, queries []string) (err error) {
	for _, query := range queries {
		if _, err = db.Exec(query); err != nil {
			return
		}
	}
	return
}
