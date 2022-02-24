package presto_test

import (
	"context"
	"database/sql"
	"fmt"
	assetsv1beta1 "github.com/odpf/meteor/models/odpf/assets/v1beta1"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/plugins/extractors/presto"
	"github.com/odpf/meteor/test/mocks"
	"github.com/odpf/meteor/test/utils"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	_ "github.com/prestodb/presto-go-client/presto"
	"github.com/stretchr/testify/assert"
	"log"
	"os"
	"testing"
)

const (
	testDB = "test_db"
	user   = "test_user"
	pass   = "pass"
	port   = "8080"
)

var (
	host = "127.0.0.1:" + port
	db   *sql.DB
)

func TestMain(m *testing.M) {
	pwd, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}
	// setup test
	opts := dockertest.RunOptions{
		Repository: "ahanaio/prestodb-sandbox",
		Tag:        "latest",
		Mounts: []string{
			fmt.Sprintf("%s/etc:/etc:rw", pwd),
		},
		ExposedPorts: []string{"8080"},
		PortBindings: map[docker.Port][]docker.PortBinding{
			"8080": {
				{HostIP: "0.0.0.0", HostPort: "8080"},
			},
		},
	}

	// Exponential backoff-retry for container to accept connections
	retryFn := func(r *dockertest.Resource) (err error) {
		dsn := "http://presto@localhost:8080"
		//dsn - http[s]://user[:pass]@host[:port][?parameters]
		//dsn := fmt.Sprintf("http://%s:%s@%s?catalog=default&schema=%s", user, pass, host, testDB)
		db, err = sql.Open("presto", dsn)
		if err != nil {
			fmt.Println("33")
			return err
		}
		//if err = r.Expire(100); err != nil {
		//	log.Fatal(err)
		//}
		return db.Ping()
	}
	purgeFn, err := utils.CreateContainer(opts, retryFn)
	if err != nil {
		fmt.Println("1")
		log.Fatal(err)
	}
	if err := setup(); err != nil {
		fmt.Println("test1")
		log.Fatal(err)
	}

	// Run tests
	code := m.Run()

	// Clean tests
	if err = db.Close(); err != nil {
		return
	}
	if err := purgeFn(); err != nil {
		fmt.Println("12")

		log.Fatal(err)
	}
	os.Exit(code)
}

// TestInit tests the configs
func TestInit(t *testing.T) {
	t.Run("should return error for invalid config", func(t *testing.T) {
		err := presto.New(utils.Logger).Init(context.TODO(), map[string]interface{}{
			"invalid_config": "invalid_config_value",
		})
		assert.Equal(t, plugins.InvalidConfigError{}, err)
	})
}

// TestExtract tests that the extractor returns the expected result
func TestExtract(t *testing.T) {
	t.Run("should return mock data we generated with presto", func(t *testing.T) {
		ctx := context.TODO()
		newExtractor := presto.New(utils.Logger)

		err := newExtractor.Init(ctx, map[string]interface{}{
			"connection_url": fmt.Sprintf("%s:%s@tcp(%s)/", user, pass, host),
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
	fmt.Println("setup enter")

	// querie for sample testing
	var querie = []string{
		"CREATE TABLE memory.default.nation AS SELECT * from tpch.tiny.nation;",
		"INSERT INTO memory.default.nation SELECT * FROM tpch.tiny.nation;",
		"SELECT * FROM memory.default.nation;",
	}
	//var queries = []string{
	//fmt.Sprintf("SHOW %s", testDB),
	//fmt.Sprintf("SELECT * FROM %s", testDB),
	//fmt.Sprintf("DROP SCHEMA IF EXISTS %s", testDB),
	//fmt.Sprintf("CREATE SCHEMA %s", testDB),
	//fmt.Sprintf("USE %s;", testDB),
	//fmt.Sprintf(`CREATE USER IF NOT EXISTS '%s'@'%%' IDENTIFIED BY '%s';`, user, pass),
	//fmt.Sprintf(`GRANT ALL PRIVILEGES ON *.* TO '%s'@'%%';`, user),
	//}
	if err = execute(db, querie); err != nil {
		fmt.Println("exec 1")
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
		fmt.Println("exec 2")
		return
	}
	return
}

// execute is a helper function to execute a list of queries
func execute(db *sql.DB, queries []string) (err error) {
	for _, query := range queries {
		_, err = db.Exec(query) // Exec not supported by presto client
		if err != nil {
			fmt.Println(err)
			return
		}
	}
	return
}
