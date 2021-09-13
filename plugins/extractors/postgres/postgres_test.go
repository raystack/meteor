//+build integration

package postgres_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"

	"database/sql"

	_ "github.com/lib/pq"
	"github.com/odpf/meteor/models"
	"github.com/odpf/meteor/models/odpf/assets"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/plugins/extractors/postgres"
	"github.com/odpf/meteor/test"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/assert"
)

var db *sql.DB

const (
	testDB    = "test_db"
	user      = "test_user"
	pass      = "pass"
	port      = "5438"
	root      = "root"
	defaultDB = "postgres"
)

var host = "localhost:" + port

func TestMain(m *testing.M) {
	opts := dockertest.RunOptions{
		Repository:   "postgres",
		Tag:          "12.3",
		Env:          []string{"POSTGRES_USER=" + root, "POSTGRES_PASSWORD=" + pass, "POSTGRES_DB=" + defaultDB},
		ExposedPorts: []string{port, "5432"},
		PortBindings: map[docker.Port][]docker.PortBinding{"5432": {{HostIP: "0.0.0.0", HostPort: port}}},
	}

	// Exponential backoff-retry for container to be resy to accept connections
	retryFn := func(r *dockertest.Resource) (err error) {
		db, err = sql.Open("postgres", fmt.Sprintf("postgres://root:%s@%s/%s?sslmode=disable", pass, host, defaultDB))
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

	// Run tests
	code := m.Run()

	// Clean tests
	db.Close()
	if err := purgeFn(); err != nil {
		log.Fatal(err)
	}
	os.Exit(code)
}

func TestExtract(t *testing.T) {
	t.Run("should return error for invalid config", func(t *testing.T) {
		err := newExtractor().Extract(context.TODO(), map[string]interface{}{
			"password": "pass",
			"host":     host,
		}, make(chan<- models.Record))

		assert.Equal(t, plugins.InvalidConfigError{}, err)
	})

	t.Run("should return mockdata we generated with postgres", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		extractOut := make(chan models.Record)

		go func() {
			err := newExtractor().Extract(ctx, map[string]interface{}{
				"user_id":       user,
				"password":      pass,
				"host":          host,
				"database_name": testDB,
			}, extractOut)
			close(extractOut)

			assert.Nil(t, err)
		}()

		var urns []string
		for val := range extractOut {
			urns = append(urns, val.Data().(*assets.Table).Resource.Urn)

		}
		assert.Equal(t, []string{"test_db.article", "test_db.post"}, urns)
	})
}

func setup() (err error) {

	var queries = []string{
		fmt.Sprintf("DROP DATABASE IF EXISTS %s", testDB),
		fmt.Sprintf("CREATE DATABASE %s", testDB),
		fmt.Sprintf(`DROP ROLE IF EXISTS "%s";`, user),
		fmt.Sprintf(`CREATE ROLE "%s" WITH SUPERUSER LOGIN PASSWORD '%s';`, user, pass),
		fmt.Sprintf(`SET ROLE "%s";`, user),
	}
	err = execute(db, queries)

	userDB, err := sql.Open("postgres", fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable", user, pass, host, testDB))
	if err != nil {
		return
	}
	defer userDB.Close()

	var populate = []string{
		"CREATE TABLE article (id bigserial primary key,name varchar(20) NOT NULL);",
		"CREATE TABLE post (id bigserial primary key,title varchar(20) NOT NULL);",
	}
	err = execute(userDB, populate)

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

func newExtractor() *postgres.Extractor {
	return postgres.New(test.Logger)
}
