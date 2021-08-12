//+ build integration

package postgres_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"

	"database/sql"

	_ "github.com/lib/pq"
	"github.com/odpf/meteor/plugins"
	_ "github.com/odpf/meteor/plugins/extractors/postgres"
	"github.com/odpf/meteor/plugins/testutils"
	"github.com/odpf/meteor/proto/odpf/meta"
	"github.com/odpf/meteor/registry"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/assert"
)

var db *sql.DB

const (
	testDB = "test_db"
	user   = "test_user"
	pass   = "pass"
	port   = "5432"
)

func TestMain(m *testing.M) {
	opts := dockertest.RunOptions{
		Repository:   "postgres",
		Tag:          "12.3",
		Env:          []string{"POSTGRES_USER=root", "POSTGRES_PASSWORD=pass", "POSTGRES_DB=postgres"},
		ExposedPorts: []string{"5432"},
		PortBindings: map[docker.Port][]docker.PortBinding{"5432": {{HostIP: "0.0.0.0", HostPort: port}}},
	}

	// Exponential backoff-retry for container to be resy to accept connections
	retryFn := func(r *dockertest.Resource) (err error) {
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
		extr, _ := registry.Extractors.Get("postgres")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		err := extr.Extract(ctx, map[string]interface{}{
			"password": "pass",
			"host":     "localhost:5432",
		}, make(chan interface{}))

		assert.Equal(t, plugins.InvalidConfigError{}, err)
	})

	t.Run("should return mockdata we generated with postgres running on localhost", func(t *testing.T) {
		extr, _ := registry.Extractors.Get("postgres")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		extractOut := make(chan interface{})

		go func() {
			extr.Extract(ctx, map[string]interface{}{
				"user_id":       user,
				"password":      pass,
				"host":          "localhost:5432",
				"database_name": testDB,
			}, extractOut)
			close(extractOut)
		}()

		var urns []string
		for val := range extractOut {
			urns = append(urns, val.(*meta.Table).Urn)

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

	userDB, err := sql.Open("postgres", fmt.Sprintf("postgres://%s:%s@localhost:5432/%s?sslmode=disable", user, pass, testDB))
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
