//go:build plugins
// +build plugins

package postgres_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/raystack/meteor/models"
	meteorv1beta1 "github.com/raystack/meteor/models/raystack/meteor/v1beta1"
	"github.com/raystack/meteor/test/utils"

	"database/sql"

	_ "github.com/lib/pq"
	"github.com/ory/dockertest/v3"
	"github.com/raystack/meteor/plugins"
	"github.com/raystack/meteor/plugins/extractors/postgres"
	"github.com/raystack/meteor/test/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	db              *sql.DB
	host            string
	dockerAvailable bool
)

const (
	user      = "test_user"
	pass      = "pass"
	root      = "root"
	defaultDB = "postgres"
	urnScope  = "test-postgres"
)

func TestMain(m *testing.M) {
	dockerAvailable = utils.CheckDockerAvailability()
	if !dockerAvailable {
		os.Exit(m.Run())
	}

	opts := dockertest.RunOptions{
		Repository:   "postgres",
		Tag:          "12.3",
		Env:          []string{"POSTGRES_USER=" + root, "POSTGRES_PASSWORD=" + pass, "POSTGRES_DB=" + defaultDB},
		ExposedPorts: []string{"5432"},
	}

	// Exponential backoff-retry for container to be ready to accept connections
	retryFn := func(r *dockertest.Resource) (err error) {
		host = r.GetHostPort("5432/tcp")
		db, err = sql.Open("postgres", fmt.Sprintf("postgres://root:%s@%s/%s?sslmode=disable", pass, host, defaultDB))
		if err != nil {
			return err
		}
		return db.Ping()
	}
	purgeFn, err := utils.CreateContainer(opts, retryFn)
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

func TestInit(t *testing.T) {
	utils.SkipIfNoDocker(t, dockerAvailable)
	t.Run("should return error for invalid config", func(t *testing.T) {
		err := postgres.New(utils.Logger).Init(context.TODO(), plugins.Config{
			URNScope: urnScope,
			RawConfig: map[string]any{
				"invalid_config": "invalid_config_value",
			}})

		assert.ErrorAs(t, err, &plugins.InvalidConfigError{})
	})
}

func TestExtract(t *testing.T) {
	utils.SkipIfNoDocker(t, dockerAvailable)
	t.Run("should return mockdata we generated with postgres", func(t *testing.T) {
		ctx := context.TODO()
		extr := postgres.New(utils.Logger)

		err := extr.Init(ctx, plugins.Config{
			URNScope: urnScope,
			RawConfig: map[string]any{
				"connection_url": fmt.Sprintf("postgres://%s:%s@%s/postgres?sslmode=disable", user, pass, host),
			}})
		if err != nil {
			t.Fatal(err)
		}

		emitter := mocks.NewEmitter()
		err = extr.Extract(ctx, emitter.Push)
		require.NoError(t, err)

		utils.AssertEqualProtos(t, getExpected(t), emitter.GetAllEntities())
	})
}

func setup() (err error) {
	testDB := "test_db"

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

func getExpected(t *testing.T) []*meteorv1beta1.Entity {
	return []*meteorv1beta1.Entity{
		models.NewEntity("urn:postgres:test-postgres:table:test_db.article", "table", "article", "postgres", map[string]any{
			"columns": []any{
				map[string]any{"name": "id", "data_type": "bigint", "is_nullable": false},
				map[string]any{"name": "name", "data_type": "character varying", "is_nullable": false, "length": float64(20)},
			},
			"grants": []any{
				map[string]any{
					"user":            "test_user",
					"privilege_types": []any{"INSERT", "SELECT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER"},
				},
			},
		}),
		models.NewEntity("urn:postgres:test-postgres:table:test_db.post", "table", "post", "postgres", map[string]any{
			"columns": []any{
				map[string]any{"name": "id", "data_type": "bigint", "is_nullable": false},
				map[string]any{"name": "title", "data_type": "character varying", "is_nullable": false, "length": float64(20)},
			},
			"grants": []any{
				map[string]any{
					"user":            "test_user",
					"privilege_types": []any{"INSERT", "SELECT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER"},
				},
			},
		}),
	}
}
