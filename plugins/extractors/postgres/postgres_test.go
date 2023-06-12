//go:build plugins
// +build plugins

package postgres_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"

	v1beta2 "github.com/raystack/meteor/models/raystack/assets/v1beta2"
	"github.com/raystack/meteor/test/utils"
	ut "github.com/raystack/meteor/utils"

	"database/sql"

	_ "github.com/lib/pq"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/raystack/meteor/plugins"
	"github.com/raystack/meteor/plugins/extractors/postgres"
	"github.com/raystack/meteor/test/mocks"
	testUtils "github.com/raystack/meteor/test/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var db *sql.DB

const (
	user      = "test_user"
	pass      = "pass"
	port      = "5438"
	root      = "root"
	defaultDB = "postgres"
	urnScope  = "test-postgres"
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
	t.Run("should return error for invalid config", func(t *testing.T) {
		err := postgres.New(utils.Logger).Init(context.TODO(), plugins.Config{
			URNScope: urnScope,
			RawConfig: map[string]interface{}{
				"invalid_config": "invalid_config_value",
			}})

		assert.ErrorAs(t, err, &plugins.InvalidConfigError{})
	})
}

func TestExtract(t *testing.T) {
	t.Run("should return mockdata we generated with postgres", func(t *testing.T) {
		ctx := context.TODO()
		extr := postgres.New(utils.Logger)

		err := extr.Init(ctx, plugins.Config{
			URNScope: urnScope,
			RawConfig: map[string]interface{}{
				"connection_url": fmt.Sprintf("postgres://%s:%s@%s/postgres?sslmode=disable", user, pass, host),
			}})
		if err != nil {
			t.Fatal(err)
		}

		emitter := mocks.NewEmitter()
		err = extr.Extract(ctx, emitter.Push)
		require.NoError(t, err)

		testUtils.AssertAssetsWithJSON(t, getExpected(t), emitter.GetAllData())
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

func getExpected(t *testing.T) []*v1beta2.Asset {
	return []*v1beta2.Asset{
		{
			Urn:     "urn:postgres:test-postgres:table:test_db.article",
			Name:    "article",
			Service: "postgres",
			Type:    "table",
			Data: testUtils.BuildAny(t, &v1beta2.Table{
				Columns: []*v1beta2.Column{
					{
						Name:       "id",
						DataType:   "bigint",
						IsNullable: false,
						Length:     0,
					},
					{
						Name:       "name",
						DataType:   "character varying",
						IsNullable: false,
						Length:     20,
					},
				},
				Attributes: ut.TryParseMapToProto(map[string]interface{}{
					"grants": []interface{}{
						map[string]interface{}{
							"user":            "test_user",
							"privilege_types": []interface{}{"INSERT", "SELECT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER"},
						},
					},
				}),
			}),
		},
		{
			Urn:     "urn:postgres:test-postgres:table:test_db.post",
			Name:    "post",
			Service: "postgres",
			Type:    "table",
			Data: testUtils.BuildAny(t, &v1beta2.Table{
				Columns: []*v1beta2.Column{
					{
						Name:       "id",
						DataType:   "bigint",
						IsNullable: false,
						Length:     0,
					},
					{
						Name:       "title",
						DataType:   "character varying",
						IsNullable: false,
						Length:     20,
					},
				},
				Attributes: ut.TryParseMapToProto(map[string]interface{}{
					"grants": []interface{}{
						map[string]interface{}{
							"user":            "test_user",
							"privilege_types": []interface{}{"INSERT", "SELECT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER"},
						},
					},
				}),
			}),
		},
	}
}
