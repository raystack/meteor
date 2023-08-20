//go:build plugins
// +build plugins

package mssql_test

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"testing"

	_ "github.com/denisenkom/go-mssqldb"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	v1beta2 "github.com/raystack/meteor/models/raystack/assets/v1beta2"
	"github.com/raystack/meteor/plugins"
	"github.com/raystack/meteor/plugins/extractors/mssql"
	"github.com/raystack/meteor/test/mocks"
	"github.com/raystack/meteor/test/utils"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"
)

const (
	testDB   = "mockdata_meteor_metadata_test"
	user     = "sa"
	pass     = "P@ssword1234"
	port     = "1433"
	urnScope = "test-mssql"
)

var host = "localhost:" + port

var db *sql.DB

func TestMain(m *testing.M) {
	// setup test
	opts := dockertest.RunOptions{
		Repository: "mcr.microsoft.com/mssql/server",
		Tag:        "2022-latest",
		Platform:   "linux/amd64",
		Env: []string{
			"SA_PASSWORD=" + pass,
			"ACCEPT_EULA=Y",
		},
		ExposedPorts: []string{port},
		PortBindings: map[docker.Port][]docker.PortBinding{
			port: {
				{HostIP: "0.0.0.0", HostPort: port},
			},
		},
	}
	retryFn := func(resource *dockertest.Resource) (err error) {
		db, err = sql.Open("mssql", fmt.Sprintf("sqlserver://%s:%s@%s/", user, pass, host))
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

	// run tests
	code := m.Run()

	// clean tests
	db.Close()
	if err := purgeFn(); err != nil {
		log.Fatal(err)
	}
	os.Exit(code)
}

func TestInit(t *testing.T) {
	t.Run("should error for invalid configurations", func(t *testing.T) {
		err := mssql.New(utils.Logger).Init(context.TODO(), plugins.Config{
			URNScope: urnScope,
			RawConfig: map[string]interface{}{
				"invalid_config": "invalid_config_value",
			},
		})

		assert.ErrorAs(t, err, &plugins.InvalidConfigError{})
	})
}

func TestExtract(t *testing.T) {
	t.Run("should extract and output tables metadata along with its columns", func(t *testing.T) {
		ctx := context.TODO()
		extr := mssql.New(utils.Logger)

		err := extr.Init(ctx, plugins.Config{
			URNScope: urnScope,
			RawConfig: map[string]interface{}{
				"connection_url": fmt.Sprintf("sqlserver://%s:%s@%s/", user, pass, host),
			},
		})
		if err != nil {
			t.Fatal(err)
		}

		emitter := mocks.NewEmitter()
		err = extr.Extract(ctx, emitter.Push)

		assert.NoError(t, err)
		utils.AssertEqualProtos(t, getExpected(t), emitter.GetAllData())
	})
}

func setup() (err error) {
	err = execute(db, []string{
		fmt.Sprintf("DROP DATABASE IF EXISTS %s;", testDB),
		fmt.Sprintf("CREATE DATABASE %s;", testDB),
		fmt.Sprintf("USE %s;", testDB),
	})
	if err != nil {
		return
	}

	err = execute(db, []string{
		fmt.Sprintf("CREATE TABLE %s.dbo.applicant (applicant_id int, last_name varchar(255), first_name varchar(255));", testDB),
		fmt.Sprintf("INSERT INTO %s.dbo.applicant VALUES (1, 'test1', 'test11');", testDB),
		fmt.Sprintf("CREATE TABLE %s.dbo.jobs (job_id int, job varchar(255), department varchar(255));", testDB),
		fmt.Sprintf("INSERT INTO %s.dbo.jobs VALUES (2, 'test2', 'test22');", testDB),
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

func getExpected(t *testing.T) []*v1beta2.Asset {
	data1, err := anypb.New(&v1beta2.Table{
		Columns: []*v1beta2.Column{
			{
				DataType:   "int",
				Name:       "applicant_id",
				IsNullable: true,
				Length:     0,
			},
			{
				DataType:   "varchar",
				Name:       "first_name",
				IsNullable: true,
				Length:     255,
			},
			{
				DataType:   "varchar",
				Name:       "last_name",
				IsNullable: true,
				Length:     255,
			},
		},
		Attributes: &structpb.Struct{},
	})
	if err != nil {
		t.Fatal(err, "failed to build Any struct")
	}
	data2, err := anypb.New(&v1beta2.Table{
		Columns: []*v1beta2.Column{
			{
				DataType:   "varchar",
				Name:       "department",
				IsNullable: true,
				Length:     255,
			},
			{
				DataType:   "varchar",
				Name:       "job",
				IsNullable: true,
				Length:     255,
			},
			{
				DataType:   "int",
				Name:       "job_id",
				IsNullable: true,
				Length:     0,
			},
		},
		Attributes: &structpb.Struct{},
	})
	if err != nil {
		t.Fatal(err, "failed to build Any struct")
	}
	return []*v1beta2.Asset{
		{
			Urn:     "urn:mssql:test-mssql:table:mockdata_meteor_metadata_test.applicant",
			Name:    "applicant",
			Type:    "table",
			Data:    data1,
			Service: "mssql",
		},
		{
			Urn:     "urn:mssql:test-mssql:table:mockdata_meteor_metadata_test.jobs",
			Name:    "jobs",
			Type:    "table",
			Data:    data2,
			Service: "mssql",
		},
	}
}
