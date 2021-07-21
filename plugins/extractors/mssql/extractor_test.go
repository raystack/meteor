//+build integration

package mssql_test

import (
	"fmt"
	"testing"

	"database/sql"

	_ "github.com/denisenkom/go-mssqldb"
	"github.com/odpf/meteor/plugins/extractors/mssql"
	"github.com/odpf/meteor/proto/odpf/meta"
	"github.com/odpf/meteor/proto/odpf/meta/facets"

	"github.com/stretchr/testify/assert"
)

const testDB = "mockdata_meteor_metadata_test"
const user = "sa"
const pass = "P@ssword1234"

func TestExtract(t *testing.T) {
	t.Run("should return error if no user_id in config", func(t *testing.T) {
		extractor := new(mssql.Extractor)
		_, err := extractor.Extract(map[string]interface{}{
			"password": "pass",
			"host":     "localhost:1433",
		})
		assert.NotNil(t, err)
	})

	t.Run("should return error if no password in config", func(t *testing.T) {
		extractor := new(mssql.Extractor)
		_, err := extractor.Extract(map[string]interface{}{
			"user_id": user,
			"host":    "localhost:1433",
		})

		assert.NotNil(t, err)
	})

	t.Run("should return error if no host in config", func(t *testing.T) {
		extractor := new(mssql.Extractor)
		_, err := extractor.Extract(map[string]interface{}{
			"user_id":  user,
			"password": pass,
		})

		assert.NotNil(t, err)
	})

	t.Run("should return mockdata we generated with mysql running on localhost", func(t *testing.T) {
		err, cleanUp := setup()
		if err != nil {
			t.Fatal(err)
		}
		defer cleanUp()

		extractor := new(mssql.Extractor)
		result, err := extractor.Extract(map[string]interface{}{
			"user_id":  user,
			"password": pass,
			"host":     "localhost:1433",
		})
		if err != nil {
			t.Fatal(err)
		}
		expected := getExpectedVal()
		assert.Equal(t, expected, result)
	})
}

func getExpectedVal() (expected []meta.Table) {
	return []meta.Table{
		{
			Urn:  "mockdata_meteor_metadata_test.applicant",
			Name: "applicant",
			Schema: &facets.Columns{
				Columns: []*facets.Column{
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
			},
		},
		{
			Urn:  "mockdata_meteor_metadata_test.jobs",
			Name: "jobs",
			Schema: &facets.Columns{
				Columns: []*facets.Column{
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
			},
		},
	}
}

func setup() (err error, cleanUp func()) {
	db, err := sql.Open("mssql", "sqlserver://sa:P@ssword1234@localhost:1433/")
	if err != nil {
		return
	}
	cleanUp = func() {
		cleanDatabase(db)
		db.Close()
	}

	_, err = db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s;", testDB))
	if err != nil {
		return
	}
	_, err = db.Exec(fmt.Sprintf("CREATE DATABASE %s;", testDB))
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
	err = createTable(db, table1, columns1)
	if err != nil {
		return
	}
	err = createTable(db, table2, columns2)
	if err != nil {
		return
	}
	return
}

func createTable(db *sql.DB, table string, columns string) (err error) {
	query := "CREATE TABLE "
	tableSchema := testDB + ".dbo." + table
	_, err = db.Exec(query + tableSchema + columns + ";")
	if err != nil {
		return
	}
	values1 := "(1, 'test1', 'test11');"
	values2 := "(2, 'test2', 'test22');"
	err = populateTable(db, tableSchema, values1)
	if err != nil {
		return
	}
	err = populateTable(db, tableSchema, values2)
	if err != nil {
		return
	}

	return
}

func populateTable(db *sql.DB, table string, values string) (err error) {
	query := "INSERT INTO "
	completeQuery := query + table + " VALUES " + values
	_, err = db.Exec(completeQuery)
	if err != nil {
		return
	}
	return
}

func cleanDatabase(db *sql.DB) (err error) {
	_, err = db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", testDB))
	if err != nil {
		return
	}
	db.Close()
	return
}
