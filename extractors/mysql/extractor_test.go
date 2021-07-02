package mysql_test

import (
	"fmt"
	"testing"

	"database/sql"

	_ "github.com/go-sql-driver/mysql"
	"github.com/odpf/meteor/extractors/mysql"

	"github.com/stretchr/testify/assert"
)

const testDB = "mockdata_meteor_metadata_test"

func TestExtract(t *testing.T) {
	t.Run("should return error if no user_id in config", func(t *testing.T) {
		extractor := new(mysql.Extractor)
		_, err := extractor.Extract(map[string]interface{}{
			"password": "pass",
			"host":     "localhost:27017",
		})

		assert.NotNil(t, err)
	})

	t.Run("should return error if no password in config", func(t *testing.T) {
		extractor := new(mysql.Extractor)
		_, err := extractor.Extract(map[string]interface{}{
			"user_id": "user",
			"host":    "localhost:27017",
		})

		assert.NotNil(t, err)
	})

	t.Run("should return error if no host in config", func(t *testing.T) {
		extractor := new(mysql.Extractor)
		_, err := extractor.Extract(map[string]interface{}{
			"user_id":  "user",
			"password": "pass",
		})

		assert.NotNil(t, err)
	})

	t.Run("should return mockdata we generated with mysql running on localhost", func(t *testing.T) {
		extractor := new(mysql.Extractor)
		err := mockDataGenerator()
		if err != nil {
			t.Fatal(err)
		}
		result, err := extractor.Extract(map[string]interface{}{
			"user_id":  "user2",
			"password": "pass",
			"host":     "localhost:3306",
		})
		if err != nil {
			t.Fatal(err)
		}
		expected := getExpectedVal()
		assert.Equal(t, result, expected)
	})
}

func getExpectedVal() (expected []map[string]interface{}) {
	expected = []map[string]interface{}{
		{
			"columns": []map[string]interface{}{
				{
					"data_type":   "int",
					"field_desc":  "",
					"field_name":  "ApplicantID",
					"is_nullable": "YES",
					"length":      0,
				},
				{
					"data_type":   "varchar",
					"field_desc":  "",
					"field_name":  "LastName",
					"is_nullable": "YES",
					"length":      255,
				},
				{
					"data_type":   "varchar",
					"field_desc":  "",
					"field_name":  "FirstName",
					"is_nullable": "YES",
					"length":      255,
				},
			},
			"database_name": "mockdata_meteor_metadata_test",
			"table_name":    "applicant",
		},
		{
			"columns": []map[string]interface{}{
				{
					"data_type":   "int",
					"field_desc":  "",
					"field_name":  "JobID",
					"is_nullable": "YES",
					"length":      0,
				},
				{
					"data_type":   "varchar",
					"field_desc":  "",
					"field_name":  "Job",
					"is_nullable": "YES",
					"length":      255,
				},
				{
					"data_type":   "varchar",
					"field_desc":  "",
					"field_name":  "Department",
					"is_nullable": "YES",
					"length":      255,
				},
			},
			"database_name": "mockdata_meteor_metadata_test",
			"table_name":    "jobs",
		},
	}
	// expected = string(b)
	return
}

func mockDataGenerator() (err error) {
	db, err := sql.Open("mysql", "root:pass@tcp(127.0.0.1:3306)/")
	if err != nil {
		return
	}
	_, err = db.Exec("DROP DATABASE IF EXISTS " + testDB)
	if err != nil {
		return
	}
	_, err = db.Exec("CREATE DATABASE " + testDB)
	if err != nil {
		return
	}
	_, err = db.Exec(fmt.Sprintf("USE %s;", testDB))
	if err != nil {
		return
	}
	table1 := "applicant"
	var1 := "(ApplicantID int, LastName varchar(255), FirstName varchar(255))"
	table2 := "jobs"
	var2 := "(JobID int, Job varchar(255), Department varchar(255))"
	tableQuery := "CREATE TABLE "
	err = createTable(tableQuery, table1, var1, db)
	if err != nil {
		return
	}
	err = createTable(tableQuery, table2, var2, db)
	if err != nil {
		return
	}
	_, err = db.Exec("CREATE USER IF NOT EXISTS 'user2'@'172.25.0.1' IDENTIFIED BY 'pass';")
	if err != nil {
		return
	}
	_, err = db.Exec("GRANT ALL PRIVILEGES ON *.* TO 'user2'@'172.25.0.1';")
	if err != nil {
		return
	}
	defer db.Close()
	return
}

func createTable(query string, table string, columns string, db *sql.DB) (err error) {
	_, err = db.Exec(query + table + columns + ";")
	if err != nil {
		return
	}
	valueQuery := " INSERT INTO "
	values1 := "(1, 'test1', 'test11');"
	values2 := "(2, 'test2', 'test22');"
	err = populateTable(valueQuery, table, columns, values1, db)
	if err != nil {
		return
	}
	err = populateTable(valueQuery, table, columns, values2, db)
	if err != nil {
		return
	}

	return
}

func populateTable(query string, table string, columns string, value string, db *sql.DB) (err error) {
	completeQuery := query + table + " VALUES " + value
	_, err = db.Exec(completeQuery)
	if err != nil {
		return
	}
	return
}
