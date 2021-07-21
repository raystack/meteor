package mysql_test

import (
	"fmt"
	"testing"

	"database/sql"

	_ "github.com/go-sql-driver/mysql"
	"github.com/odpf/meteor/plugins/extractors/mysql"
	"github.com/odpf/meteor/proto/odpf/meta"
	"github.com/odpf/meteor/proto/odpf/meta/facets"
	"github.com/stretchr/testify/assert"
)

const testDB = "mockdata_meteor_metadata_test"
const user = "meteor_test_user"
const pass = "pass"
const globalhost = "%"

func TestExtract(t *testing.T) {
	t.Run("should return error if no user_id in config", func(t *testing.T) {
		extractor := new(mysql.Extractor)
		_, err := extractor.Extract(map[string]interface{}{
			"password": "pass",
			"host":     "localhost:3306",
		})

		assert.NotNil(t, err)
	})

	t.Run("should return error if no password in config", func(t *testing.T) {
		extractor := new(mysql.Extractor)
		_, err := extractor.Extract(map[string]interface{}{
			"user_id": user,
			"host":    "localhost:3306",
		})

		assert.NotNil(t, err)
	})

	t.Run("should return error if no host in config", func(t *testing.T) {
		extractor := new(mysql.Extractor)
		_, err := extractor.Extract(map[string]interface{}{
			"user_id":  user,
			"password": pass,
		})

		assert.NotNil(t, err)
	})

	t.Run("should return mockdata we generated with mysql running on localhost", func(t *testing.T) {
		err, cleanUp := mockDataGenerator()
		if err != nil {
			t.Fatal(err)
		}
		defer cleanUp()

		extractor := new(mysql.Extractor)
		result, err := extractor.Extract(map[string]interface{}{
			"user_id":  user,
			"password": pass,
			"host":     "localhost:3306",
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
						DataType:    "int",
						Description: "",
						IsNullable:  true,
						Length:      0,
					},
					{
						Name:        "first_name",
						DataType:    "varchar",
						Description: "",
						IsNullable:  true,
						Length:      255,
					},
					{
						Name:        "last_name",
						DataType:    "varchar",
						Description: "",
						IsNullable:  true,
						Length:      255,
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
						Name:        "department",
						DataType:    "varchar",
						Description: "",
						IsNullable:  true,
						Length:      255,
					},
					{
						Name:        "job",
						DataType:    "varchar",
						Description: "",
						IsNullable:  true,
						Length:      255,
					},
					{
						Name:        "job_id",
						DataType:    "int",
						Description: "",
						IsNullable:  true,
						Length:      0,
					},
				},
			},
		},
	}
}

func mockDataGenerator() (err error, cleanUp func()) {
	db, err := sql.Open("mysql", "root@tcp(localhost:3306)/")
	if err != nil {
		return
	}
	cleanUp = func() {
		cleanDatabase(db)
		db.Close()
	}

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
	err = createTable(db, table1, columns1)
	if err != nil {
		return
	}
	err = createTable(db, table2, columns2)
	if err != nil {
		return
	}
	_, err = db.Exec(fmt.Sprintf(`CREATE USER IF NOT EXISTS '%s'@'%%' IDENTIFIED BY '%s';`, user, pass))
	if err != nil {
		return
	}
	_, err = db.Exec(fmt.Sprintf(`GRANT ALL PRIVILEGES ON *.* TO '%s'@'%%';`, user))
	if err != nil {
		return
	}

	return
}

func createTable(db *sql.DB, table string, columns string) (err error) {
	query := "CREATE TABLE "
	_, err = db.Exec(query + table + columns + ";")
	if err != nil {
		return
	}
	values1 := "(1, 'test1', 'test11');"
	values2 := "(2, 'test2', 'test22');"
	err = populateTable(table, values1, db)
	if err != nil {
		return
	}
	err = populateTable(table, values2, db)
	if err != nil {
		return
	}

	return
}

func populateTable(table string, values string, db *sql.DB) (err error) {
	query := " INSERT INTO "
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
	_, err = db.Exec(fmt.Sprintf("DROP USER IF EXISTS %s@%s", user, globalhost))
	if err != nil {
		return
	}
	db.Close()
	return
}
