package postgres

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
	"github.com/odpf/meteor/core/extractor"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/proto/odpf/meta"
	"github.com/odpf/meteor/proto/odpf/meta/facets"
	"github.com/odpf/meteor/utils"
)

var defaultDBList = []string{
	"information_schema",
	"postgres",
	"root",
}

type Config struct {
	UserID       string `mapstructure:"user_id" validate:"required"`
	Password     string `mapstructure:"password" validate:"required"`
	Host         string `mapstructure:"host" validate:"required"`
	DatabaseName string `mapstructure:"database_name" default:"postgres"`
}

type Extractor struct {
	logger plugins.Logger
}

func (e *Extractor) Extract(ctx context.Context, configMap map[string]interface{}, out chan<- interface{}) (err error) {

	var config Config
	err = utils.BuildConfig(configMap, &config)
	if err != nil {
		return extractor.InvalidConfigError{}
	}

	db, err := sql.Open("postgres", fmt.Sprintf(
		"postgres://%s:%s@%s/%s?sslmode=disable",
		config.UserID, config.Password, config.Host, config.DatabaseName))
	if err != nil {
		return err
	}
	defer db.Close()

	result, err := e.getDatabases(db)
	if err != nil {
		return err
	}

	out <- result

	return nil
}

func (e *Extractor) getDatabases(db *sql.DB) (result []meta.Table, err error) {
	res, err := db.Query("SELECT datname FROM pg_database WHERE datistemplate = false;")
	if err != nil {
		return
	}
	for res.Next() {
		var database string
		res.Scan(&database)
		if checkNotDefaultDatabase(database) {
			result, err = e.getTablesInfo(db, database, result)
			if err != nil {
				return
			}
		}
	}
	return
}

func (e *Extractor) getTablesInfo(db *sql.DB, dbName string, result []meta.Table) (_ []meta.Table, err error) {
	sqlStr := `SELECT table_name
	FROM information_schema.tables
	WHERE table_schema = 'public'
	ORDER BY table_name;`
	_, err = db.Exec(fmt.Sprintf("SET search_path TO %s, public;", dbName))
	if err != nil {
		return
	}
	rows, err := db.Query(sqlStr)
	if err != nil {
		return
	}
	for rows.Next() {
		var tableName string
		err = rows.Scan(&tableName)
		if err != nil {
			return
		}
		var columns []*facets.Column
		columns, err = e.getColumns(db, dbName, tableName)
		if err != nil {
			return
		}

		result = append(result, meta.Table{
			Urn:  fmt.Sprintf("%s.%s", dbName, tableName),
			Name: tableName,
			Schema: &facets.Columns{
				Columns: columns,
			},
		})
	}
	return result, err
}

func (e *Extractor) getColumns(db *sql.DB, dbName string, tableName string) (result []*facets.Column, err error) {
	sqlStr := `SELECT COLUMN_NAME,DATA_TYPE,
				IS_NULLABLE,coalesce(CHARACTER_MAXIMUM_LENGTH,0)
				FROM information_schema.columns
				WHERE TABLE_NAME = '%s' ORDER BY COLUMN_NAME ASC;`
	rows, err := db.Query(fmt.Sprintf(sqlStr, tableName))
	if err != nil {
		return
	}
	for rows.Next() {
		var fieldName, dataType, isNullableString string
		var length int
		err = rows.Scan(&fieldName, &dataType, &isNullableString, &length)
		if err != nil {
			return
		}
		result = append(result, &facets.Column{
			Name:       fieldName,
			DataType:   dataType,
			IsNullable: e.isNullable(isNullableString),
			Length:     int64(length),
		})
	}
	return result, nil
}

func (e *Extractor) isNullable(value string) bool {
	return value == "YES"
}

func checkNotDefaultDatabase(database string) bool {
	for i := 0; i < len(defaultDBList); i++ {
		if database == defaultDBList[i] {
			return false
		}
	}
	return true
}

// Register the extractor to catalog
func init() {
	if err := extractor.Catalog.Register("postgres", &Extractor{
		logger: plugins.Log,
	}); err != nil {
		panic(err)
	}
}
