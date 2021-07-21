package mysql

import (
	"database/sql"
	"errors"
	"fmt"

	"github.com/mitchellh/mapstructure"
	"github.com/odpf/meteor/core/extractor"
	"github.com/odpf/meteor/proto/odpf/meta"
	"github.com/odpf/meteor/proto/odpf/meta/facets"
)

type Config struct {
	UserID   string `mapstructure:"user_id"`
	Password string `mapstructure:"password"`
	Host     string `mapstructure:"host"`
}

var defaultDBList = []string{
	"information_schema",
	"mysql",
	"performance_schema",
	"sys",
}

type Extractor struct{}

func New() extractor.TableExtractor {
	return &Extractor{}
}

func (e *Extractor) Extract(configMap map[string]interface{}) (result []meta.Table, err error) {
	config, err := e.getConfig(configMap)
	if err != nil {
		return
	}
	err = e.validateConfig(config)
	if err != nil {
		return
	}
	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s)/", config.UserID, config.Password, config.Host))
	if err != nil {
		return
	}
	result, err = e.getDatabases(db)
	return
}

func (e *Extractor) getDatabases(db *sql.DB) (result []meta.Table, err error) {
	res, err := db.Query("SHOW DATABASES;")
	if err != nil {
		return
	}
	for res.Next() {
		var database string
		res.Scan(&database)
		if checkNotDefaultDatabase(database) {
			result, err = e.getTablesInfo(database, result, db)
			if err != nil {
				return
			}
		}
	}
	return
}

func (e *Extractor) getTablesInfo(dbName string, result []meta.Table, db *sql.DB) (_ []meta.Table, err error) {
	sqlStr := "SHOW TABLES;"
	_, err = db.Exec(fmt.Sprintf("USE %s;", dbName))
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
		columns, err = e.getColumns(dbName, tableName, db)
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

func (e *Extractor) getColumns(dbName string, tableName string, db *sql.DB) (result []*facets.Column, err error) {
	sqlStr := `SELECT COLUMN_NAME,column_comment,DATA_TYPE,
				IS_NULLABLE,IFNULL(CHARACTER_MAXIMUM_LENGTH,0)
				FROM information_schema.columns
				WHERE table_name = ?
				ORDER BY COLUMN_NAME ASC`

	rows, err := db.Query(sqlStr, tableName)
	if err != nil {
		return
	}
	for rows.Next() {
		var fieldName, fieldDesc, dataType, isNullableString string
		var length int
		err = rows.Scan(&fieldName, &fieldDesc, &dataType, &isNullableString, &length)
		if err != nil {
			return
		}
		result = append(result, &facets.Column{
			Name:        fieldName,
			DataType:    dataType,
			Description: fieldDesc,
			IsNullable:  e.isNullable(isNullableString),
			Length:      int64(length),
		})
	}
	return result, nil
}

func (e *Extractor) isNullable(value string) bool {
	if value == "YES" {
		return true
	}

	return false
}

func (e *Extractor) getConfig(configMap map[string]interface{}) (config Config, err error) {
	err = mapstructure.Decode(configMap, &config)
	return
}

func (e *Extractor) validateConfig(config Config) (err error) {
	if config.UserID == "" {
		return errors.New("user_id is required")
	}
	if config.Password == "" {
		return errors.New("password is required")
	}
	if config.Host == "" {
		return errors.New("host address is required")
	}
	return
}

func checkNotDefaultDatabase(database string) bool {
	for i := 0; i < len(defaultDBList); i++ {
		if database == defaultDBList[i] {
			return false
		}
	}
	return true
}
