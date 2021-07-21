package postgres

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
	UserID       string `mapstructure:"user_id"`
	Password     string `mapstructure:"password"`
	Host         string `mapstructure:"host"`
	DatabaseName string `mapstructure:"database_name"`
}

var defaultDBList = []string{
	"information_schema",
	"postgres",
	"root",
}

type Extractor struct{}

func New() extractor.TableExtractor {
	return &Extractor{}
}

func (e *Extractor) Extract(c map[string]interface{}) (result []meta.Table, err error) {
	config, err := e.getConfig(c)
	if err != nil {
		return
	}
	err = e.validateConfig(config)
	if err != nil {
		return
	}

	db, err := sql.Open("postgres", fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable",
		config.UserID, config.Password, config.Host, config.DatabaseName))
	if err != nil {
		db.Close()
		return
	}
	defer db.Close()
	result, err = e.getDatabases(db)
	if err != nil {
		return
	}
	return
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
			result, _ = e.getTablesInfo(db, database, result)
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
		columns, err1 := e.getColumns(db, dbName, tableName)
		if err1 != nil {
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
		var fieldName, dataType string
		var isNull bool
		var length int
		err = rows.Scan(&fieldName, &dataType, &isNull, &length)
		if err != nil {
			return
		}
		result = append(result, &facets.Column{
			Name:       fieldName,
			DataType:   dataType,
			IsNullable: isNull,
			Length:     int64(length),
		})
	}
	return result, nil
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
	if config.DatabaseName == "" {
		config.DatabaseName = "postgres"
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
