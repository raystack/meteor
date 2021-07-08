package mssql

import (
	"database/sql"
	"errors"
	"fmt"

	"github.com/mitchellh/mapstructure"
)

type Config struct {
	UserID   string `mapstructure:"user_id"`
	Password string `mapstructure:"password"`
	Host     string `mapstructure:"host"`
}

var defaultDBList = []string{
	"master",
	"msdb",
	"model",
	"tempdb",
}

type Extractor struct{}

func (e *Extractor) Extract(configMap map[string]interface{}) (result []map[string]interface{}, err error) {
	config, err := e.getConfig(configMap)
	if err != nil {
		return
	}
	err = e.validateConfig(config)
	if err != nil {
		return
	}
	db, err := sql.Open("mssql", fmt.Sprintf("sqlserver://%s:%s@%s/", config.UserID, config.Password, config.Host))
	if err != nil {
		return
	}
	defer db.Close()
	result, err = e.getDatabases(db)
	if err != nil {
		return
	}
	return
}

func (e *Extractor) getDatabases(db *sql.DB) (result []map[string]interface{}, err error) {
	res, err := db.Query("SELECT name FROM sys.databases;")
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

func (e *Extractor) getTablesInfo(db *sql.DB, dbName string, result []map[string]interface{}) (_ []map[string]interface{}, err error) {
	sqlStr := `SELECT TABLE_NAME FROM %s.INFORMATION_SCHEMA.TABLES 
				WHERE TABLE_TYPE = 'BASE TABLE';`
	_, err = db.Exec(fmt.Sprintf("USE %s;", dbName))
	if err != nil {
		return
	}
	rows, err := db.Query(fmt.Sprintf(sqlStr, dbName))
	if err != nil {
		return
	}
	for rows.Next() {
		var tableName string
		err = rows.Scan(&tableName)
		if err != nil {
			return
		}
		columns, err1 := e.getTablesFieldInfo(db, dbName, tableName)
		if err1 != nil {
			return
		}
		tableData := make(map[string]interface{})
		tableData["database_name"] = dbName
		tableData["table_name"] = tableName
		tableData["columns"] = columns
		result = append(result, tableData)
	}
	return result, err
}

func (e *Extractor) getTablesFieldInfo(db *sql.DB, dbName string, tableName string) (result []map[string]interface{}, err error) {
	sqlStr := `SELECT COLUMN_NAME,DATA_TYPE,
				IS_NULLABLE,coalesce(CHARACTER_MAXIMUM_LENGTH,0)
				FROM %s.information_schema.columns
				WHERE TABLE_NAME = '%s' 
				ORDER BY COLUMN_NAME ASC`
	rows, err := db.Query(fmt.Sprintf(sqlStr, dbName, tableName))
	if err != nil {
		return
	}
	for rows.Next() {
		var fieldName, dataType, isNull string
		var length int
		err = rows.Scan(&fieldName, &dataType, &isNull, &length)
		if err != nil {
			return
		}
		row := make(map[string]interface{})
		row["field_name"] = fieldName
		row["data_type"] = dataType
		row["is_nullable"] = isNull
		row["length"] = length
		result = append(result, row)
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
