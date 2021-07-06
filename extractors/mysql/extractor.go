package mysql

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
	"information_schema",
	"mysql",
	"performance_schema",
	"sys",
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
	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s)/", config.UserID, config.Password, config.Host))
	if err != nil {
		fmt.Println(err)
		return
	}
	result, err = getDatabases(db)
	return
}

func getDatabases(db *sql.DB) (result []map[string]interface{}, err error) {
	res, err := db.Query("SHOW DATABASES;")
	if err != nil {
		return
	}
	for res.Next() {
		var database string
		res.Scan(&database)
		if checkNotDefaultDatabase(database) {
			result, _ = tableInfo(database, result, db)
		}
	}
	return
}

func tableInfo(dbName string, result []map[string]interface{}, db *sql.DB) (_ []map[string]interface{}, err error) {
	sqlStr := "SHOW TABLES;"
	_, err = db.Exec(fmt.Sprintf("USE %s;", dbName))
	if err != nil {
		fmt.Println(err)
		return
	}
	rows, err := db.Query(sqlStr)
	if err != nil {
		fmt.Println(err)
		return
	}
	for rows.Next() {
		var tableName string
		err = rows.Scan(&tableName)
		if err != nil {
			fmt.Println(err)
			return
		}
		columns, err1 := fieldInfo(dbName, tableName, db)
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

func fieldInfo(dbName string, tableName string, db *sql.DB) (result []map[string]interface{}, err error) {
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
		var fieldName, fieldDesc, dataType, isNull string
		var length int
		err = rows.Scan(&fieldName, &fieldDesc, &dataType, &isNull, &length)
		if err != nil {
			return
		}
		row := make(map[string]interface{})
		row["field_name"] = fieldName
		row["field_desc"] = fieldDesc
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
