package clickhouse

import (
	"database/sql"
	"fmt"

	_ "github.com/ClickHouse/clickhouse-go"
	"github.com/odpf/meteor/core/extractor"
	"github.com/odpf/meteor/proto/odpf/meta"
	"github.com/odpf/meteor/proto/odpf/meta/facets"
	"github.com/odpf/meteor/utils"
)

var db *sql.DB

type Config struct {
	UserID   string `mapstructure:"user_id" validate:"required"`
	Password string `mapstructure:"password" validate:"required"`
	Host     string `mapstructure:"host" validate:"required"`
}

type Extractor struct{}

func New() extractor.TableExtractor {
	return &Extractor{}
}

func (e *Extractor) Extract(configMap map[string]interface{}) (result []meta.Table, err error) {
	var config Config
	err = utils.BuildConfig(configMap, &config)
	if err != nil {
		return result, extractor.InvalidConfigError{}
	}

	db, err = sql.Open("clickhouse", fmt.Sprintf("tcp://%s?username=%s&password=%s&debug=true", config.Host, config.UserID, config.Password))
	if err != nil {
		return
	}
	result, err = e.getTables()
	return
}

func (e *Extractor) getTables() (result []meta.Table, err error) {
	res, err := db.Query("SELECT name, database FROM system.tables WHERE database not like 'system'")
	if err != nil {
		return
	}
	for res.Next() {
		var dbName, tableName string
		res.Scan(&tableName, &dbName)

		var columns []*facets.Column
		columns, err = e.getColumnsInfo(dbName, tableName)
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
	return
}

func (e *Extractor) getColumnsInfo(dbName string, tableName string) (result []*facets.Column, err error) {
	sqlStr := fmt.Sprintf("DESCRIBE TABLE %s.%s", dbName, tableName)

	rows, err := db.Query(sqlStr)
	if err != nil {
		return
	}
	for rows.Next() {
		var colName, colDesc, dataType string
		var temp1, temp2, temp3, temp4 string
		err = rows.Scan(&colName, &dataType, &colDesc, &temp1, &temp2, &temp3, &temp4)
		if err != nil {
			return
		}
		result = append(result, &facets.Column{
			Name:        colName,
			DataType:    dataType,
			Description: colDesc,
		})
	}
	return result, nil
}
