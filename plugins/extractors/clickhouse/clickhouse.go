package clickhouse

import (
	"context"
	"database/sql"
	_ "embed"
	"fmt"

	_ "github.com/ClickHouse/clickhouse-go"
	"github.com/MakeNowJust/heredoc"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/proto/odpf/assets"
	"github.com/odpf/meteor/proto/odpf/assets/common"
	"github.com/odpf/meteor/proto/odpf/assets/facets"
	"github.com/odpf/meteor/registry"
	"github.com/odpf/meteor/utils"
	"github.com/odpf/salt/log"
)

var db *sql.DB

//go:embed README.md
var summary string

type Config struct {
	UserID   string `mapstructure:"user_id" validate:"required"`
	Password string `mapstructure:"password" validate:"required"`
	Host     string `mapstructure:"host" validate:"required"`
}

type Extractor struct {
	out chan<- interface{}

	logger log.Logger
}

func New(logger log.Logger) *Extractor {
	return &Extractor{
		logger: logger,
	}
}

func (e *Extractor) Info() plugins.Info {
	return plugins.Info{
		Description: "Column-oriented DBMS for online analytical processing.",
		SampleConfig: heredoc.Doc(`
			host: localhost:9000
			user_id: admin
			password: 1234
		`),
		Summary: summary,
		Tags:    []string{"GCP,extractor"},
	}
}

func (e *Extractor) Validate(configMap map[string]interface{}) (err error) {
	return utils.BuildConfig(configMap, &Config{})
}

func (e *Extractor) Extract(ctx context.Context, configMap map[string]interface{}, out chan<- interface{}) (err error) {
	e.out = out
	var config Config
	err = utils.BuildConfig(configMap, &config)
	if err != nil {
		return plugins.InvalidConfigError{}
	}

	db, err = sql.Open("clickhouse", fmt.Sprintf("tcp://%s?username=%s&password=%s&debug=true", config.Host, config.UserID, config.Password))
	if err != nil {
		return
	}
	err = e.extractTables()
	if err != nil {
		return
	}

	return
}

func (e *Extractor) extractTables() (err error) {
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

		e.out <- assets.Table{
			Resource: &common.Resource{
				Urn:  fmt.Sprintf("%s.%s", dbName, tableName),
				Name: tableName,
			}, Schema: &facets.Columns{
				Columns: columns,
			},
		}
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

// Register the extractor to catalog
func init() {
	if err := registry.Extractors.Register("mysql", func() plugins.Extractor {
		return New(plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}
