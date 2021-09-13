package clickhouse

import (
	"context"
	"database/sql"
	_ "embed" // used to print the embedded assets
	"fmt"

	_ "github.com/ClickHouse/clickhouse-go" // clickhouse driver
	"github.com/odpf/meteor/models"
	"github.com/odpf/meteor/models/odpf/assets"
	"github.com/odpf/meteor/models/odpf/assets/common"
	"github.com/odpf/meteor/models/odpf/assets/facets"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/registry"
	"github.com/odpf/meteor/utils"
	"github.com/odpf/salt/log"
)

//go:embed README.md
var summary string

// Config hold the set of configuration for the extractor
type Config struct {
	UserID   string `mapstructure:"user_id" validate:"required"`
	Password string `mapstructure:"password" validate:"required"`
	Host     string `mapstructure:"host" validate:"required"`
}

var sampleConfig = `
 host: localhost:9000
 user_id: admin
 password: "1234"`

// Extractor manages the output stream
// and logger interface for the extractor
type Extractor struct {
	config Config
	logger log.Logger
	db     *sql.DB
}

// New returns a pointer to an initialized Extractor Object
func New(logger log.Logger) *Extractor {
	return &Extractor{
		logger: logger,
	}
}

// Info returns the brief information about the extractor
func (e *Extractor) Info() plugins.Info {
	return plugins.Info{
		Description:  "Column-oriented DBMS for online analytical processing.",
		SampleConfig: sampleConfig,
		Summary:      summary,
		Tags:         []string{"oss,extractor"},
	}
}

// Validate validates the configuration of the extractor
func (e *Extractor) Validate(configMap map[string]interface{}) (err error) {
	return utils.BuildConfig(configMap, &Config{})
}

func (e *Extractor) Init(ctx context.Context, configMap map[string]interface{}) (err error) {
	err = utils.BuildConfig(configMap, &e.config)
	if err != nil {
		return plugins.InvalidConfigError{}
	}

	e.db, err = sql.Open("clickhouse", fmt.Sprintf("tcp://%s?username=%s&password=%s&debug=true", e.config.Host, e.config.UserID, e.config.Password))
	if err != nil {
		return
	}

	return
}

//Extract checks if the extractor is configured and
// if the connection to the DB is successful
// and then starts the extraction process
func (e *Extractor) Extract(ctx context.Context, emitter plugins.Emitter) (err error) {
	err = e.extractTables(emitter)
	if err != nil {
		return
	}

	return
}

func (e *Extractor) extractTables(emitter plugins.Emitter) (err error) {
	res, err := e.db.Query("SELECT name, database FROM system.tables WHERE database not like 'system'")
	if err != nil {
		return
	}
	for res.Next() {
		var dbName, tableName string
		err = res.Scan(&tableName, &dbName)
		if err != nil {
			return
		}

		var columns []*facets.Column
		columns, err = e.getColumnsInfo(dbName, tableName)
		if err != nil {
			return
		}

		emitter.Emit(models.NewRecord(&assets.Table{
			Resource: &common.Resource{
				Urn:  fmt.Sprintf("%s.%s", dbName, tableName),
				Name: tableName,
			}, Schema: &facets.Columns{
				Columns: columns,
			},
		}))
	}
	return
}

func (e *Extractor) getColumnsInfo(dbName string, tableName string) (result []*facets.Column, err error) {
	sqlStr := fmt.Sprintf("DESCRIBE TABLE %s.%s", dbName, tableName)

	rows, err := e.db.Query(sqlStr)
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
	if err := registry.Extractors.Register("clickhouse", func() plugins.Extractor {
		return New(plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}
