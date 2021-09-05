package mysql

import (
	"context"
	"database/sql"
	_ "embed"
	"fmt"

	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/proto/odpf/assets"
	"github.com/odpf/meteor/proto/odpf/assets/common"
	"github.com/odpf/meteor/proto/odpf/assets/facets"
	"github.com/odpf/meteor/registry"
	"github.com/odpf/meteor/utils"
	"github.com/odpf/salt/log"
)

//go:embed README.md
var summary string

var defaultDBList = []string{
	"information_schema",
	"mysql",
	"performance_schema",
	"sys",
}

type Config struct {
	UserID   string `mapstructure:"user_id" validate:"required"`
	Password string `mapstructure:"password" validate:"required"`
	Host     string `mapstructure:"host" validate:"required"`
}

var sampleConfig = `
 host: localhost:1433
 user_id: admin
 password: 1234`

type Extractor struct {
	out         chan<- interface{}
	excludedDbs map[string]bool

	// dependencies
	logger log.Logger
}

func New(logger log.Logger) *Extractor {
	return &Extractor{
		logger: logger,
	}
}

func (e *Extractor) Info() plugins.Info {
	return plugins.Info{
		Description:  "Table metadata from MySQL server.",
		SampleConfig: sampleConfig,
		Summary:      summary,
		Tags:         []string{"oss,extractor"},
	}
}

func (e *Extractor) Validate(configMap map[string]interface{}) (err error) {
	return utils.BuildConfig(configMap, &Config{})
}

func (e *Extractor) Extract(ctx context.Context, configMap map[string]interface{}, out chan<- interface{}) (err error) {
	e.out = out

	// build config
	var config Config
	err = utils.BuildConfig(configMap, &config)
	if err != nil {
		return plugins.InvalidConfigError{}
	}

	// build excluded database list
	e.buildExcludedDBs()

	// create client
	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s)/", config.UserID, config.Password, config.Host))
	if err != nil {
		return
	}
	defer db.Close()

	// extraction process
	err = e.extract(db)
	if err != nil {
		return
	}

	return
}

// Extract all tables from databases
func (e *Extractor) extract(db *sql.DB) (err error) {
	res, err := db.Query("SHOW DATABASES;")
	if err != nil {
		return
	}
	for res.Next() {
		var database string
		if err := res.Scan(&database); err != nil {
			return err
		}

		if err := e.extractTables(db, database); err != nil {
			return err
		}
	}
	return
}

// Extract tables from a given database
func (e *Extractor) extractTables(db *sql.DB, database string) (err error) {
	// skip if database is default
	if e.isExcludedDB(database) {
		return
	}

	// extract tables
	_, err = db.Exec(fmt.Sprintf("USE %s;", database))
	if err != nil {
		return
	}
	rows, err := db.Query("SHOW TABLES;")
	if err != nil {
		return
	}

	// process each rows
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return err
		}

		if err := e.processTable(db, database, tableName); err != nil {
			return err
		}
	}

	return
}

// Build and push table to out channel
func (e *Extractor) processTable(db *sql.DB, database string, tableName string) (err error) {
	var columns []*facets.Column
	columns, err = e.extractColumns(db, tableName)
	if err != nil {
		return
	}

	// push table to channel
	e.out <- assets.Table{
		Resource: &common.Resource{
			Urn:  fmt.Sprintf("%s.%s", database, tableName),
			Name: tableName,
		},
		Schema: &facets.Columns{
			Columns: columns,
		},
	}

	return
}

// Extract columns from a given table
func (e *Extractor) extractColumns(db *sql.DB, tableName string) (columns []*facets.Column, err error) {
	query := `SELECT COLUMN_NAME,column_comment,DATA_TYPE,
				IS_NULLABLE,IFNULL(CHARACTER_MAXIMUM_LENGTH,0)
				FROM information_schema.columns
				WHERE table_name = ?
				ORDER BY COLUMN_NAME ASC`
	rows, err := db.Query(query, tableName)
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

		columns = append(columns, &facets.Column{
			Name:        fieldName,
			DataType:    dataType,
			Description: fieldDesc,
			IsNullable:  e.isNullable(isNullableString),
			Length:      int64(length),
		})
	}

	return
}

func (e *Extractor) buildExcludedDBs() {
	excludedMap := make(map[string]bool)
	for _, db := range defaultDBList {
		excludedMap[db] = true
	}

	e.excludedDbs = excludedMap
}

func (e *Extractor) isExcludedDB(database string) bool {
	_, ok := e.excludedDbs[database]
	return ok
}

func (e *Extractor) isNullable(value string) bool {
	return value == "YES"
}

// Register the extractor to catalog
func init() {
	if err := registry.Extractors.Register("mysql", func() plugins.Extractor {
		return New(plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}
