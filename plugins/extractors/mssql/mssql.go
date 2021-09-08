package mssql

import (
	"context"
	"database/sql"
	_ "embed" // used to print the embedded assets
	"fmt"

	"github.com/odpf/salt/log"

	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/registry"

	"github.com/odpf/meteor/proto/odpf/assets"
	"github.com/odpf/meteor/proto/odpf/assets/common"
	"github.com/odpf/meteor/proto/odpf/assets/facets"
	"github.com/odpf/meteor/utils"
)

//go:embed README.md
var summary string

var defaultDBList = []string{
	"master",
	"msdb",
	"model",
	"tempdb",
}

// Config holds the set of configuration for the extractor
type Config struct {
	UserID   string `mapstructure:"user_id" validate:"required"`
	Password string `mapstructure:"password" validate:"required"`
	Host     string `mapstructure:"host" validate:"required"`
}

var sampleConfig = `
 host: localhost:1433
 user_id: admin
 password: "1234"`

// Extractor manages the extraction of data from the database
type Extractor struct {
	out         chan<- interface{}
	excludedDbs map[string]bool
	logger log.Logger
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
		Description:  "Table metdata from MSSQL server",
		SampleConfig: sampleConfig,
		Summary:      summary,
		Tags:         []string{"microsoft,extractor"},
	}
}

// Validate validates the configuration of the extractor
func (e *Extractor) Validate(configMap map[string]interface{}) (err error) {
	return utils.BuildConfig(configMap, &Config{})
}

// Extract checks if the extractor is ready to extract
// and then extract and push data into stream
func (e *Extractor) Extract(ctx context.Context, configMap map[string]interface{}, out chan<- interface{}) (err error) {
	e.out = out

	// build and verify config
	var config Config
	err = utils.BuildConfig(configMap, &config)
	if err != nil {
		return plugins.InvalidConfigError{}
	}

	// build excluded database list
	e.buildExcludedDBs()

	// create client
	db, err := sql.Open("mssql", fmt.Sprintf("sqlserver://%s:%s@%s/", config.UserID, config.Password, config.Host))
	if err != nil {
		return
	}
	defer db.Close()

	// extract and push data into stream
	err = e.extract(db)
	if err != nil {
		return err
	}

	return
}

// Extract all tables from databases
func (e *Extractor) extract(db *sql.DB) (err error) {
	res, err := db.Query("SELECT name FROM sys.databases;")
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
	// skip if database is excluded
	if e.isExcludedDB(database) {
		return
	}

	// extract tables
	rows, err := db.Query(
		fmt.Sprintf(`SELECT TABLE_NAME FROM %s.INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE';`, database))
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

func (e *Extractor) processTable(db *sql.DB, database string, tableName string) (err error) {
	columns, err := e.getColumns(db, database, tableName)
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

func (e *Extractor) getColumns(db *sql.DB, database, tableName string) (columns []*facets.Column, err error) {
	query := fmt.Sprintf(
		`SELECT COLUMN_NAME, DATA_TYPE, 
		IS_NULLABLE, coalesce(CHARACTER_MAXIMUM_LENGTH,0) 
		FROM %s.information_schema.columns 
		WHERE TABLE_NAME = ?
		ORDER BY COLUMN_NAME ASC`, database)
	rows, err := db.Query(query, tableName)
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
		columns = append(columns, &facets.Column{
			Name:       fieldName,
			DataType:   dataType,
			IsNullable: e.isNullable(isNullableString),
			Length:     int64(length),
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

func init() {
	if err := registry.Extractors.Register("mssql", func() plugins.Extractor {
		return New(plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}
