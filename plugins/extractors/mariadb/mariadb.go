package mariadb

import (
	"context"
	"database/sql"
	_ "embed" // used to print the embedded assets
	"fmt"
	_ "github.com/go-sql-driver/mysql" // used to register the mariadb driver
	"github.com/odpf/meteor/models"
	"github.com/odpf/meteor/models/odpf/assets"
	"github.com/odpf/meteor/models/odpf/assets/common"
	"github.com/odpf/meteor/models/odpf/assets/facets"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/registry"
	"github.com/odpf/meteor/utils"
	"github.com/odpf/salt/log"
	"github.com/pkg/errors"
)

//go:embed README.md
var summary string

// defaultDBList is a list of databases that should be excluded
var defaultDBList = []string{
	"information_schema",
	"mysql",
	"performance_schema",
	"sys",
}

// Config hold the set of configuration for the extractor
type Config struct {
	UserID   string `mapstructure:"user_id" validate:"required"`
	Password string `mapstructure:"password" validate:"required"`
	Host     string `mapstructure:"host" validate:"required"`
}

var sampleConfig = `
 host: localhost:1433
 user_id: admin
 password: "1234"`

// Extractor manages the extraction of data from Mariadb
type Extractor struct {
	excludedDbs map[string]bool
	logger      log.Logger
	config      Config
	db          *sql.DB
	emit        plugins.Emit
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
		Description:  "Table metadata from Mariadb server.",
		SampleConfig: sampleConfig,
		Summary:      summary,
		Tags:         []string{"oss", "extractor"},
	}
}

// Validate validates the configuration of the extractor
func (e *Extractor) Validate(configMap map[string]interface{}) (err error) {
	return utils.BuildConfig(configMap, &Config{})
}

// Init initializes the extractor
func (e *Extractor) Init(_ context.Context, configMap map[string]interface{}) (err error) {
	// Build and validate config received from recipe
	if err = utils.BuildConfig(configMap, &e.config); err != nil {
		return plugins.InvalidConfigError{}
	}

	// build excluded database list
	e.buildExcludedDBs()

	// create client
	if e.db, err = connection(e.config); err != nil {
		return
	}
	return
}

// Extract collects metadata of the database through emitter
func (e *Extractor) Extract(_ context.Context, emit plugins.Emit) (err error) {
	defer e.db.Close()
	e.emit = emit

	// Get list of databases
	dbs, err := e.db.Query("SHOW DATABASES;")
	if err != nil {
		return errors.Wrapf(err, "failed to get the list of databases")
	}

	// Iterate through all tables and databases
	for dbs.Next() {
		var database string
		if err := dbs.Scan(&database); err != nil {
			return errors.Wrapf(err, "failed to scan: %s", database)
		}
		if err := e.extractTables(database); err != nil {
			return errors.Wrapf(err, "failed to extract tables from %s", database)
		}
	}
	return
}

// extractTables extracts tables from a given database
func (e *Extractor) extractTables(database string) (err error) {
	// skip if database is default
	if e.isExcludedDB(database) {
		return
	}

	// extract tables
	_, err = e.db.Exec(fmt.Sprintf("USE %s;", database))
	if err != nil {
		return errors.Wrapf(err, "failed to execute USE query on %s", database)
	}
	rows, err := e.db.Query("SHOW TABLES;")
	if err != nil {
		return errors.Wrapf(err, "failed to show tables for %s", database)
	}

	// process each rows
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return err
		}
		if err := e.processTable(database, tableName); err != nil {
			return err
		}
	}
	return
}

// processTable builds and push table to out channel
func (e *Extractor) processTable(database string, tableName string) (err error) {
	var columns []*facets.Column
	columns, err = e.extractColumns(tableName)
	if err != nil {
		return errors.Wrapf(err, "failed to extract columns")
	}

	// push table to channel
	e.emit(models.NewRecord(&assets.Table{
		Resource: &common.Resource{
			Urn:  fmt.Sprintf("%s.%s", database, tableName),
			Name: tableName,
		},
		Schema: &facets.Columns{
			Columns: columns,
		},
	}))
	return
}

// extractColumns extracts columns from a given table
func (e *Extractor) extractColumns(tableName string) (result []*facets.Column, err error) {
	sqlStr := `SELECT COLUMN_NAME,column_comment,DATA_TYPE,
				IS_NULLABLE,IFNULL(CHARACTER_MAXIMUM_LENGTH,0)
				FROM information_schema.columns
				WHERE table_name = ?
				ORDER BY COLUMN_NAME ASC;`
	rows, err := e.db.Query(sqlStr, tableName)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to execute a query to extract columns metadata")
	}

	for rows.Next() {
		var fieldName, fieldDesc, dataType, isNullableString string
		var length int
		err = rows.Scan(&fieldName, &fieldDesc, &dataType, &isNullableString, &length)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to scan fields from query")
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

// buildExcludedDBs builds a list of databases to exclude
func (e *Extractor) buildExcludedDBs() {
	excludedMap := make(map[string]bool)
	for _, db := range defaultDBList {
		excludedMap[db] = true
	}
	e.excludedDbs = excludedMap
}

// isExcludedDB checks if the database is in the excluded list
func (e *Extractor) isExcludedDB(database string) bool {
	_, ok := e.excludedDbs[database]
	return ok
}

// isNullable returns true if the string is "YES"
func (e *Extractor) isNullable(value string) bool {
	return value == "YES"
}

// connection generates a string to connect MariaDB
func connection(config Config) (db *sql.DB, err error) {
	connStr := fmt.Sprintf("%s:%s@tcp(%s)/", config.UserID, config.Password, config.Host)
	return sql.Open("mysql", connStr)
}

// Register the extractor to catalog
func init() {
	if err := registry.Extractors.Register("mariadb", func() plugins.Extractor {
		return New(plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}
