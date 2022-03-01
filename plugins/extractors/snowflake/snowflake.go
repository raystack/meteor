package snowflake

import (
	"context"
	"database/sql"
	_ "embed" // used to print the embedded assets
	"fmt"
	"github.com/odpf/meteor/models"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/registry"
	"github.com/odpf/meteor/utils"
	"github.com/odpf/salt/log"
	"github.com/pkg/errors"
	_ "github.com/snowflakedb/gosnowflake" // used to register the snowflake driver

	commonv1beta1 "github.com/odpf/meteor/models/odpf/assets/common/v1beta1"
	facetsv1beta1 "github.com/odpf/meteor/models/odpf/assets/facets/v1beta1"
	assetsv1beta1 "github.com/odpf/meteor/models/odpf/assets/v1beta1"
)

//go:embed README.md
var summary string

// Config holds the connection URL for the extractor
type Config struct {
	ConnectionURL string `mapstructure:"connection_url" validate:"required"`
}

var sampleConfig = `connection_url: "user:password@my_organization-my_account/mydb"`

// Extractor manages the extraction of data from snowflake
type Extractor struct {
	logger log.Logger
	config Config
	db     *sql.DB
	emit   plugins.Emit
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
		Description:  "Table metadata from Snowflake server.",
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

	// create snowflake client

	// https://github.com/snowflakedb/gosnowflake/blob/e24bda449ced75324e8ce61377c88e4cea9c1efa/driver_test.go#L79
	if e.db, err = sql.Open("snowflake", e.config.ConnectionURL); err != nil {
		return errors.Wrap(err, "failed to create client")
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
	var columns []*facetsv1beta1.Column
	columns, err = e.extractColumns(tableName)
	if err != nil {
		return errors.Wrapf(err, "failed to extract columns")
	}

	// push table to channel
	e.emit(models.NewRecord(&assetsv1beta1.Table{
		Resource: &commonv1beta1.Resource{
			Urn:     fmt.Sprintf("%s.%s", database, tableName),
			Name:    tableName,
			Service: "Snowflake",
		},
		Schema: &facetsv1beta1.Columns{
			Columns: columns,
		},
	}))
	return
}

// extractColumns extracts columns from a given table
func (e *Extractor) extractColumns(tableName string) (result []*facetsv1beta1.Column, err error) {
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

		result = append(result, &facetsv1beta1.Column{
			Name:        fieldName,
			DataType:    dataType,
			Description: fieldDesc,
			IsNullable:  e.isNullable(isNullableString),
			Length:      int64(length),
		})
	}
	return result, nil
}

// isNullable returns true if the string is "YES"
func (e *Extractor) isNullable(value string) bool {
	return value == "YES"
}

// Register the extractor to catalog
func init() {
	if err := registry.Extractors.Register("snowflake", func() plugins.Extractor {
		return New(plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}
