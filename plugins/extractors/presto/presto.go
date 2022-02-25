package presto

import (
	"context"
	"database/sql"
	_ "embed"
	"fmt"

	"github.com/odpf/meteor/models"
	commonv1beta1 "github.com/odpf/meteor/models/odpf/assets/common/v1beta1"
	facetsv1beta1 "github.com/odpf/meteor/models/odpf/assets/facets/v1beta1"
	assetsv1beta1 "github.com/odpf/meteor/models/odpf/assets/v1beta1"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/registry"
	"github.com/odpf/meteor/utils"
	"github.com/odpf/salt/log"
	_ "github.com/prestodb/presto-go-client/presto" // presto driver
)

//go:embed README.md
var summary string

// Config holds the connection URL for the extractor
type Config struct {
	ConnectionURL string `mapstructure:"connection_url" validate:"required"`
}

var sampleConfig = `connection_url: "http://user:pass@localhost:8080?catalog=default&schema=test"`

// Extractor manages the extraction of data
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
		Description:  "Table metadata from Presto server.",
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

	// create presto client
	if e.db, err = sql.Open("presto", e.config.ConnectionURL); err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}

	return
}

// Extract collects metadata of the database through emitter
func (e *Extractor) Extract(_ context.Context, emit plugins.Emit) (err error) {
	defer e.db.Close()
	e.emit = emit

	// link: https://docs.treasuredata.com/display/public/PD/How+do+I+Access+TD+table+Metadata+using+Presto
	catalogs, err := e.db.Query("SHOW CATALOGS")
	if err != nil {
		return fmt.Errorf("failed to get the list of catalogs: %w", err)
	}

	for catalogs.Next() {
		var schema string
		if err = catalogs.Scan(&schema); err != nil {
			return fmt.Errorf("failed to scan schema %s: %w", schema, err)
		}

		// Get list of databases
		showSchemasQuery := fmt.Sprintf("show schemas in %s", schema)
		dbs, err := e.db.Query(showSchemasQuery)
		if err != nil {
			return fmt.Errorf("failed to get the list of schemas: %w", err)
		}

		// Iterate through all tables and databases
		for dbs.Next() {
			var database string
			if err = dbs.Scan(&database); err != nil {
				return fmt.Errorf("failed to scan %s: %w", database, err)
			}
			if err = e.extractTables(database); err != nil {
				return fmt.Errorf("failed to extract tables from %s: %w", database, err)
			}
		}
	}
	return
}

// extractTables extracts tables from a given database
func (e *Extractor) extractTables(database string) (err error) {
	// extract tables
	_, err = e.db.Exec(fmt.Sprintf("USE %s;", database))
	if err != nil {
		return fmt.Errorf("failed to execute USE query on %s: %w", database, err)
	}
	rows, err := e.db.Query("SHOW TABLES;")
	if err != nil {
		return fmt.Errorf("failed to show tables for %s: %w", database, err)
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
		return fmt.Errorf("failed to extract columns: %w", err)
	}

	// push table to channel
	e.emit(models.NewRecord(&assetsv1beta1.Table{
		Resource: &commonv1beta1.Resource{
			Urn:  fmt.Sprintf("%s.%s", database, tableName),
			Name: tableName,
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
	rows, err := e.db.Query(sqlStr, sql.Named("X-Presto-User", tableName))
	if err != nil {
		return nil, fmt.Errorf("failed to execute a query to extract columns metadata: %w", err)
	}

	for rows.Next() {
		var fieldName, fieldDesc, dataType, isNullableString string
		var length int
		err = rows.Scan(&fieldName, &fieldDesc, &dataType, &isNullableString, &length)
		if err != nil {
			return nil, fmt.Errorf("failed to scan fields from query: %w", err)
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
	if err := registry.Extractors.Register("presto", func() plugins.Extractor {
		return New(plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}

// https://prestodb.io/docs/current/sql.html
