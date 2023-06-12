package mariadb

import (
	"context"
	"database/sql"
	_ "embed" // used to print the embedded assets
	"fmt"

	_ "github.com/go-sql-driver/mysql" // used to register the mariadb driver
	"github.com/pkg/errors"
	"github.com/raystack/meteor/models"
	"github.com/raystack/meteor/plugins"
	"github.com/raystack/meteor/registry"
	"github.com/raystack/salt/log"
	"google.golang.org/protobuf/types/known/anypb"

	v1beta2 "github.com/raystack/meteor/models/raystack/assets/v1beta2"
	"github.com/raystack/meteor/plugins/sqlutil"
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

// Config holds the connection URL for the extractor
type Config struct {
	ConnectionURL string  `json:"connection_url" yaml:"connection_url" mapstructure:"connection_url" validate:"required"`
	Exclude       Exclude `json:"exclude" yaml:"exclude" mapstructure:"exclude"`
}

type Exclude struct {
	Databases []string `json:"databases" yaml:"databases" mapstructure:"databases"`
	Tables    []string `json:"tables" yaml:"tables" mapstructure:"tables"`
}

var sampleConfig = `
connection_url: admin:pass123@tcp(localhost:3306)/
exclude:
  databases: [database_a,database_b]
  tables: [table_a,table_b]`

var info = plugins.Info{
	Description:  "Table metadata from Mariadb server.",
	SampleConfig: sampleConfig,
	Summary:      summary,
	Tags:         []string{"oss", "extractor"},
}

// Extractor manages the extraction of data from Mariadb
type Extractor struct {
	plugins.BaseExtractor
	excludedDbs map[string]bool
	excludedTbl map[string]bool
	logger      log.Logger
	config      Config
	db          *sql.DB
	emit        plugins.Emit
}

// New returns a pointer to an initialized Extractor Object
func New(logger log.Logger) *Extractor {
	e := &Extractor{
		logger: logger,
	}
	e.BaseExtractor = plugins.NewBaseExtractor(info, &e.config)

	return e
}

// Init initializes the extractor
func (e *Extractor) Init(ctx context.Context, config plugins.Config) (err error) {
	if err = e.BaseExtractor.Init(ctx, config); err != nil {
		return err
	}

	// build excluded database and tables list
	excludeDBList := append(defaultDBList, e.config.Exclude.Databases...)
	e.excludedDbs = sqlutil.BuildBoolMap(excludeDBList)
	e.excludedTbl = sqlutil.BuildBoolMap(e.config.Exclude.Tables)

	// create mariadb client
	if e.db, err = sql.Open("mysql", e.config.ConnectionURL); err != nil {
		return errors.Wrap(err, "failed to create client")
	}

	return
}

// Extract collects metadata of the database through emitter
func (e *Extractor) Extract(_ context.Context, emit plugins.Emit) (err error) {
	defer e.db.Close()
	e.emit = emit

	// Get list of databases
	dbs, err := sqlutil.FetchDBs(e.db, e.logger, "SHOW DATABASES;")
	if err != nil {
		return err
	}

	// Iterate through all tables and databases
	for _, database := range dbs {
		// skip excluded databases
		if e.isExcludedDB(database) {
			continue
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

	// get list of tables
	tables, err := sqlutil.FetchTablesInDB(e.db, database, "SHOW TABLES;")
	for _, tableName := range tables {
		// skip excluded tables
		if e.isExcludedTable(tableName, database) {
			continue
		}
		if err := e.processTable(database, tableName); err != nil {
			return errors.Wrap(err, "failed to process table")
		}
	}
	return
}

// processTable builds and push table to out channel
func (e *Extractor) processTable(database string, tableName string) (err error) {
	var columns []*v1beta2.Column
	columns, err = e.extractColumns(tableName)
	if err != nil {
		return errors.Wrapf(err, "failed to extract columns")
	}
	data, err := anypb.New(&v1beta2.Table{
		Columns: columns,
	})
	if err != nil {
		return errors.Wrap(err, "failed to build Any struct")
	}
	// push table to channel
	e.emit(models.NewRecord(&v1beta2.Asset{
		Urn:     models.NewURN("mariadb", e.UrnScope, "table", fmt.Sprintf("%s.%s", database, tableName)),
		Name:    tableName,
		Type:    "table",
		Service: "mariadb",
		Data:    data,
	}))
	return
}

// extractColumns extracts columns from a given table
func (e *Extractor) extractColumns(tableName string) (result []*v1beta2.Column, err error) {
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

		result = append(result, &v1beta2.Column{
			Name:        fieldName,
			DataType:    dataType,
			Description: fieldDesc,
			IsNullable:  e.isNullable(isNullableString),
			Length:      int64(length),
		})
	}
	return result, nil
}

// isExcludedDB checks if the database is in the excluded list
func (e *Extractor) isExcludedDB(database string) bool {
	_, ok := e.excludedDbs[database]
	return ok
}

// isExcludedTable checks if the given table is in the list of excluded tables
func (e *Extractor) isExcludedTable(tableName, database string) bool {
	tableName = fmt.Sprintf("%s.%s", database, tableName)
	_, ok := e.excludedTbl[tableName]
	return ok
}

// isNullable returns true if the string is "YES"
func (e *Extractor) isNullable(value string) bool {
	return value == "YES"
}

// Register the extractor to catalog
func init() {
	if err := registry.Extractors.Register("mariadb", func() plugins.Extractor {
		return New(plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}
