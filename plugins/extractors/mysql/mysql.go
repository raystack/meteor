package mysql

import (
	"context"
	"database/sql"
	_ "embed" // used to print the embedded assets
	"fmt"

	"github.com/pkg/errors"
	"google.golang.org/protobuf/types/known/anypb"

	_ "github.com/go-sql-driver/mysql"
	"github.com/odpf/meteor/models"

	v1beta2 "github.com/odpf/meteor/models/odpf/assets/v1beta2"

	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/plugins/sqlutil"
	"github.com/odpf/meteor/registry"
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
connection_url: "admin:pass123@tcp(localhost:3306)/"
exclude:
  databases:
	- database_a
	- database_b
  tables:
	- dataset_c.table_a`

var info = plugins.Info{
	Description:  "Table metadata from MySQL server.",
	SampleConfig: sampleConfig,
	Summary:      summary,
	Tags:         []string{"oss", "extractor"},
}

// Extractor manages the extraction of data from MySQL
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

	excludeDBList := append(defaultDBList, e.config.Exclude.Databases...)
	e.excludedDbs = sqlutil.BuildBoolMap(excludeDBList)
	e.excludedTbl = sqlutil.BuildBoolMap(e.config.Exclude.Tables)

	// create client
	if e.db, err = sql.Open("mysql", e.config.ConnectionURL); err != nil {
		return errors.Wrap(err, "failed to create client")
	}

	return
}

// Extract extracts the data from the MySQL server
// and collected through the emitter
func (e *Extractor) Extract(ctx context.Context, emit plugins.Emit) (err error) {
	defer e.db.Close()
	e.emit = emit

	dbs, err := sqlutil.FetchDBs(e.db, e.logger, "SHOW DATABASES;")
	if err != nil {
		return err
	}

	for _, db := range dbs {
		// skip excluded databases
		if e.isExcludedDB(db) {
			continue
		}
		// extract tables
		err := e.extractTables(db)
		if err != nil {
			e.logger.Error("failed to get tables, skipping database", "error", err)
			continue
		}
	}

	return
}

// Extract tables from a given database
func (e *Extractor) extractTables(database string) (err error) {
	// set database
	_, err = e.db.Exec(fmt.Sprintf("USE %s;", database))
	if err != nil {
		return errors.Wrapf(err, "failed to iterate over %s", database)
	}

	// get list of tables
	tables, err := sqlutil.FetchTablesInDB(e.db, database, "SHOW TABLES;")
	for _, tableName := range tables {
		// skip excluded tables
		if e.isExcludedTable(database, tableName) {
			continue
		}
		if err := e.processTable(database, tableName); err != nil {
			return errors.Wrap(err, "failed to process table")
		}
	}

	return
}

// processTable builds and push table to emitter
func (e *Extractor) processTable(database string, tableName string) (err error) {
	var columns []*v1beta2.Column
	if columns, err = e.extractColumns(tableName); err != nil {
		return errors.Wrap(err, "failed to extract columns")
	}
	table, err := anypb.New(&v1beta2.Table{
		Columns: columns,
	})
	if err != nil {
		err = fmt.Errorf("error creating Any struct: %w", err)
		return err
	}
	// push table to channel
	e.emit(models.NewRecord(&v1beta2.Asset{
		Urn:     models.NewURN("mysql", e.UrnScope, "table", fmt.Sprintf("%s.%s", database, tableName)),
		Name:    tableName,
		Type:    "table",
		Service: "mysql",
		Data:    table,
	}))

	return
}

// Extract columns from a given table
func (e *Extractor) extractColumns(tableName string) (columns []*v1beta2.Column, err error) {
	query := `SELECT COLUMN_NAME,column_comment,DATA_TYPE,
				IS_NULLABLE,IFNULL(CHARACTER_MAXIMUM_LENGTH,0)
				FROM information_schema.columns
				WHERE table_name = ?
				ORDER BY COLUMN_NAME ASC`
	rows, err := e.db.Query(query, tableName)
	if err != nil {
		err = errors.Wrap(err, "failed to execute query")
		return
	}

	for rows.Next() {
		var fieldName, fieldDesc, dataType, isNullableString string
		var length int
		if err = rows.Scan(&fieldName, &fieldDesc, &dataType, &isNullableString, &length); err != nil {
			e.logger.Error("failed to get fields", "error", err)
			continue
		}

		columns = append(columns, &v1beta2.Column{
			Name:        fieldName,
			DataType:    dataType,
			Description: fieldDesc,
			IsNullable:  e.isNullable(isNullableString),
			Length:      int64(length),
		})
	}

	return
}

// isExcludedDB checks if the given db is in the list of excluded databases
func (e *Extractor) isExcludedDB(database string) bool {
	_, ok := e.excludedDbs[database]
	return ok
}

// isExcludedTable checks if the given table is in the list of excluded tables
func (e *Extractor) isExcludedTable(database, tableName string) bool {
	tableName = fmt.Sprintf("%s.%s", database, tableName)
	_, ok := e.excludedTbl[tableName]
	return ok
}

// isNullable checks if the given string is null or not
func (e *Extractor) isNullable(value string) bool {
	return value == "YES"
}

// init register the extractor to the catalog
func init() {
	if err := registry.Extractors.Register("mysql", func() plugins.Extractor {
		return New(plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}
