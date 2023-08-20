package mariadb

import (
	"context"
	"database/sql"
	_ "embed" // used to print the embedded assets
	"fmt"

	_ "github.com/go-sql-driver/mysql" // used to register the mariadb driver
	"github.com/raystack/meteor/models"
	v1beta2 "github.com/raystack/meteor/models/raystack/assets/v1beta2"
	"github.com/raystack/meteor/plugins"
	"github.com/raystack/meteor/plugins/sqlutil"
	"github.com/raystack/meteor/registry"
	"github.com/raystack/salt/log"
	semconv "go.opentelemetry.io/otel/semconv/v1.20.0"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"
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
	err = e.BaseExtractor.Init(ctx, config)
	if err != nil {
		return err
	}

	// build excluded database and tables list
	excludeDBList := append(defaultDBList, e.config.Exclude.Databases...)
	e.excludedDbs = sqlutil.BuildBoolMap(excludeDBList)
	e.excludedTbl = sqlutil.BuildBoolMap(e.config.Exclude.Tables)

	e.db, err = sqlutil.OpenWithOtel("mysql", e.config.ConnectionURL, semconv.DBSystemMariaDB)
	if err != nil {
		return fmt.Errorf("create a client: %w", err)
	}

	return nil
}

// Extract collects metadata of the database through emitter
func (e *Extractor) Extract(ctx context.Context, emit plugins.Emit) error {
	defer e.db.Close()
	e.emit = emit

	// Get list of databases
	dbs, err := sqlutil.FetchDBs(ctx, e.db, e.logger, "SHOW DATABASES;")
	if err != nil {
		return err
	}

	// Iterate through all tables and databases
	for _, database := range dbs {
		// skip excluded databases
		if e.isExcludedDB(database) {
			continue
		}
		if err := e.extractTables(ctx, database); err != nil {
			return fmt.Errorf("extract tables from %s: %w", database, err)
		}
	}
	return nil
}

// extractTables extracts tables from a given database
func (e *Extractor) extractTables(ctx context.Context, database string) error {
	// extract tables
	_, err := e.db.ExecContext(ctx, fmt.Sprintf("USE %s;", database))
	if err != nil {
		return fmt.Errorf("execute USE query on %s: %w", database, err)
	}

	// get list of tables
	tables, _ := sqlutil.FetchTablesInDB(ctx, e.db, database, "SHOW TABLES;")
	for _, tableName := range tables {
		// skip excluded tables
		if e.isExcludedTable(tableName, database) {
			continue
		}
		if err := e.processTable(ctx, database, tableName); err != nil {
			return fmt.Errorf("process table: %w", err)
		}
	}

	return nil
}

// processTable builds and push table to out channel
func (e *Extractor) processTable(ctx context.Context, database, tableName string) error {
	columns, err := e.extractColumns(ctx, tableName)
	if err != nil {
		return fmt.Errorf("extract columns: %w", err)
	}
	data, err := anypb.New(&v1beta2.Table{
		Columns:    columns,
		Attributes: &structpb.Struct{},
	})
	if err != nil {
		return fmt.Errorf("build Any struct: %w", err)
	}
	// push table to channel
	e.emit(models.NewRecord(&v1beta2.Asset{
		Urn:     models.NewURN("mariadb", e.UrnScope, "table", fmt.Sprintf("%s.%s", database, tableName)),
		Name:    tableName,
		Type:    "table",
		Service: "mariadb",
		Data:    data,
	}))
	return nil
}

// extractColumns extracts columns from a given table
func (e *Extractor) extractColumns(ctx context.Context, tableName string) ([]*v1beta2.Column, error) {
	sqlStr := `SELECT COLUMN_NAME,column_comment,DATA_TYPE,
				IS_NULLABLE,IFNULL(CHARACTER_MAXIMUM_LENGTH,0)
				FROM information_schema.columns
				WHERE table_name = ?
				ORDER BY COLUMN_NAME ASC;`
	rows, err := e.db.QueryContext(ctx, sqlStr, tableName)
	if err != nil {
		return nil, fmt.Errorf("execute a query to extract columns metadata: %w", err)
	}
	defer rows.Close()

	var result []*v1beta2.Column
	for rows.Next() {
		var fieldName, fieldDesc, dataType, isNullableString string
		var length int
		err = rows.Scan(&fieldName, &fieldDesc, &dataType, &isNullableString, &length)
		if err != nil {
			return nil, fmt.Errorf("scan fields from query: %w", err)
		}

		result = append(result, &v1beta2.Column{
			Name:        fieldName,
			DataType:    dataType,
			Description: fieldDesc,
			IsNullable:  e.isNullable(isNullableString),
			Length:      int64(length),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate column metadata results: %w", err)
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
