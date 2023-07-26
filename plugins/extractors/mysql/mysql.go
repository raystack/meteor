package mysql

import (
	"context"
	"database/sql"
	_ "embed" // used to print the embedded assets
	"fmt"

	_ "github.com/go-sql-driver/mysql"
	"github.com/goto/meteor/models"
	v1beta2 "github.com/goto/meteor/models/gotocompany/assets/v1beta2"
	"github.com/goto/meteor/plugins"
	"github.com/goto/meteor/plugins/sqlutil"
	"github.com/goto/meteor/registry"
	"github.com/goto/salt/log"
	semconv "go.opentelemetry.io/otel/semconv/v1.20.0"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"
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
	ConnectionURL string `mapstructure:"connection_url" validate:"required"`
}

var sampleConfig = `connection_url: "admin:pass123@tcp(localhost:3306)/"`

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

	// build excluded database list
	e.excludedDbs = sqlutil.BuildBoolMap(defaultDBList)

	// create mysql client
	e.db, err = sqlutil.OpenWithOtel("mysql", e.config.ConnectionURL, semconv.DBSystemMySQL)
	if err != nil {
		return fmt.Errorf("create a client: %w", err)
	}

	return nil
}

// Extract extracts the data from the MySQL server
// and collected through the emitter
func (e *Extractor) Extract(ctx context.Context, emit plugins.Emit) error {
	defer e.db.Close()
	e.emit = emit

	dbs, err := sqlutil.FetchDBs(ctx, e.db, e.logger, "SHOW DATABASES;")
	if err != nil {
		return err
	}

	for _, db := range dbs {
		err := e.extractTables(ctx, db)
		if err != nil {
			e.logger.Error("failed to get tables, skipping database", "error", err)
			continue
		}
	}

	return nil
}

// Extract tables from a given database
func (e *Extractor) extractTables(ctx context.Context, database string) error {
	// skip if database is default
	if e.isExcludedDB(database) {
		return nil
	}

	// set database
	_, err := e.db.Exec(fmt.Sprintf("USE %s;", database))
	if err != nil {
		return fmt.Errorf("iterate over %s: %w", database, err)
	}

	// get list of tables
	tables, err := sqlutil.FetchTablesInDB(ctx, e.db, database, "SHOW TABLES;")
	for _, tableName := range tables {
		if err := e.processTable(ctx, database, tableName); err != nil {
			return fmt.Errorf("process table: %w", err)
		}
	}

	return err
}

// processTable builds and push table to emitter
func (e *Extractor) processTable(ctx context.Context, database, tableName string) error {
	columns, err := e.extractColumns(ctx, tableName)
	if err != nil {
		return fmt.Errorf("extract columns: %w", err)
	}
	table, err := anypb.New(&v1beta2.Table{
		Columns:    columns,
		Attributes: &structpb.Struct{}, // ensure attributes don't get overwritten if present
	})
	if err != nil {
		return fmt.Errorf("create Any struct: %w", err)
	}
	// push table to channel
	e.emit(models.NewRecord(&v1beta2.Asset{
		Urn:     models.NewURN("mysql", e.UrnScope, "table", fmt.Sprintf("%s.%s", database, tableName)),
		Name:    tableName,
		Type:    "table",
		Service: "mysql",
		Data:    table,
	}))

	return nil
}

// Extract columns from a given table
func (e *Extractor) extractColumns(ctx context.Context, tableName string) ([]*v1beta2.Column, error) {
	query := `SELECT COLUMN_NAME,column_comment,DATA_TYPE,
				IS_NULLABLE,IFNULL(CHARACTER_MAXIMUM_LENGTH,0)
				FROM information_schema.columns
				WHERE table_name = ?
				ORDER BY COLUMN_NAME ASC`
	rows, err := e.db.QueryContext(ctx, query, tableName)
	if err != nil {
		return nil, fmt.Errorf("execute query: %w", err)
	}
	defer rows.Close()

	var columns []*v1beta2.Column
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
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate over columns: %w", err)
	}

	return columns, nil
}

// isExcludedDB checks if the given db is in the list of excluded databases
func (e *Extractor) isExcludedDB(database string) bool {
	_, ok := e.excludedDbs[database]
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
