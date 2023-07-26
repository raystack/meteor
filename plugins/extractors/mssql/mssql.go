package mssql

import (
	"context"
	"database/sql"
	_ "embed" // used to print the embedded assets
	"fmt"

	_ "github.com/denisenkom/go-mssqldb"
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
	"master",
	"msdb",
	"model",
	"tempdb",
}

// Config holds the connection URL for the extractor
type Config struct {
	ConnectionURL string `mapstructure:"connection_url" validate:"required"`
}

var sampleConfig = `connection_url: "sqlserver://admin:pass123@localhost:3306/"`

var info = plugins.Info{
	Description:  "Table metdata from MSSQL server",
	SampleConfig: sampleConfig,
	Summary:      summary,
	Tags:         []string{"microsoft", "extractor"},
}

// Extractor manages the extraction of data from the database
type Extractor struct {
	plugins.BaseExtractor
	excludedDbs map[string]bool
	logger      log.Logger
	db          *sql.DB
	config      Config
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

	// create mssql client
	e.db, err = sqlutil.OpenWithOtel("mssql", e.config.ConnectionURL, semconv.DBSystemMSSQL)
	if err != nil {
		return fmt.Errorf("create a client: %w", err)
	}

	return nil
}

// Extract checks if the extractor is ready to extract
// and then extract and push data into stream
func (e *Extractor) Extract(ctx context.Context, emit plugins.Emit) error {
	defer e.db.Close()
	e.emit = emit

	dbs, err := sqlutil.FetchDBs(ctx, e.db, e.logger, "SELECT name FROM sys.databases;")
	if err != nil {
		return err
	}
	for _, database := range dbs {
		if e.isExcludedDB(database) {
			continue
		}

		tableQuery := fmt.Sprintf(`SELECT TABLE_NAME FROM %s.INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE';`, database)
		tables, err := sqlutil.FetchTablesInDB(ctx, e.db, database, tableQuery)
		if err != nil {
			e.logger.Error("failed to get tables, skipping database", "error", err)
			continue
		}

		for _, tableName := range tables {
			if err := e.processTable(ctx, database, tableName); err != nil {
				return fmt.Errorf("process Table: %w", err)
			}
		}
	}

	return nil
}

// processTable builds and push table to emitter
func (e *Extractor) processTable(ctx context.Context, database, tableName string) error {
	columns, err := e.getColumns(ctx, database, tableName)
	if err != nil {
		return fmt.Errorf("get columns: %w", err)
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
		Urn:     models.NewURN("mssql", e.UrnScope, "table", fmt.Sprintf("%s.%s", database, tableName)),
		Name:    tableName,
		Type:    "table",
		Service: "mssql",
		Data:    table,
	}))

	return nil
}

// getColumns extract columns from the given table
func (e *Extractor) getColumns(ctx context.Context, database, tableName string) ([]*v1beta2.Column, error) {
	//nolint:gosec
	query := fmt.Sprintf(
		`SELECT COLUMN_NAME, DATA_TYPE, 
		IS_NULLABLE, coalesce(CHARACTER_MAXIMUM_LENGTH,0) 
		FROM %s.information_schema.columns 
		WHERE TABLE_NAME = ?
		ORDER BY COLUMN_NAME ASC`, database)
	rows, err := e.db.QueryContext(ctx, query, tableName)
	if err != nil {
		return nil, fmt.Errorf("execute query: %w", err)
	}
	defer rows.Close()

	var columns []*v1beta2.Column
	for rows.Next() {
		var fieldName, dataType, isNullableString string
		var length int
		if err = rows.Scan(&fieldName, &dataType, &isNullableString, &length); err != nil {
			e.logger.Error("failed to scan fields", "error", err)
			continue
		}
		columns = append(columns, &v1beta2.Column{
			Name:       fieldName,
			DataType:   dataType,
			IsNullable: e.isNullable(isNullableString),
			Length:     int64(length),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate result rows: %w", err)
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
	if err := registry.Extractors.Register("mssql", func() plugins.Extractor {
		return New(plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}
