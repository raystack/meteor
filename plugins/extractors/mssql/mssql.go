package mssql

import (
	"context"
	"database/sql"
	_ "embed" // used to print the embedded assets
	"fmt"

	_ "github.com/denisenkom/go-mssqldb"
	"github.com/raystack/meteor/models"
	meteorv1beta1 "github.com/raystack/meteor/models/raystack/meteor/v1beta1"
	"github.com/raystack/meteor/plugins"
	"github.com/raystack/meteor/plugins/sqlutil"
	"github.com/raystack/meteor/registry"
	log "github.com/raystack/salt/observability/logger"
	semconv "go.opentelemetry.io/otel/semconv/v1.20.0"
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
	ConnectionURL string  `json:"connection_url" yaml:"connection_url" mapstructure:"connection_url" validate:"required"`
	Exclude       Exclude `json:"exclude" yaml:"exclude" mapstructure:"exclude"`
}

type Exclude struct {
	Databases []string `json:"databases" yaml:"databases" mapstructure:"databases"`
	Tables    []string `json:"tables" yaml:"tables" mapstructure:"tables"`
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
	excludedTbl map[string]bool
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
	excludeDBList := append(defaultDBList, e.config.Exclude.Databases...)
	e.excludedDbs = sqlutil.BuildBoolMap(excludeDBList)
	e.excludedTbl = sqlutil.BuildBoolMap(e.config.Exclude.Tables)

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
			if e.isExcludedTable(tableName, database) {
				continue
			}
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

	tableURN := models.NewURN("mssql", e.UrnScope, "table", fmt.Sprintf("%s.%s", database, tableName))
	entity := models.NewEntity(tableURN, "table", tableName, "mssql", map[string]any{"columns": columns})

	edges, err := e.getForeignKeyEdges(ctx, database, tableName, tableURN)
	if err != nil {
		e.logger.Warn("unable to fetch foreign key info", "err", err, "table", fmt.Sprintf("%s.%s", database, tableName))
	}

	e.emit(models.NewRecord(entity, edges...))
	return nil
}

// getForeignKeyEdges queries foreign key constraints and returns lineage edges.
func (e *Extractor) getForeignKeyEdges(ctx context.Context, database, tableName, tableURN string) ([]*meteorv1beta1.Edge, error) {
	//nolint:gosec
	query := fmt.Sprintf(
		`SELECT DISTINCT OBJECT_NAME(fk.referenced_object_id) AS referenced_table
		FROM %s.sys.foreign_keys fk
		WHERE OBJECT_NAME(fk.parent_object_id) = ?`, database)

	rows, err := e.db.QueryContext(ctx, query, tableName)
	if err != nil {
		return nil, fmt.Errorf("execute foreign key query: %w", err)
	}
	defer rows.Close()

	var edges []*meteorv1beta1.Edge
	for rows.Next() {
		var referencedTable string
		if err := rows.Scan(&referencedTable); err != nil {
			e.logger.Error("failed to scan foreign key row", "error", err)
			continue
		}
		targetURN := models.NewURN("mssql", e.UrnScope, "table", fmt.Sprintf("%s.%s", database, referencedTable))
		edges = append(edges, models.LineageEdge(tableURN, targetURN, "mssql"))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate over foreign keys: %w", err)
	}

	return edges, nil
}

// getColumns extract columns from the given table
func (e *Extractor) getColumns(ctx context.Context, database, tableName string) ([]any, error) {
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

	var columns []any
	for rows.Next() {
		var fieldName, dataType, isNullableString string
		var length int
		if err = rows.Scan(&fieldName, &dataType, &isNullableString, &length); err != nil {
			e.logger.Error("failed to scan fields", "error", err)
			continue
		}
		col := map[string]any{
			"name":        fieldName,
			"data_type":   dataType,
			"is_nullable": e.isNullable(isNullableString),
		}
		if length != 0 {
			col["length"] = int64(length)
		}
		columns = append(columns, col)
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

// isExcludedTable checks if the given table is in the list of excluded tables
func (e *Extractor) isExcludedTable(tableName, database string) bool {
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
	if err := registry.Extractors.Register("mssql", func() plugins.Extractor {
		return New(plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}
