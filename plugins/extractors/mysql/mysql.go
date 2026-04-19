package mysql

import (
	"context"
	"database/sql"
	_ "embed" // used to print the embedded assets
	"fmt"

	_ "github.com/go-sql-driver/mysql"
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
	Tags:         []string{"oss", "database"},
	Entities: []plugins.EntityInfo{
		{Type: "table", URNPattern: "urn:mysql:{scope}:table:{database}.{table}"},
	},
	Edges: []plugins.EdgeInfo{
		{Type: "references", From: "table", To: "table"},
	},
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
	err = e.BaseExtractor.Init(ctx, config)
	if err != nil {
		return err
	}

	excludeDBList := append(defaultDBList, e.config.Exclude.Databases...)
	e.excludedDbs = sqlutil.BuildBoolMap(excludeDBList)
	e.excludedTbl = sqlutil.BuildBoolMap(e.config.Exclude.Tables)

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
		// skip excluded databases
		if e.isExcludedDB(db) {
			continue
		}
		// extract tables
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
	// set database
	_, err := e.db.Exec(fmt.Sprintf("USE %s;", database))
	if err != nil {
		return fmt.Errorf("iterate over %s: %w", database, err)
	}

	// get list of tables
	tables, err := sqlutil.FetchTablesInDB(ctx, e.db, database, "SHOW TABLES;")
	for _, tableName := range tables {
		// skip excluded tables
		if e.isExcludedTable(database, tableName) {
			continue
		}
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

	tableURN := models.NewURN("mysql", e.UrnScope, "table", fmt.Sprintf("%s.%s", database, tableName))
	entity := models.NewEntity(tableURN, "table", tableName, "mysql", map[string]any{"columns": columns})

	edges, err := e.getForeignKeyEdges(ctx, database, tableName, tableURN)
	if err != nil {
		e.logger.Warn("unable to fetch foreign key info", "err", err, "table", fmt.Sprintf("%s.%s", database, tableName))
	}

	e.emit(models.NewRecord(entity, edges...))
	return nil
}

// getForeignKeyEdges queries foreign key constraints and returns references edges.
func (e *Extractor) getForeignKeyEdges(ctx context.Context, database, tableName, tableURN string) ([]*meteorv1beta1.Edge, error) {
	query := `SELECT DISTINCT REFERENCED_TABLE_NAME
		FROM information_schema.KEY_COLUMN_USAGE
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
		  AND REFERENCED_TABLE_NAME IS NOT NULL;`

	rows, err := e.db.QueryContext(ctx, query, database, tableName)
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
		targetURN := models.NewURN("mysql", e.UrnScope, "table", fmt.Sprintf("%s.%s", database, referencedTable))
		edges = append(edges, models.ReferencesEdge(tableURN, targetURN, "mysql"))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate over foreign keys: %w", err)
	}

	return edges, nil
}

// Extract columns from a given table
func (e *Extractor) extractColumns(ctx context.Context, tableName string) ([]any, error) {
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

	var columns []any
	for rows.Next() {
		var fieldName, fieldDesc, dataType, isNullableString string
		var length int
		if err = rows.Scan(&fieldName, &fieldDesc, &dataType, &isNullableString, &length); err != nil {
			e.logger.Error("failed to get fields", "error", err)
			continue
		}

		col := map[string]any{
			"name":        fieldName,
			"data_type":   dataType,
			"is_nullable": e.isNullable(isNullableString),
		}
		if fieldDesc != "" {
			col["description"] = fieldDesc
		}
		if length != 0 {
			col["length"] = int64(length)
		}
		columns = append(columns, col)
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
