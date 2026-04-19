package clickhouse

import (
	"context"
	"database/sql"
	_ "embed" // used to print the embedded assets
	"fmt"

	_ "github.com/ClickHouse/clickhouse-go" // clickhouse driver
	"github.com/raystack/meteor/models"
	"github.com/raystack/meteor/plugins"
	"github.com/raystack/meteor/plugins/sqlutil"
	"github.com/raystack/meteor/registry"
	log "github.com/raystack/salt/observability/logger"
	semconv "go.opentelemetry.io/otel/semconv/v1.20.0"
)

//go:embed README.md
var summary string

// Config holds the connection URL for the extractor
type Config struct {
	ConnectionURL string  `json:"connection_url" yaml:"connection_url" mapstructure:"connection_url" validate:"required"`
	Exclude       Exclude `json:"exclude" yaml:"exclude" mapstructure:"exclude"`
}

type Exclude struct {
	Databases []string `json:"databases" yaml:"databases" mapstructure:"databases"`
	Tables    []string `json:"tables" yaml:"tables" mapstructure:"tables"`
}

var sampleConfig = `connection_url: "tcp://localhost:3306?username=admin&password=pass123&debug=true"
exclude:
  databases: [database_a, database_b]
  tables: [dataset_c.table_a]`

var info = plugins.Info{
	Description:  "Table metadata from ClickHouse server.",
	SampleConfig: sampleConfig,
	Summary:      summary,
	Tags:         []string{"oss", "database"},
	Entities: []plugins.EntityInfo{
		{Type: "table", URNPattern: "urn:clickhouse:{scope}:table:{database}.{table}"},
	},
}

// Extractor manages the output stream
// and logger interface for the extractor
type Extractor struct {
	plugins.BaseExtractor
	config      Config
	logger      log.Logger
	excludedDBs map[string]bool
	excludedTbl map[string]bool
	db          *sql.DB
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

	// initialize excluded databases and tables
	e.excludedDBs = sqlutil.BuildBoolMap(e.config.Exclude.Databases)
	e.excludedTbl = sqlutil.BuildBoolMap(e.config.Exclude.Tables)

	e.db, err = sqlutil.OpenWithOtel("clickhouse", e.config.ConnectionURL, semconv.DBSystemClickhouse)
	if err != nil {
		return fmt.Errorf("create a client: %w", err)
	}

	return nil
}

// Extract checks if the extractor is configured and
// if the connection to the DB is successful
// and then starts the extraction process
func (e *Extractor) Extract(ctx context.Context, emit plugins.Emit) error {
	if err := e.extractTables(ctx, emit); err != nil {
		return fmt.Errorf("extract tables: %w", err)
	}

	return nil
}

// extractTables extract tables from a given database
func (e *Extractor) extractTables(ctx context.Context, emit plugins.Emit) error {
	res, err := e.db.QueryContext(ctx, "SELECT name, database FROM system.tables WHERE database not like 'system'")
	if err != nil {
		return fmt.Errorf("execute query: %w", err)
	}
	defer res.Close()

	for res.Next() {
		var dbName, tableName string
		if err := res.Scan(&tableName, &dbName); err != nil {
			return err
		}

		// skip excluded databases and tables
		if e.excludedDBs[dbName] || e.excludedTbl[fmt.Sprintf("%s.%s", dbName, tableName)] {
			continue
		}

		columns, err := e.getColumnsInfo(ctx, dbName, tableName)
		if err != nil {
			return err
		}

		entity := models.NewEntity(
			models.NewURN("clickhouse", e.UrnScope, "table", fmt.Sprintf("%s.%s", dbName, tableName)),
			"table", tableName, "clickhouse",
			map[string]any{"columns": columns},
		)
		emit(models.NewRecord(entity))
	}
	if err := res.Err(); err != nil {
		return fmt.Errorf("iterate over tables: %w", err)
	}

	return nil
}

func (e *Extractor) getColumnsInfo(ctx context.Context, dbName, tableName string) ([]any, error) {
	sqlStr := fmt.Sprintf("DESCRIBE TABLE %s.%s", dbName, tableName)

	rows, err := e.db.QueryContext(ctx, sqlStr)
	if err != nil {
		return nil, fmt.Errorf("execute query: %w", err)
	}
	defer rows.Close()

	var result []any
	for rows.Next() {
		var colName, colDesc, dataType string
		var temp1, temp2, temp3, temp4 string
		err := rows.Scan(&colName, &dataType, &colDesc, &temp1, &temp2, &temp3, &temp4)
		if err != nil {
			return nil, err
		}
		col := map[string]any{
			"name":      colName,
			"data_type": dataType,
		}
		if colDesc != "" {
			col["description"] = colDesc
		}
		result = append(result, col)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate over columns: %w", err)
	}

	return result, nil
}

// Register the extractor to catalog
func init() {
	if err := registry.Extractors.Register("clickhouse", func() plugins.Extractor {
		return New(plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}
