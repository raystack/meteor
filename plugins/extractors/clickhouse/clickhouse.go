package clickhouse

import (
	"context"
	"database/sql"
	_ "embed" // used to print the embedded assets
	"fmt"

	_ "github.com/ClickHouse/clickhouse-go" // clickhouse driver
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
	Description:  "Column-oriented DBMS for online analytical processing.",
	SampleConfig: sampleConfig,
	Summary:      summary,
	Tags:         []string{"oss", "extractor"},
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

		table, err := anypb.New(&v1beta2.Table{
			Columns:    columns,
			Attributes: &structpb.Struct{}, // ensure attributes don't get overwritten if present
		})
		if err != nil {
			return fmt.Errorf("create Any struct: %w", err)
		}

		asset := v1beta2.Asset{
			Urn:     models.NewURN("clickhouse", e.UrnScope, "table", fmt.Sprintf("%s.%s", dbName, tableName)),
			Name:    tableName,
			Type:    "table",
			Service: "clickhouse",
			Data:    table,
		}
		emit(models.NewRecord(&asset))
	}
	if err := res.Err(); err != nil {
		return fmt.Errorf("iterate over tables: %w", err)
	}

	return nil
}

func (e *Extractor) getColumnsInfo(ctx context.Context, dbName, tableName string) ([]*v1beta2.Column, error) {
	sqlStr := fmt.Sprintf("DESCRIBE TABLE %s.%s", dbName, tableName)

	rows, err := e.db.QueryContext(ctx, sqlStr)
	if err != nil {
		return nil, fmt.Errorf("execute query: %w", err)
	}
	defer rows.Close()

	var result []*v1beta2.Column
	for rows.Next() {
		var colName, colDesc, dataType string
		var temp1, temp2, temp3, temp4 string
		err := rows.Scan(&colName, &dataType, &colDesc, &temp1, &temp2, &temp3, &temp4)
		if err != nil {
			return nil, err
		}
		result = append(result, &v1beta2.Column{
			Name:        colName,
			DataType:    dataType,
			Description: colDesc,
		})
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
