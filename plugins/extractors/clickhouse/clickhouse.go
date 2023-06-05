package clickhouse

import (
	"context"
	"database/sql"
	_ "embed" // used to print the embedded assets
	"fmt"

	_ "github.com/ClickHouse/clickhouse-go" // clickhouse driver
	"github.com/goto/meteor/models"
	v1beta2 "github.com/goto/meteor/models/gotocompany/assets/v1beta2"
	"github.com/goto/meteor/plugins"
	"github.com/goto/meteor/registry"
	"github.com/goto/salt/log"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"
)

//go:embed README.md
var summary string

// Config holds the connection URL for the extractor
type Config struct {
	ConnectionURL string `mapstructure:"connection_url" validate:"required"`
}

var sampleConfig = `connection_url: "tcp://localhost:3306?username=admin&password=pass123&debug=true"`

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
	config Config
	logger log.Logger
	db     *sql.DB
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
func (e *Extractor) Init(ctx context.Context, config plugins.Config) error {
	if err := e.BaseExtractor.Init(ctx, config); err != nil {
		return err
	}

	var err error
	e.db, err = sql.Open("clickhouse", e.config.ConnectionURL)
	if err != nil {
		return fmt.Errorf("create a client: %w", err)
	}

	return nil
}

// Extract checks if the extractor is configured and
// if the connection to the DB is successful
// and then starts the extraction process
func (e *Extractor) Extract(_ context.Context, emit plugins.Emit) error {
	if err := e.extractTables(emit); err != nil {
		return fmt.Errorf("extract tables: %w", err)
	}

	return nil
}

// extractTables extract tables from a given database
func (e *Extractor) extractTables(emit plugins.Emit) error {
	res, err := e.db.Query("SELECT name, database FROM system.tables WHERE database not like 'system'")
	if err != nil {
		return fmt.Errorf("execute query: %w", err)
	}
	defer res.Close()

	for res.Next() {
		var dbName, tableName string
		if err := res.Scan(&tableName, &dbName); err != nil {
			return err
		}

		columns, err := e.getColumnsInfo(dbName, tableName)
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

func (e *Extractor) getColumnsInfo(dbName, tableName string) ([]*v1beta2.Column, error) {
	sqlStr := fmt.Sprintf("DESCRIBE TABLE %s.%s", dbName, tableName)

	rows, err := e.db.Query(sqlStr)
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
