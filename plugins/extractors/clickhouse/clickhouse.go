package clickhouse

import (
	"context"
	"database/sql"
	_ "embed" // used to print the embedded assets
	"fmt"

	"github.com/pkg/errors"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"

	_ "github.com/ClickHouse/clickhouse-go" // clickhouse driver
	"github.com/raystack/meteor/models"
	v1beta2 "github.com/raystack/meteor/models/raystack/assets/v1beta2"
	"github.com/raystack/meteor/plugins"
	"github.com/raystack/meteor/plugins/sqlutil"
	"github.com/raystack/meteor/registry"
	"github.com/raystack/salt/log"
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
	if err = e.BaseExtractor.Init(ctx, config); err != nil {
		return err
	}

	// initialize excluded databases and tables
	e.excludedDBs = sqlutil.BuildBoolMap(e.config.Exclude.Databases)
	e.excludedTbl = sqlutil.BuildBoolMap(e.config.Exclude.Tables)

	if e.db, err = sql.Open("clickhouse", e.config.ConnectionURL); err != nil {
		return errors.Wrap(err, "failed to create a client")
	}

	return
}

// Extract checks if the extractor is configured and
// if the connection to the DB is successful
// and then starts the extraction process
func (e *Extractor) Extract(ctx context.Context, emit plugins.Emit) (err error) {
	err = e.extractTables(emit)
	if err != nil {
		return errors.Wrap(err, "failed to extract tables")
	}

	return
}

// extractTables extract tables from a given database
func (e *Extractor) extractTables(emit plugins.Emit) (err error) {
	res, err := e.db.Query("SELECT name, database FROM system.tables WHERE database not like 'system'")
	if err != nil {
		return errors.Wrap(err, "failed to execute query")
	}
	for res.Next() {
		var dbName, tableName string
		err = res.Scan(&tableName, &dbName)
		if err != nil {
			return
		}

		// skip excluded databases and tables
		if e.excludedDBs[dbName] || e.excludedTbl[fmt.Sprintf("%s.%s", dbName, tableName)] {
			continue
		}

		var columns []*v1beta2.Column
		columns, err = e.getColumnsInfo(dbName, tableName)
		if err != nil {
			return
		}

		table, err := anypb.New(&v1beta2.Table{
			Columns:    columns,
			Attributes: &structpb.Struct{}, // ensure attributes don't get overwritten if present
		})
		if err != nil {
			err = fmt.Errorf("error creating Any struct: %w", err)
			return err
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
	return
}

func (e *Extractor) getColumnsInfo(dbName string, tableName string) (result []*v1beta2.Column, err error) {
	sqlStr := fmt.Sprintf("DESCRIBE TABLE %s.%s", dbName, tableName)

	rows, err := e.db.Query(sqlStr)
	if err != nil {
		err = errors.Wrapf(err, "failed to execute query %s", sqlStr)
		return
	}
	for rows.Next() {
		var colName, colDesc, dataType string
		var temp1, temp2, temp3, temp4 string
		err = rows.Scan(&colName, &dataType, &colDesc, &temp1, &temp2, &temp3, &temp4)
		if err != nil {
			return
		}
		result = append(result, &v1beta2.Column{
			Name:        colName,
			DataType:    dataType,
			Description: colDesc,
		})
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
