package oracle

import (
	"context"
	"database/sql"
	_ "embed"
	"fmt"

	"github.com/goto/meteor/models"
	v1beta2 "github.com/goto/meteor/models/gotocompany/assets/v1beta2"
	"github.com/goto/meteor/plugins"
	"github.com/goto/meteor/plugins/sqlutil"
	"github.com/goto/meteor/registry"
	"github.com/goto/salt/log"
	_ "github.com/sijms/go-ora/v2"
	semconv "go.opentelemetry.io/otel/semconv/v1.20.0"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"
)

var summary string

// Config holds the set of configuration options for the extractor
type Config struct {
	ConnectionURL string `mapstructure:"connection_url" validate:"required"`
}

var sampleConfig = `connection_url: oracle://username:passwd@localhost:1521/xe`

var info = plugins.Info{
	Description:  "Table metadata oracle SQL Database.",
	SampleConfig: sampleConfig,
	Summary:      summary,
	Tags:         []string{"oss", "extractor"},
}

// Extractor manages the extraction of data from the extractor
type Extractor struct {
	plugins.BaseExtractor
	logger log.Logger
	config Config
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
func (e *Extractor) Init(ctx context.Context, config plugins.Config) (err error) {
	err = e.BaseExtractor.Init(ctx, config)
	if err != nil {
		return err
	}

	e.db, err = sqlutil.OpenWithOtel("oracle", e.config.ConnectionURL, semconv.DBSystemOracle)
	if err != nil {
		return fmt.Errorf("create a client: %w", err)
	}

	return nil
}

// Extract collects metadata from the source. Metadata is collected through the emitter
func (e *Extractor) Extract(ctx context.Context, emit plugins.Emit) error {
	defer e.db.Close()

	// Get username
	userName, err := e.getUserName(ctx, e.db)
	if err != nil {
		return fmt.Errorf("get user name: %w", err)
	}

	// Get DB name
	dbs, err := sqlutil.FetchDBs(ctx, e.db, e.logger, "select ora_database_name from dual")
	if err != nil {
		return err
	}
	database := dbs[0]

	tableQuery := fmt.Sprintf(`SELECT object_name 
 		FROM all_objects
		WHERE object_type = 'TABLE'
		AND upper(owner) = upper('%s')`, userName)
	tables, err := sqlutil.FetchTablesInDB(ctx, e.db, database, tableQuery)
	if err != nil {
		e.logger.Error("failed to get tables, skipping database", "error", err)
		// continue
	}

	for _, table := range tables {
		result, err := e.getTableMetadata(ctx, e.db, database, table)
		if err != nil {
			e.logger.Error("failed to get table metadata, skipping table", "error", err)
			// continue
		}
		// Publish metadata to channel
		emit(models.NewRecord(result))
	}

	return nil
}

func (*Extractor) getUserName(ctx context.Context, db *sql.DB) (string, error) {
	sqlStr := `select user from dual`

	rows, err := db.QueryContext(ctx, sqlStr)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	var userName string
	for rows.Next() {
		if err := rows.Scan(&userName); err != nil {
			return "", err
		}
	}
	if err := rows.Err(); err != nil {
		return "", fmt.Errorf("iterate user rows: %w", err)
	}

	return userName, nil
}

// Prepares the list of tables and the attached metadata
func (e *Extractor) getTableMetadata(ctx context.Context, db *sql.DB, dbName, tableName string) (*v1beta2.Asset, error) {
	columns, err := e.getColumnMetadata(ctx, db, tableName)
	if err != nil {
		return nil, err
	}

	// get table row count
	rows, err := db.QueryContext(ctx, `select count(*) from `+tableName) //nolint:gosec
	if err != nil {
		return nil, fmt.Errorf("run query for count: %w", err)
	}
	defer rows.Close()

	var rowCount int64
	for rows.Next() {
		if err = rows.Scan(&rowCount); err != nil {
			e.logger.Error("failed to get fields", "error", err)
			continue
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("scan row count: %w", err)
	}

	table, err := anypb.New(&v1beta2.Table{
		Columns: columns,
		Profile: &v1beta2.TableProfile{
			TotalRows: rowCount,
		},
		Attributes: &structpb.Struct{}, // ensure attributes don't get overwritten if present
	})
	if err != nil {
		return nil, fmt.Errorf("create Any struct: %w", err)
	}
	return &v1beta2.Asset{
		Urn:     models.NewURN("oracle", e.UrnScope, "table", fmt.Sprintf("%s.%s", dbName, tableName)),
		Name:    tableName,
		Service: "Oracle",
		Type:    "table",
		Data:    table,
	}, nil
}

// Prepares the list of columns and the attached metadata
func (e *Extractor) getColumnMetadata(ctx context.Context, db *sql.DB, tableName string) ([]*v1beta2.Column, error) {
	sqlStr := `select utc.column_name, utc.data_type, 
			decode(utc.char_used, 'C', utc.char_length, utc.data_length) as data_length,
			utc.nullable, nvl(ucc.comments, '') as col_comment
			from USER_TAB_COLUMNS utc
			INNER JOIN USER_COL_COMMENTS ucc ON
			utc.column_name = ucc.column_name AND
			utc.table_name = ucc.table_name
			WHERE utc.table_name ='%s'`

	rows, err := db.QueryContext(ctx, fmt.Sprintf(sqlStr, tableName))
	if err != nil {
		return nil, fmt.Errorf("fetch query results: %w", err)
	}
	defer rows.Close()

	var result []*v1beta2.Column
	for rows.Next() {
		var fieldName, dataType, isNullableString string
		var fieldDesc sql.NullString
		var length int
		if err = rows.Scan(&fieldName, &dataType, &length, &isNullableString, &fieldDesc); err != nil {
			e.logger.Error("failed to get fields", "error", err)
			continue
		}

		result = append(result, &v1beta2.Column{
			Name:        fieldName,
			DataType:    dataType,
			Description: fieldDesc.String,
			IsNullable:  isNullable(isNullableString),
			Length:      int64(length),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate over query results: %w", err)
	}

	return result, nil
}

// Convert nullable string to a boolean
func isNullable(value string) bool {
	return value == "Y"
}

// Register the extractor to catalog
func init() {
	if err := registry.Extractors.Register("oracle", func() plugins.Extractor {
		return New(plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}
