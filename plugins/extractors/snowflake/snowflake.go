package snowflake

import (
	"context"
	"database/sql"
	_ "embed" // used to print the embedded assets
	"fmt"
	"net/http"

	"github.com/goto/meteor/models"
	v1beta2 "github.com/goto/meteor/models/gotocompany/assets/v1beta2"
	"github.com/goto/meteor/plugins"
	"github.com/goto/meteor/plugins/sqlutil"
	"github.com/goto/meteor/registry"
	"github.com/goto/salt/log"
	"github.com/snowflakedb/gosnowflake"
	_ "github.com/snowflakedb/gosnowflake" // used to register the snowflake driver
	semconv "go.opentelemetry.io/otel/semconv/v1.20.0"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"
)

//go:embed README.md
var summary string

// Config holds the connection URL for the extractor
type Config struct {
	ConnectionURL string `mapstructure:"connection_url" validate:"required"`
}

var (
	sampleConfig = `connection_url: "user:password@my_organization-my_account/mydb"`
	info         = plugins.Info{
		Description:  "Table metadata from Snowflake server.",
		SampleConfig: sampleConfig,
		Summary:      summary,
		Tags:         []string{"oss", "extractor"},
	}
)

// Extractor manages the extraction of data from snowflake
type Extractor struct {
	plugins.BaseExtractor
	logger        log.Logger
	config        Config
	httpTransport http.RoundTripper
	db            *sql.DB
	emit          plugins.Emit
}

// Option provides extension abstraction to Extractor constructor
type Option func(*Extractor)

// WithHTTPTransport assign custom http client to the Extractor constructor
func WithHTTPTransport(htr http.RoundTripper) Option {
	return func(e *Extractor) {
		e.httpTransport = htr
	}
}

// New returns a pointer to an initialized Extractor Object
func New(logger log.Logger, opts ...Option) *Extractor {
	e := &Extractor{
		logger: logger,
	}
	e.BaseExtractor = plugins.NewBaseExtractor(info, &e.config)

	for _, opt := range opts {
		opt(e)
	}

	return e
}

// Init initializes the extractor
func (e *Extractor) Init(ctx context.Context, config plugins.Config) (err error) {
	err = e.BaseExtractor.Init(ctx, config)
	if err != nil {
		return err
	}

	if e.httpTransport == nil {
		// create snowflake client
		e.db, err = sqlutil.OpenWithOtel("snowflake", e.config.ConnectionURL, semconv.DBSystemKey.String("snowflake"))
		if err != nil {
			return fmt.Errorf("create a client: %w", err)
		}

		return nil
	}

	cfg, err := gosnowflake.ParseDSN(e.config.ConnectionURL)
	if err != nil {
		return fmt.Errorf("parse dsn when creating client: %w", err)
	}

	cfg.Transporter = e.httpTransport
	connector := gosnowflake.NewConnector(&gosnowflake.SnowflakeDriver{}, *cfg)
	e.db = sql.OpenDB(connector)

	return nil
}

// Extract collects metadata of the database through emitter
func (e *Extractor) Extract(ctx context.Context, emit plugins.Emit) error {
	defer e.db.Close()
	e.emit = emit

	// Get list of databases
	dbs, err := e.db.QueryContext(ctx, "SHOW DATABASES;")
	if err != nil {
		return fmt.Errorf("get databases: %w", err)
	}
	defer dbs.Close()

	// Iterate through all tables and databases
	for dbs.Next() {
		var createdOn, name, isDefault, isCurrent, origin, owner, comment, options string
		var retentionTime int

		if err = dbs.Scan(&createdOn, &name, &isDefault, &isCurrent, &origin, &owner, &comment, &options, &retentionTime); err != nil {
			return fmt.Errorf("scan database row: %w", err)
		}
		if err = e.extractTables(ctx, name); err != nil {
			return fmt.Errorf("extract tables from %s: %w", name, err)
		}
	}
	if err := dbs.Err(); err != nil {
		return fmt.Errorf("iterate over database rows: %w", err)
	}

	return nil
}

// extractTables extracts tables from a given database
func (e *Extractor) extractTables(ctx context.Context, database string) error {
	// extract tables
	_, err := e.db.Exec(fmt.Sprintf("USE %s;", database))
	if err != nil {
		return fmt.Errorf("execute USE query on %s: %w", database, err)
	}

	rows, err := e.db.QueryContext(ctx, "SHOW TABLES;")
	if err != nil {
		return fmt.Errorf("show tables for %s: %w", database, err)
	}
	defer rows.Close()

	// process each rows
	for rows.Next() {
		var createdOn, name, databaseName, schemaName, kind, comment, clusterBy, owner, autoClustering, changeTracking, isExternal string
		var bytes, rowsCount, retentionTime int

		if err := rows.Scan(&createdOn, &name, &databaseName, &schemaName, &kind, &comment, &clusterBy, &rowsCount,
			&bytes, &owner, &retentionTime, &autoClustering, &changeTracking, &isExternal); err != nil {
			return err
		}
		if err := e.processTable(ctx, database, name); err != nil {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate over tables: %w", err)
	}

	return nil
}

// processTable builds and push table to out channel
func (e *Extractor) processTable(ctx context.Context, database, tableName string) error {
	columns, err := e.extractColumns(ctx, database, tableName)
	if err != nil {
		return fmt.Errorf("extract columns from %s.%s: %w", database, tableName, err)
	}
	data, err := anypb.New(&v1beta2.Table{
		Columns:    columns,
		Attributes: &structpb.Struct{}, // ensure attributes don't get overwritten if present
	})
	if err != nil {
		return fmt.Errorf("create Any struct: %w", err)
	}
	// push table to channel
	e.emit(models.NewRecord(&v1beta2.Asset{
		Urn:     models.NewURN("snowflake", e.UrnScope, "table", fmt.Sprintf("%s.%s", database, tableName)),
		Name:    tableName,
		Service: "Snowflake",
		Type:    "table",
		Data:    data,
	}))

	return nil
}

// extractColumns extracts columns from a given table
func (e *Extractor) extractColumns(ctx context.Context, database, tableName string) ([]*v1beta2.Column, error) {
	// extract columns
	_, err := e.db.Exec(fmt.Sprintf("USE %s;", database))
	if err != nil {
		return nil, fmt.Errorf("execute USE query on %s: %w", database, err)
	}

	sqlStr := `SELECT COLUMN_NAME,COMMENT,DATA_TYPE,IS_NULLABLE,IFNULL(CHARACTER_MAXIMUM_LENGTH,0)
			   FROM information_schema.columns
		       WHERE TABLE_NAME = ?
		       ORDER BY COLUMN_NAME ASC;`
	rows, err := e.db.QueryContext(ctx, sqlStr, tableName)
	if err != nil {
		return nil, fmt.Errorf("execute a query to extract columns metadata: %w", err)
	}
	defer rows.Close()

	var result []*v1beta2.Column
	for rows.Next() {
		var fieldName, fieldDesc, dataType, isNullableString sql.NullString
		var length int

		if err = rows.Scan(&fieldName, &fieldDesc, &dataType, &isNullableString, &length); err != nil {
			return nil, fmt.Errorf("scan fields from query: %w", err)
		}
		result = append(result, &v1beta2.Column{
			Name:        fieldName.String,
			DataType:    dataType.String,
			Description: fieldDesc.String,
			IsNullable:  e.isNullable(isNullableString.String),
			Length:      int64(length),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate over columns: %w", err)
	}

	return result, nil
}

// isNullable returns true if the string is "YES"
func (e *Extractor) isNullable(value string) bool {
	return value == "YES"
}

// Register the extractor to catalog
func init() {
	if err := registry.Extractors.Register("snowflake", func() plugins.Extractor {
		return New(plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}
