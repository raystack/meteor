package postgres

import (
	"context"
	"database/sql"
	_ "embed" // // used to print the embedded assets
	"fmt"
	"net/url"
	"strings"

	_ "github.com/lib/pq" // register postgres driver
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

var defaultDBList = []string{"information_schema", "root", "postgres"}

// Config holds the set of configuration options for the extractor
type Config struct {
	ConnectionURL string `json:"connection_url" yaml:"connection_url" mapstructure:"connection_url" validate:"required"`
	Exclude       string `json:"exclude" yaml:"exclude" mapstructure:"exclude"`
}

var sampleConfig = `
connection_url: "postgres://admin:pass123@localhost:3306/postgres?sslmode=disable"
exclude: testDB,secondaryDB`

var info = plugins.Info{
	Description:  "Table metadata and metrics from Postgres SQL sever.",
	SampleConfig: sampleConfig,
	Summary:      summary,
	Tags:         []string{"oss", "extractor"},
}

// Extractor manages the extraction of data from the extractor
type Extractor struct {
	plugins.BaseExtractor
	excludedDbs map[string]bool
	logger      log.Logger
	config      Config
	db          *sql.DB

	// These below values are used to recreate a connection for each database
	host     string
	username string
	password string
	sslmode  string
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
	excludeList := append(defaultDBList, strings.Split(e.config.Exclude, ",")...)
	e.excludedDbs = sqlutil.BuildBoolMap(excludeList)

	// Create database connection
	e.db, err = sqlutil.OpenWithOtel("postgres", e.config.ConnectionURL, semconv.DBSystemPostgreSQL)
	if err != nil {
		return fmt.Errorf("create a client: %w", err)
	}

	if err := e.extractConnectionComponents(e.config.ConnectionURL); err != nil {
		return fmt.Errorf("split host from connection string: %w", err)
	}

	return nil
}

// Extract collects metadata from the source. Metadata is collected through the emitter
func (e *Extractor) Extract(ctx context.Context, emit plugins.Emit) error {
	defer e.db.Close()

	// Get list of databases
	dbs, err := sqlutil.FetchDBs(ctx, e.db, e.logger, "SELECT datname FROM pg_database WHERE datistemplate = false;")
	if err != nil {
		return fmt.Errorf("fetch databases: %w", err)
	}

	// Iterate through all tables and databases
	for _, database := range dbs {
		// skip dbs meant to be excluded
		if e.isExcludedDB(database) {
			continue
		}
		// Open a new connection to the given database to collect
		// tables information without this default database
		// information will be returned

		db, err := e.connection(database)
		if err != nil {
			e.logger.Error("failed to connect, skipping database", "error", err)
			continue
		}
		query := `SELECT table_name
		FROM information_schema.tables
		WHERE table_schema = 'public'
		ORDER BY table_name;`

		_, err = db.Exec(fmt.Sprintf("SET search_path TO %s, public;", database))
		if err != nil {
			e.logger.Error("failed to get tables, skipping database", "error", err)
			continue
		}
		tables, err := sqlutil.FetchTablesInDB(ctx, db, database, query)
		if err != nil {
			e.logger.Error("failed to get tables, skipping database", "error", err)
			continue
		}

		for _, table := range tables {
			result, err := e.getTableMetadata(ctx, db, database, table)
			if err != nil {
				e.logger.Error("failed to get table metadata, skipping table", "error", err)
				continue
			}
			// Publish metadata to channel
			emit(models.NewRecord(result))
		}
	}

	return nil
}

// Prepares the list of tables and the attached metadata
func (e *Extractor) getTableMetadata(ctx context.Context, db *sql.DB, dbName, tableName string) (*meteorv1beta1.Entity, error) {
	columns, err := e.getColumnMetadata(ctx, db, tableName)
	if err != nil {
		return nil, err
	}

	props := map[string]interface{}{
		"columns": columns,
	}

	usrPrivilegeInfo, err := e.userPrivilegesInfo(ctx, db, dbName, tableName)
	if err != nil {
		e.logger.Warn("unable to fetch user privileges info", "err", err, "table", fmt.Sprintf("%s.%s", dbName, tableName))
	}
	if usrPrivilegeInfo != nil {
		props["grants"] = usrPrivilegeInfo["grants"]
	}

	return models.NewEntity(
		models.NewURN("postgres", e.UrnScope, "table", fmt.Sprintf("%s.%s", dbName, tableName)),
		"table", tableName, "postgres",
		props,
	), nil
}

// Prepares the list of columns and the attached metadata
func (e *Extractor) getColumnMetadata(ctx context.Context, db *sql.DB, tableName string) ([]interface{}, error) {
	sqlStr := `SELECT COLUMN_NAME,DATA_TYPE,
				IS_NULLABLE,coalesce(CHARACTER_MAXIMUM_LENGTH,0)
				FROM information_schema.columns
				WHERE TABLE_NAME = '%s' ORDER BY COLUMN_NAME ASC;`
	rows, err := db.QueryContext(ctx, fmt.Sprintf(sqlStr, tableName))
	if err != nil {
		return nil, fmt.Errorf("execute query: %w", err)
	}
	defer rows.Close()

	var result []interface{}
	for rows.Next() {
		var fieldName, dataType, isNullableString string
		var length int
		if err = rows.Scan(&fieldName, &dataType, &isNullableString, &length); err != nil {
			e.logger.Error("failed to get fields", "error", err)
			continue
		}
		col := map[string]interface{}{
			"name":        fieldName,
			"data_type":   dataType,
			"is_nullable": isNullable(isNullableString),
		}
		if length != 0 {
			col["length"] = int64(length)
		}
		result = append(result, col)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate over table columns: %w", err)
	}

	return result, nil
}

func (e *Extractor) userPrivilegesInfo(ctx context.Context, db *sql.DB, dbName, tableName string) (map[string]interface{}, error) {
	query := `SELECT grantee, string_agg(privilege_type, ',')
	FROM information_schema.role_table_grants
	WHERE table_name='%s' AND table_catalog='%s'
	GROUP BY grantee;`

	rows, err := db.QueryContext(ctx, fmt.Sprintf(query, tableName, dbName))
	if err != nil {
		return nil, fmt.Errorf("execute query: %w", err)
	}
	defer rows.Close()

	var usrs []interface{}
	for rows.Next() {
		var grantee, privilege_type string
		if err := rows.Scan(&grantee, &privilege_type); err != nil {
			e.logger.Error("failed to get fields", "error", err)
			continue
		}

		usrs = append(usrs, map[string]interface{}{
			"user":            grantee,
			"privilege_types": ConvertStringListToInterface(strings.Split(privilege_type, ",")),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate over user privileges: %w", err)
	}

	return map[string]interface{}{
		"grants": usrs,
	}, nil
}

// Convert nullable string to a boolean
func isNullable(value string) bool {
	return value == "YES"
}

// connection generates a connection string
func (e *Extractor) connection(database string) (*sql.DB, error) {
	connStr := fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=%s", e.username, e.password, e.host, database, e.sslmode)
	return sql.Open("postgres", connStr)
}

// extractConnectionComponents extracts the components from the connection URL
func (e *Extractor) extractConnectionComponents(connectionURL string) error {
	connectionStr, err := url.Parse(connectionURL)
	if err != nil {
		return fmt.Errorf("parse connection URL: %w", err)
	}
	e.host = connectionStr.Host
	e.username = connectionStr.User.Username()
	e.password, _ = connectionStr.User.Password()
	e.sslmode = connectionStr.Query().Get("sslmode")

	return nil
}

// isExcludedDB checks if the given db is in the list of excluded databases
func (e *Extractor) isExcludedDB(database string) bool {
	_, ok := e.excludedDbs[database]
	return ok
}

// Register the extractor to catalog
func init() {
	if err := registry.Extractors.Register("postgres", func() plugins.Extractor {
		return New(plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}

func ConvertStringListToInterface(s []string) []interface{} {
	out := make([]interface{}, len(s))
	for i, v := range s {
		out[i] = v
	}
	return out
}
