package presto

import (
	"context"
	"database/sql"
	_ "embed"
	"fmt"
	"net/url"
	"strings"

	"github.com/odpf/meteor/models"
	v1beta2 "github.com/odpf/meteor/models/odpf/assets/v1beta2"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/odpf/meteor/plugins/sqlutil"

	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/registry"
	"github.com/odpf/salt/log"
	_ "github.com/prestodb/presto-go-client/presto" // presto driver
)

//go:embed README.md
var summary string

// Config holds the set of configuration options for the extractor
type Config struct {
	ConnectionURL string `json:"connection_url" yaml:"connection_url" mapstructure:"connection_url" validate:"required"`
	Exclude       string `json:"exclude_catalog" yaml:"exclude_catalog" mapstructure:"exclude_catalog"`
}

var sampleConfig = `
connection_url: "http://user:pass@localhost:8080"
exclude_catalog: "memory,system,tpcds,tpch"`

var info = plugins.Info{
	Description:  "Table metadata from Presto server.",
	SampleConfig: sampleConfig,
	Summary:      summary,
	Tags:         []string{"oss", "extractor"},
}

// Extractor manages the extraction of data
type Extractor struct {
	plugins.BaseExtractor
	logger          log.Logger
	config          Config
	client          *sql.DB
	excludedCatalog map[string]bool

	// These below values are used to recreate a connection for each catalog
	host     string
	username string
	password string
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

	// build excluded catalog list
	var excludeList []string
	excludeList = append(excludeList, strings.Split(e.config.Exclude, ",")...)
	e.excludedCatalog = sqlutil.BuildBoolMap(excludeList)

	// create presto client
	if e.client, err = sql.Open("presto", e.config.ConnectionURL); err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}

	if err = e.extractConnectionComponents(e.config.ConnectionURL); err != nil {
		err = fmt.Errorf("failed to split configs from connection string: %w", err)
		return
	}

	return
}

// Extract collects metadata of the database through emitter
func (e *Extractor) Extract(_ context.Context, emit plugins.Emit) (err error) {
	defer e.client.Close()

	catalogs, err := e.getCatalogs()
	for _, catalog := range catalogs {
		// Open a new connection to the given catalogs list
		// to collect schemas information

		db, err := e.connection(catalog)
		if err != nil {
			e.logger.Error("failed to connect, skipping catalog", "error", err)
			continue
		}

		dbQuery := fmt.Sprintf("SHOW SCHEMAS IN %s", catalog)

		dbs, err := sqlutil.FetchDBs(db, e.logger, dbQuery)
		if err != nil {
			return fmt.Errorf("failed to extract tables from %s: %w", catalog, err)
		}
		for _, database := range dbs {
			showTablesQuery := fmt.Sprintf("SHOW TABLES FROM %s.%s", catalog, database)
			tables, err := sqlutil.FetchTablesInDB(db, database, showTablesQuery)
			if err != nil {
				e.logger.Error("failed to get tables, skipping database", "catalog", catalog, "error", err)
				continue
			}

			for _, table := range tables {
				result, err := e.processTable(db, catalog, database, table)
				if err != nil {
					e.logger.Error("failed to get table metadata, skipping table", "error", err)
					continue
				}
				// Publish metadata to channel
				emit(models.NewRecord(result))
			}
		}
	}
	return nil
}

// getCatalogs prepares the list of catalogs
func (e *Extractor) getCatalogs() (list []string, err error) {
	// Get list of catalogs
	catalogs, err := e.client.Query("SHOW CATALOGS")
	if err != nil {
		return nil, fmt.Errorf("failed to get the list of catalogs: %w", err)
	}

	for catalogs.Next() {
		var catalog string
		if err = catalogs.Scan(&catalog); err != nil {
			return nil, fmt.Errorf("failed to scan schema from %s: %w", catalog, err)
		}
		if e.isExcludedCatalog(catalog) {
			continue
		}
		list = append(list, catalog)
	}

	return list, err
}

// processTable builds and push table to out channel
func (e *Extractor) processTable(db *sql.DB, catalog string, database string, tableName string) (result *v1beta2.Asset, err error) {
	var columns []*v1beta2.Column
	columns, err = e.extractColumns(db, catalog)
	if err != nil {
		return result, fmt.Errorf("failed to extract columns: %w", err)
	}
	table, err := anypb.New(&v1beta2.Table{
		Columns: columns,
	})
	if err != nil {
		err = fmt.Errorf("error creating Any struct: %w", err)
		return nil, err
	}
	// push table to channel
	result = &v1beta2.Asset{
		Urn:     models.NewURN("presto", e.UrnScope, "table", fmt.Sprintf("%s.%s.%s", catalog, database, tableName)),
		Name:    tableName,
		Service: "presto",
		Type:    "table",
		Data:    table,
	}

	return
}

// extractColumns extracts columns from a given table
func (e *Extractor) extractColumns(db *sql.DB, catalog string) (result []*v1beta2.Column, err error) {
	sqlStr := fmt.Sprintf(`SELECT COLUMN_NAME,DATA_TYPE,IS_NULLABLE,COMMENT
				FROM %s.information_schema.columns
				ORDER BY COLUMN_NAME ASC`, catalog)
	rows, err := db.Query(sqlStr)
	if err != nil {
		return nil, fmt.Errorf("failed to execute a query to extract columns metadata: %w", err)
	}

	for rows.Next() {
		var fieldName, dataType, isNullableString, comment sql.NullString
		err = rows.Scan(&fieldName, &dataType, &isNullableString, &comment)
		if err != nil {
			return nil, fmt.Errorf("failed to scan fields from query: %w", err)
		}

		result = append(result, &v1beta2.Column{
			Name:        fieldName.String,
			DataType:    dataType.String,
			IsNullable:  isNullable(isNullableString.String),
			Description: comment.String,
		})
	}

	return result, nil
}

// isNullable returns true if the string is "YES"
func isNullable(value string) bool {
	return value == "YES"
}

// connection generates a connection string
func (e *Extractor) connection(catalog string) (db *sql.DB, err error) {
	var connStr string
	if len(e.password) != 0 {
		connStr = fmt.Sprintf("http://%s:%s@%s?catalog=%s", e.username, e.password, e.host, catalog)
	} else {
		connStr = fmt.Sprintf("http://%s@%s?catalog=%s", e.username, e.host, catalog)
	}

	return sql.Open("presto", connStr)
}

// extractConnectionComponents extracts the components from the connection URL
func (e *Extractor) extractConnectionComponents(connectionURL string) (err error) {
	connectionStr, err := url.Parse(connectionURL)
	if err != nil {
		err = fmt.Errorf("failed to parse connection url: %w", err)
		return
	}
	e.host = connectionStr.Host
	e.username = connectionStr.User.Username()
	e.password, _ = connectionStr.User.Password()

	return
}

// checks if the catalog is in the ignored catalogs
func (e *Extractor) isExcludedCatalog(catalog string) bool {
	_, ok := e.excludedCatalog[catalog]
	return ok
}

// Register the extractor to catalog
func init() {
	if err := registry.Extractors.Register("presto", func() plugins.Extractor {
		return New(plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}
