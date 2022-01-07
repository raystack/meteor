package postgres

import (
	"context"
	"database/sql"
	_ "embed" // // used to print the embedded assets
	"fmt"
	"net/url"
	"strings"

	"github.com/pkg/errors"

	_ "github.com/lib/pq" // used to register the postgres driver
	"github.com/odpf/meteor/models"
	commonv1beta1 "github.com/odpf/meteor/models/odpf/assets/common/v1beta1"
	facetsv1beta1 "github.com/odpf/meteor/models/odpf/assets/facets/v1beta1"
	assetsv1beta1 "github.com/odpf/meteor/models/odpf/assets/v1beta1"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/registry"
	"github.com/odpf/meteor/utils"
	"github.com/odpf/salt/log"
)

//go:embed README.md
var summary string

var defaults = []string{"information_schema", "root", "postgres"}

// Config holds the set of configuration options for the extractor
type Config struct {
	ConnectionURL string `mapstructure:"connection_url" validate:"required"`
	Exclude       string `mapstructure:"exclude"`
}

var sampleConfig = `
connection_url: "postgres://admin:pass123@localhost:3306/testDB?sslmode=disable"
exclude: testDB`

// Extractor manages the extraction of data from the extractor
type Extractor struct {
	logger   log.Logger
	config   Config
	db       *sql.DB
	userDB   string
	host     string
	username string
	password string
	sslmode  string
}

// New returns a pointer to an initialized Extractor Object
func New(logger log.Logger) *Extractor {
	return &Extractor{
		logger: logger,
	}
}

// Info returns the brief information about the extractor
func (e *Extractor) Info() plugins.Info {
	return plugins.Info{
		Description:  "Table metadata and metrics from Postgres SQL sever.",
		SampleConfig: sampleConfig,
		Summary:      summary,
		Tags:         []string{"oss", "extractor"},
	}
}

// Validate validates the configuration of the extractor
func (e *Extractor) Validate(configMap map[string]interface{}) (err error) {
	return utils.BuildConfig(configMap, &Config{})
}

// Init initializes the extractor
func (e *Extractor) Init(ctx context.Context, config map[string]interface{}) (err error) {
	// Build and validate config received from recipe
	if err := utils.BuildConfig(config, &e.config); err != nil {
		return plugins.InvalidConfigError{}
	}

	if err = e.extractConnectionComponents(e.config.ConnectionURL); err != nil {
		err = errors.Wrap(err, "failed to split host from connection string")
		return
	}

	// Create database connection
	e.db, err = e.connection(e.config, e.userDB)
	if err != nil {
		return errors.Wrap(err, "failed to create connection")
	}

	return
}

// Extract collects metadata from the source. Metadata is collected through the emitter
func (e *Extractor) Extract(ctx context.Context, emit plugins.Emit) (err error) {
	defer e.db.Close()

	// Get list of databases
	dbs, err := e.getDatabases()
	if err != nil {
		return errors.Wrap(err, "failed to fetch databases")
	}

	// Iterate through all tables and databases
	for _, database := range dbs {
		// Open a new connection to the given database to collect
		// tables information without this default database
		// information will be returned

		db, err := e.connection(e.config, database)
		if err != nil {
			e.logger.Error("failed to connect, skipping database", "error", err)
			continue
		}
		tables, err := e.getTables(db, database)
		if err != nil {
			e.logger.Error("failed to get tables, skipping database", "error", err)
			continue
		}

		for _, table := range tables {
			result, err := e.getTableMetadata(db, database, table)
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

func (e *Extractor) getDatabases() (list []string, err error) {
	res, err := e.db.Query("SELECT datname FROM pg_database WHERE datistemplate = false;")
	if err != nil {
		return nil, err
	}

	excludeList := append(defaults, strings.Split(e.config.Exclude, ",")...)

	for res.Next() {
		var database string
		err := res.Scan(&database)
		if err != nil {
			return nil, err
		}
		if exclude(excludeList, database) {
			continue
		}
		list = append(list, database)
	}
	return list, nil
}

func (e *Extractor) getTables(db *sql.DB, dbName string) (list []string, err error) {
	sqlStr := `SELECT table_name
	FROM information_schema.tables
	WHERE table_schema = 'public'
	ORDER BY table_name;`

	_, err = db.Exec(fmt.Sprintf("SET search_path TO %s, public;", dbName))
	if err != nil {
		return
	}
	rows, err := db.Query(sqlStr)
	if err != nil {
		return
	}
	for rows.Next() {
		var table string
		err = rows.Scan(&table)
		if err != nil {
			return
		}
		list = append(list, table)
	}
	return list, err
}

// Prepares the list of tables and the attached metadata
func (e *Extractor) getTableMetadata(db *sql.DB, dbName string, tableName string) (result *assetsv1beta1.Table, err error) {
	var columns []*facetsv1beta1.Column
	columns, err = e.getColumnMetadata(db, dbName, tableName)
	if err != nil {
		return result, nil
	}

	result = &assetsv1beta1.Table{
		Resource: &commonv1beta1.Resource{
			Urn:     models.TableURN("postgres", e.host, dbName, tableName),
			Name:    tableName,
			Service: "postgres",
		},
		Schema: &facetsv1beta1.Columns{
			Columns: columns,
		},
	}

	return
}

// Prepares the list of columns and the attached metadata
func (e *Extractor) getColumnMetadata(db *sql.DB, dbName string, tableName string) (result []*facetsv1beta1.Column, err error) {
	sqlStr := `SELECT COLUMN_NAME,DATA_TYPE,
				IS_NULLABLE,coalesce(CHARACTER_MAXIMUM_LENGTH,0)
				FROM information_schema.columns
				WHERE TABLE_NAME = '%s' ORDER BY COLUMN_NAME ASC;`
	rows, err := db.Query(fmt.Sprintf(sqlStr, tableName))
	if err != nil {
		err = errors.Wrap(err, "failed to fetch data from query")
		return
	}
	for rows.Next() {
		var fieldName, dataType, isNullableString string
		var length int
		if err = rows.Scan(&fieldName, &dataType, &isNullableString, &length); err != nil {
			e.logger.Error("failed to get fields", "error", err)
			continue
		}
		result = append(result, &facetsv1beta1.Column{
			Name:       fieldName,
			DataType:   dataType,
			IsNullable: isNullable(isNullableString),
			Length:     int64(length),
		})
	}
	return result, nil
}

// Convert nullable string to a boolean
func isNullable(value string) bool {
	return value == "YES"
}

// connection generates a connection string
func (e *Extractor) connection(cfg Config, database string) (db *sql.DB, err error) {
	connStr := fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=%s", e.username, e.password, e.host, database, e.sslmode)
	return sql.Open("postgres", connStr)
}

// extractConnectionComponents extracts the components from the connection URL
func (e *Extractor) extractConnectionComponents(connectionURL string) (err error) {
	connectionStr, err := url.Parse(connectionURL)
	if err != nil {
		err = errors.Wrap(err, "failed to parse connection url")
		return
	}
	e.host = connectionStr.Host
	e.userDB = connectionStr.Path[1:]
	e.username = connectionStr.User.Username()
	e.password, _ = connectionStr.User.Password()
	e.sslmode = connectionStr.Query().Get("sslmode")

	return
}

// Exclude checks if the database is in the ignored databases
func exclude(names []string, database string) bool {
	for _, b := range names {
		if b == database {
			return true
		}
	}
	return false
}

// Register the extractor to catalog
func init() {
	if err := registry.Extractors.Register("postgres", func() plugins.Extractor {
		return &Extractor{
			logger: plugins.GetLog(),
		}
	}); err != nil {
		panic(err)
	}
}
