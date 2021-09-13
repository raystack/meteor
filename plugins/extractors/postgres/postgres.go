package postgres

import (
	"context"
	"database/sql"
	_ "embed" // // used to print the embedded assets
	"fmt"
	"strings"

	_ "github.com/lib/pq" // used to register the postgres driver
	"github.com/odpf/meteor/models"
	"github.com/odpf/meteor/models/odpf/assets"
	"github.com/odpf/meteor/models/odpf/assets/common"
	"github.com/odpf/meteor/models/odpf/assets/facets"
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
	UserID   string `mapstructure:"user_id" validate:"required"`
	Password string `mapstructure:"password" validate:"required"`
	Host     string `mapstructure:"host" validate:"required"`
	Database string `mapstructure:"database" default:"postgres"`
	Exclude  string `mapstructure:"exclude"`
}

var sampleConfig = `
 host: localhost:1433
 user_id: admin
 password: "1234"
 database: database_name
 exclude: postgres`

// Extractor manages the extraction of data from the extractor
type Extractor struct {
	logger log.Logger
	config Config
	db     *sql.DB
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
		Tags:         []string{"oss,extractor"},
	}
}

// Validate validates the configuration of the extractor
func (e *Extractor) Validate(configMap map[string]interface{}) (err error) {
	return utils.BuildConfig(configMap, &Config{})
}

func (e *Extractor) Init(ctx context.Context, config map[string]interface{}) (err error) {
	// Build and validate config received from receipe
	if err := utils.BuildConfig(config, &e.config); err != nil {
		return plugins.InvalidConfigError{}
	}

	// Create database connection
	e.db, err = connection(e.config, e.config.Database)
	if err != nil {
		return err
	}

	return
}

// Extract collects metdata from the source. Metadata is collected through the out channel
func (e *Extractor) Extract(ctx context.Context, emitter plugins.Emitter) (err error) {
	defer e.db.Close()

	// Get list of databases
	dbs, err := e.getDatabases()
	if err != nil {
		return err
	}

	// Iterate through all tables and databases
	for _, database := range dbs {
		// Open a new connection to the given database to collect
		// tables information without this default database
		// information will be returned
		db, err := connection(e.config, database)
		if err != nil {
			e.logger.Error("failed to connect, skipping database", err)
			continue
		}
		tables, err := e.getTables(db, database)
		if err != nil {
			e.logger.Error("failed to get tables, skipping database ", err)
			continue
		}

		for _, table := range tables {
			result, err := e.getTableMetadata(db, database, table)
			if err != nil {
				e.logger.Error("failed to get table metadata, skipping table", err)
				continue
			}
			// Publish metadata to channel
			emitter.Emit(models.NewRecord(result))
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
func (e *Extractor) getTableMetadata(db *sql.DB, dbName string, tableName string) (result *assets.Table, err error) {
	result = &assets.Table{
		Resource: &common.Resource{
			Urn:  fmt.Sprintf("%s.%s", dbName, tableName),
			Name: tableName,
		},
	}

	var columns []*facets.Column
	columns, err = e.getColumnMetadata(db, dbName, tableName)
	if err != nil {
		return result, nil
	}
	result.Schema = &facets.Columns{
		Columns: columns,
	}

	return result, nil
}

// Prepares the list of columns and the attached metadata
func (e *Extractor) getColumnMetadata(db *sql.DB, dbName string, tableName string) (result []*facets.Column, err error) {
	sqlStr := `SELECT COLUMN_NAME,DATA_TYPE,
				IS_NULLABLE,coalesce(CHARACTER_MAXIMUM_LENGTH,0)
				FROM information_schema.columns
				WHERE TABLE_NAME = '%s' ORDER BY COLUMN_NAME ASC;`
	rows, err := db.Query(fmt.Sprintf(sqlStr, tableName))
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var fieldName, dataType, isNullableString string
		var length int
		err = rows.Scan(&fieldName, &dataType, &isNullableString, &length)
		if err != nil {
			e.logger.Error("failed to scan row, skipping", err)
			continue
		}
		result = append(result, &facets.Column{
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

// Generate a connecion string
func connection(cfg Config, database string) (db *sql.DB, err error) {
	connStr := fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable", cfg.UserID, cfg.Password, cfg.Host, database)
	return sql.Open("postgres", connStr)
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
