package oracle

import (
	"context"
	"database/sql"
	_ "embed"
	"fmt"

	"github.com/pkg/errors"

	_ "github.com/godror/godror" //used to register the oracle driver
	"github.com/odpf/meteor/models"
	commonv1beta1 "github.com/odpf/meteor/models/odpf/assets/common/v1beta1"
	facetsv1beta1 "github.com/odpf/meteor/models/odpf/assets/facets/v1beta1"
	assetsv1beta1 "github.com/odpf/meteor/models/odpf/assets/v1beta1"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/registry"
	"github.com/odpf/meteor/utils"
	"github.com/odpf/salt/log"
)

var summary string

var defaults = []string{}

// Config holds the set of configuration options for the extractor
type Config struct {
	UserID   string `mapstructure:"user_id" validate:"required"`
	Password string `mapstructure:"password" validate:"required"`
	Host     string `mapstructure:"host" validate:"required"`
	Database string `mapstructure:"database" validate:"required"`
}

var sampleConfig = `
host: localhost:1521
user_id: system
password: "1234"
database: database_name`

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
		Description:  "Table metadata Oracle SQL Database.",
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

	// Create database connection
	e.db, err = connection(e.config, e.config.Database)
	if err != nil {
		return errors.Wrap(err, "failed to create connection")
	}

	return
}

// Extract collects metadata from the source. Metadata is collected through the emitter
func (e *Extractor) Extract(ctx context.Context, emit plugins.Emit) (err error) {
	defer e.db.Close()

	var dbs = []string{e.config.Database}

	// Iterate through all tables and databases
	for _, database := range dbs {
		// Open a new connection to the given database to collect tables information
		db, err := connection(e.config, database)
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

func (e *Extractor) getTables(db *sql.DB, dbName string) (list []string, err error) {
	sqlStr := `SELECT object_name 
 		FROM all_objects
		WHERE object_type = 'TABLE'
		AND upper(owner) = upper('%s')`

	rows, err := db.Query(fmt.Sprintf(sqlStr, e.config.UserID))
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

	// get table row count
	sqlStr := `select count(*) from %s`
	rows, err := db.Query(fmt.Sprintf(sqlStr, tableName))
	var rowCount int64
	for rows.Next() {
		if err = rows.Scan(&rowCount); err != nil {
			e.logger.Error("failed to get fields", "error", err)
			continue
		}
	}

	result = &assetsv1beta1.Table{
		Resource: &commonv1beta1.Resource{
			Urn:     fmt.Sprintf("%s.%s", dbName, tableName),
			Name:    tableName,
			Service: "Oracle",
		},
		Schema: &facetsv1beta1.Columns{
			Columns: columns,
		},
		Profile: &assetsv1beta1.TableProfile{
			TotalRows: rowCount,
		},
	}

	return
}

// Prepares the list of columns and the attached metadata
func (e *Extractor) getColumnMetadata(db *sql.DB, dbName string, tableName string) (result []*facetsv1beta1.Column, err error) {
	sqlStr := `select utc.column_name, utc.data_type, 
			decode(utc.char_used, 'C', utc.char_length, utc.data_length) as data_length,
			utc.nullable, nvl(ucc.comments, '') as col_comment
			from USER_TAB_COLUMNS utc
			INNER JOIN USER_COL_COMMENTS ucc ON
			utc.column_name = ucc.column_name AND
			utc.table_name = ucc.table_name
			WHERE utc.table_name ='%s'`

	rows, err := db.Query(fmt.Sprintf(sqlStr, tableName))
	if err != nil {
		err = errors.Wrap(err, "failed to fetch data from query")
		return
	}

	for rows.Next() {
		var fieldName, dataType, isNullableString, fieldDesc string
		var length int
		if err = rows.Scan(&fieldName, &dataType, &length, &isNullableString, &fieldDesc); err != nil {
			e.logger.Error("failed to get fields", "error", err)
			continue
		}

		result = append(result, &facetsv1beta1.Column{
			Name:        fieldName,
			DataType:    dataType,
			Description: fieldDesc,
			IsNullable:  isNullable(isNullableString),
			Length:      int64(length),
		})
	}
	return result, nil
}

// Convert nullable string to a boolean
func isNullable(value string) bool {
	return value == "Y"
}

// connection generates a connection string
func connection(cfg Config, database string) (db *sql.DB, err error) {
	connStr := fmt.Sprintf("%s/%s@%s/%s", cfg.UserID, cfg.Password, cfg.Host, database)
	return sql.Open("godror", connStr)
}

// Register the extractor to catalog
func init() {
	if err := registry.Extractors.Register("oracle", func() plugins.Extractor {
		return &Extractor{
			logger: plugins.GetLog(),
		}
	}); err != nil {
		panic(err)
	}
}
