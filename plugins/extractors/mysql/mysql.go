package mysql

import (
	"context"
	"database/sql"
	_ "embed" // used to print the embedded assets
	"fmt"

	"github.com/pkg/errors"

	_ "github.com/go-sql-driver/mysql"
	"github.com/odpf/meteor/models"

	commonv1beta1 "github.com/odpf/meteor/models/odpf/assets/common/v1beta1"
	facetsv1beta1 "github.com/odpf/meteor/models/odpf/assets/facets/v1beta1"
	assetsv1beta1 "github.com/odpf/meteor/models/odpf/assets/v1beta1"

	extutils "github.com/odpf/meteor/plugins/extractors/utils"

	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/registry"
	"github.com/odpf/meteor/utils"
	"github.com/odpf/salt/log"
)

//go:embed README.md
var summary string

var defaultDBList = []string{
	"information_schema",
	"mysql",
	"performance_schema",
	"sys",
}

var sampleConfig = `connection_url: "admin:pass123@tcp(localhost:3306)/"`

type Extractor struct {
	extutils.BaseExtractor
	logger log.Logger
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
		Description:  "Table metadata from MySQL server.",
		SampleConfig: sampleConfig,
		Summary:      summary,
		Tags:         []string{"oss", "extractor"},
	}
}

// Init initializes the extractor
func (e *Extractor) Init(ctx context.Context, configMap map[string]interface{}) (err error) {
	if err = utils.BuildConfig(configMap, &e.Config); err != nil {
		return plugins.InvalidConfigError{}
	}

	// build excluded database list
	e.ExcludedDbs = extutils.BuildExcludedDBs(defaultDBList)

	// create client
	if e.DB, err = sql.Open("mysql", e.Config.ConnectionURL); err != nil {
		return errors.Wrap(err, "failed to create client")
	}

	return
}

// Extract extracts the data from the MySQL server
// and collected through the emitter
func (e *Extractor) Extract(ctx context.Context, emit plugins.Emit) (err error) {
	defer e.DB.Close()
	e.Emit = emit

	res, err := e.DB.Query("SHOW DATABASES;")
	if err != nil {
		return errors.Wrap(err, "failed to fetch databases")
	}
	for res.Next() {
		var database string
		if err := res.Scan(&database); err != nil {
			e.logger.Error("failed to connect, skipping database", "error", err)
			continue
		}

		if err := e.extractTables(database); err != nil {
			e.logger.Error("failed to get tables, skipping database", "error", err)
			continue
		}
	}

	return
}

// Extract tables from a given database
func (e *Extractor) extractTables(database string) (err error) {
	// skip if database is default
	if extutils.IsExcludedDB(database, e.ExcludedDbs) {
		return
	}

	// extract tables
	_, err = e.DB.Exec(fmt.Sprintf("USE %s;", database))
	if err != nil {
		return errors.Wrapf(err, "failed to iterate over %s", database)
	}
	rows, err := e.DB.Query("SHOW TABLES;")
	if err != nil {
		return errors.Wrapf(err, "failed to show tables of %s", database)
	}

	// process each rows
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return errors.Wrapf(err, "failed to iterate over %s", tableName)
		}

		if err := e.processTable(database, tableName); err != nil {
			return errors.Wrap(err, "failed to process table")
		}
	}

	return
}

// processTable builds and push table to emitter
func (e *Extractor) processTable(database string, tableName string) (err error) {
	var columns []*facetsv1beta1.Column
	if columns, err = e.extractColumns(tableName); err != nil {
		return errors.Wrap(err, "failed to extract columns")
	}

	// push table to channel
	e.Emit(models.NewRecord(&assetsv1beta1.Table{
		Resource: &commonv1beta1.Resource{
			Urn:  fmt.Sprintf("%s.%s", database, tableName),
			Name: tableName,
		},
		Schema: &facetsv1beta1.Columns{
			Columns: columns,
		},
	}))

	return
}

// Extract columns from a given table
func (e *Extractor) extractColumns(tableName string) (columns []*facetsv1beta1.Column, err error) {
	query := `SELECT COLUMN_NAME,column_comment,DATA_TYPE,
				IS_NULLABLE,IFNULL(CHARACTER_MAXIMUM_LENGTH,0)
				FROM information_schema.columns
				WHERE table_name = ?
				ORDER BY COLUMN_NAME ASC`
	rows, err := e.DB.Query(query, tableName)
	if err != nil {
		err = errors.Wrap(err, "failed to execute query")
		return
	}

	for rows.Next() {
		var fieldName, fieldDesc, dataType, isNullableString string
		var length int
		if err = rows.Scan(&fieldName, &fieldDesc, &dataType, &isNullableString, &length); err != nil {
			e.logger.Error("failed to get fields", "error", err)
			continue
		}

		columns = append(columns, &facetsv1beta1.Column{
			Name:        fieldName,
			DataType:    dataType,
			Description: fieldDesc,
			IsNullable:  extutils.IsNullable(isNullableString),
			Length:      int64(length),
		})
	}

	return
}

// init register the extractor to the catalog
func init() {
	if err := registry.Extractors.Register("mysql", func() plugins.Extractor {
		return New(plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}
