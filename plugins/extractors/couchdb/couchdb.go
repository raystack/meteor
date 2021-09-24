package couchdb

import (
	"context"
	_ "embed" // used to print the embedded assets
	"fmt"

	_ "github.com/go-kivik/couchdb"
	"github.com/go-kivik/kivik"
	"github.com/odpf/meteor/models/odpf/assets/facets"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/registry"
	"github.com/odpf/meteor/utils"
	"github.com/odpf/salt/log"
)

//go:embed README.md
var summary string

var defaultDBList = []string{
	"_global_changes",
	"_replicator",
	"_users",
}

// Config hold the set of configuration for the extractor
type Config struct {
	UserID   string `mapstructure:"user_id" validate:"required"`
	Password string `mapstructure:"password" validate:"required"`
	Host     string `mapstructure:"host" validate:"required"`
}

var sampleConfig = `
 host: localhost:5984
 user_id: admin
 password: couchdb`

// Extractor manages the extraction of data from MySQL
type Extractor struct {
	client      *kivik.Client
	excludedDbs map[string]bool
	logger      log.Logger
	config      Config
	emit        plugins.Emit
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
		Description:  "Table metadata from CouchDB server,",
		SampleConfig: sampleConfig,
		Summary:      summary,
		Tags:         []string{"oss", "extractor"},
	}
}

// Validate validates the configuration of the extractor
func (e *Extractor) Validate(configMap map[string]interface{}) (err error) {
	return utils.BuildConfig(configMap, &Config{})
}

// Initialise the Extractor with Configurations
func (e *Extractor) Init(ctx context.Context, configMap map[string]interface{}) (err error) {
	err = utils.BuildConfig(configMap, &e.config)
	if err != nil {
		return plugins.InvalidConfigError{}
	}

	// build excluded database list
	e.buildExcludedDBs()

	// create client
	e.client, err = kivik.New("couch", fmt.Sprintf("http://%s:%s@%s/", e.config.UserID, e.config.Password, e.config.Host))
	if err != nil {
		return
	}

	return
}

// Extract extracts the data from the MySQL server
// and collected through the out channel
func (e *Extractor) Extract(ctx context.Context, emit plugins.Emit) (err error) {
	defer e.client.Close(context.TODO())
	e.emit = emit

	res, err := e.client.AllDBs(context.TODO())
	if err != nil {
		return
	}

	for _, dbName := range res {
		if err := e.extractTables(dbName); err != nil {
			return err
		}
	}
	return
}

// Extract tables from a given database
func (e *Extractor) extractTables(dbName string) (err error) {
	// skip if database is default
	if e.isExcludedDB(dbName) {
		return
	}
	fmt.Println(dbName)
	db := e.client.DB(context.TODO(), dbName)

	// extract documents
	rows, err := db.AllDocs(context.TODO())
	if err != nil {
		return
	}

	// process each rows
	for rows.Next() {
		var row_rev map[string]interface{}
		if err := rows.ScanValue(&row_rev); err != nil {
			return err
		}
		fmt.Println(row_rev["rev"])
		// if err := e.processTable(dbName, tableName); err != nil {
		// return err
		// }
	}

	return
}

// Build and push table to out channel
func (e *Extractor) processTable(dbName string, docName string) (err error) {
	//var columns []*facets.Column
	// columns, err = e.extractColumns(docName)
	// if err != nil {
	// return
	// }
	fmt.Println(dbName, docName)
	// push table to channel
	// e.emit(models.NewRecord(&assets.Table{
	// Resource: &common.Resource{
	// Urn:  fmt.Sprintf("%s.%s", dbName, docName),
	// Name: docName,
	// },
	// Schema: &facets.Columns{
	// Columns: columns,
	// },
	// }))

	return
}

// Extract columns from a given table
func (e *Extractor) extractColumns(tableName string) (columns []*facets.Column, err error) {
	// query := `SELECT COLUMN_NAME,column_comment,DATA_TYPE,
	// IS_NULLABLE,IFNULL(CHARACTER_MAXIMUM_LENGTH,0)
	// FROM information_schema.columns
	// WHERE table_name = ?
	// ORDER BY COLUMN_NAME ASC`
	// rows, err := e.db.Query(query, tableName)
	// if err != nil {
	// return
	// }

	// for rows.Next() {
	// var fieldName, fieldDesc, dataType, isNullableString string
	// var length int
	// err = rows.Scan(&fieldName, &fieldDesc, &dataType, &isNullableString, &length)
	// if err != nil {
	// return
	// }

	// columns = append(columns, &facets.Column{
	// Name:        fieldName,
	// DataType:    dataType,
	// Description: fieldDesc,
	// IsNullable:  e.isNullable(isNullableString),
	// Length:      int64(length),
	// })
	// }

	return
}

func (e *Extractor) buildExcludedDBs() {
	excludedMap := make(map[string]bool)
	for _, db := range defaultDBList {
		excludedMap[db] = true
	}

	e.excludedDbs = excludedMap
}

func (e *Extractor) isExcludedDB(database string) bool {
	_, ok := e.excludedDbs[database]
	return ok
}

func (e *Extractor) isNullable(value string) bool {
	return value == "YES"
}

// Register the extractor to catalog
func init() {
	if err := registry.Extractors.Register("couchdb", func() plugins.Extractor {
		return New(plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}
