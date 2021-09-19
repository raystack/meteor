package cassandra

import (
	"context"
	_ "embed" // used to print the embedded assets
	"fmt"

	"github.com/gocql/gocql"
	"github.com/odpf/meteor/models"
	_ "github.com/odpf/meteor/models"
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

// defaultKeyspaceList is the list of keyspaces to be excluded
var defaultKeyspaceList = []string{
	"system",
	"system_schema",
	"system_auth",
	"system_distributed",
	"system_traces",
}

// Config hold the set of configuration for the cassandra extractor
type Config struct {
	UserID   string `mapstructure:"user_id" validate:"required"`
	Password string `mapstructure:"password" validate:"required"`
	Host     string `mapstructure:"host" validate:"required"`
	Port     int    `mapstructure:"port" validate:"required"`
}

var sampleConfig = `
 user_id: admin
 password: "1234"
 host: localhost
 port: 9042
`

// Extractor manages the extraction of data from cassandra
type Extractor struct {
	excludedKeyspaces map[string]bool
	logger            log.Logger
	config            Config
	session           *gocql.Session
	emit              plugins.Emit
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
		Description:  "Table metadata from cassandra server.",
		SampleConfig: sampleConfig,
		Summary:      summary,
		Tags:         []string{"oss", "extractor"},
	}
}

// Validate checks if the extractor is configured correctly
func (e *Extractor) Validate(configMap map[string]interface{}) (err error) {
	return utils.BuildConfig(configMap, &Config{})
}

// Init initializes the extractor
func (e *Extractor) Init(ctx context.Context, configMap map[string]interface{}) (err error) {
	//build config
	if err := utils.BuildConfig(configMap, &e.config); err != nil {
		return plugins.InvalidConfigError{}
	}

	// build excluded database list
	e.buildExcludedKeyspaces()

	// connect to cassandra
	cluster := gocql.NewCluster(e.config.Host)
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: e.config.UserID,
		Password: e.config.Password,
	}
	cluster.Consistency = gocql.Quorum
	cluster.ProtoVersion = 4
	cluster.Port = e.config.Port
	e.session, err = cluster.CreateSession()
	if err != nil {
		fmt.Printf("show the error %s\n", err)
		return err
	}

	return
}

//Extract checks if the extractor is configured and
// if the connection to the DB is successful
// and then starts the extraction process
func (e *Extractor) Extract(ctx context.Context, emit plugins.Emit) (err error) {
	defer e.session.Close()
	e.emit = emit

	scanner := e.session.
		Query("SELECT keyspace_name FROM system_schema.keyspaces;").
		Iter().
		Scanner()

	for scanner.Next() {
		var keyspace string
		if err = scanner.Scan(&keyspace); err != nil {
			return err
		}

		// skip if database is default
		if e.isExcludedKeyspace(keyspace) {
			continue
		}
		if err = e.extractTables(keyspace); err != nil {
			return err
		}
	}

	return
}

// extractTables extract tables from a given keyspace
func (e *Extractor) extractTables(keyspace string) (err error) {
	scanner := e.session.
		Query(`SELECT table_name FROM system_schema.tables WHERE keyspace_name = ?`, keyspace).
		Iter().
		Scanner()

	for scanner.Next() {
		var tableName string
		if err = scanner.Scan(&tableName); err != nil {
			return err
		}
		if err = e.processTable(keyspace, tableName); err != nil {
			return err
		}
	}

	return
}

// processTable build and push table to out channel
func (e *Extractor) processTable(keyspace string, tableName string) (err error) {
	var columns []*facets.Column
	columns, err = e.extractColumns(keyspace, tableName)
	if err != nil {
		return
	}

	// push table to channel
	e.emit(models.NewRecord(&assets.Table{
		Resource: &common.Resource{
			Urn:  fmt.Sprintf("%s.%s", keyspace, tableName),
			Name: tableName,
		},
		Schema: &facets.Columns{
			Columns: columns,
		},
	}))

	return
}

// extractColumns extract columns from a given table
func (e *Extractor) extractColumns(keyspace string, tableName string) (columns []*facets.Column, err error) {
	query := `SELECT column_name, type 
              FROM system_schema.columns 
              WHERE keyspace_name = ?
              AND table_name = ?`
	scanner := e.session.
		Query(query, keyspace, tableName).
		Iter().
		Scanner()

	for scanner.Next() {
		var fieldName, dataType string
		err = scanner.Scan(&fieldName, &dataType)
		if err != nil {
			return
		}

		columns = append(columns, &facets.Column{
			Name:     fieldName,
			DataType: dataType,
		})
	}

	return
}

// buildExcludedKeyspaces builds the list of excluded keyspaces
func (e *Extractor) buildExcludedKeyspaces() {
	excludedMap := make(map[string]bool)
	for _, db := range defaultKeyspaceList {
		excludedMap[db] = true
	}
	e.excludedKeyspaces = excludedMap
}

// isExcludedKeyspace checks if the given db is in the list of excluded keyspaces
func (e *Extractor) isExcludedKeyspace(keyspace string) bool {
	_, ok := e.excludedKeyspaces[keyspace]
	return ok
}

// init register the extractor to the catalog
func init() {
	if err := registry.Extractors.Register("cassandra", func() plugins.Extractor {
		return New(plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}
