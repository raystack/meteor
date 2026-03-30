package redshift

import (
	"context"
	_ "embed" // used to print the embedded assets
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/redshiftdataapiservice"
	"github.com/aws/aws-sdk-go/service/redshiftdataapiservice/redshiftdataapiserviceiface"
	"github.com/raystack/meteor/models"
	meteorv1beta1 "github.com/raystack/meteor/models/raystack/meteor/v1beta1"
	"github.com/raystack/meteor/plugins"
	"github.com/raystack/meteor/registry"
	log "github.com/raystack/salt/observability/logger"
)

//go:embed README.md
var summary string

// Config holds the set of configuration for the metabase extractor
type Config struct {
	ClusterID string `json:"cluster_id" yaml:"cluster_id" mapstructure:"cluster_id" validate:"required"`
	DBName    string `json:"db_name" yaml:"db_name" mapstructure:"db_name" validate:"required"`
	DBUser    string `json:"db_user" yaml:"db_user" mapstructure:"db_user" validate:"required"`
	AWSRegion string `json:"aws_region" yaml:"aws_region" mapstructure:"aws_region" validate:"required"`
	Exclude   string `json:"exclude" yaml:"exclude" mapstructure:"exclude"`
}

var sampleConfig = `
cluster_id: cluster_test
db_name: testDB
db_user: testUser
aws_region: us-east-1
exclude: secondaryDB
`

var info = plugins.Info{
	Description:  "Table metadata from Redshift server.",
	SampleConfig: sampleConfig,
	Summary:      summary,
	Tags:         []string{"oss", "extractor"},
}

// Option provides extension abstraction to Extractor constructor
type Option func(*Extractor)

// WithClient assign custom client to the Extractor constructor
func WithClient(redshiftClient redshiftdataapiserviceiface.RedshiftDataAPIServiceAPI) Option {
	return func(e *Extractor) {
		e.client = redshiftClient
	}
}

// Extractor manages the extraction of data
// from the redshift server
type Extractor struct {
	plugins.BaseExtractor
	config Config
	logger log.Logger
	client redshiftdataapiserviceiface.RedshiftDataAPIServiceAPI
}

// New returns a pointer to an initialized Extractor Object
func New(logger log.Logger, opts ...Option) *Extractor {
	e := &Extractor{
		logger: logger,
	}
	e.BaseExtractor = plugins.NewBaseExtractor(info, &e.config)
	e.ScopeNotRequired = true

	for _, opt := range opts {
		opt(e)
	}

	return e
}

// Init initializes the extractor
func (e *Extractor) Init(ctx context.Context, config plugins.Config) error {
	if err := e.BaseExtractor.Init(ctx, config); err != nil {
		return err
	}

	if e.client == nil {
		// Create session
		sess := session.Must(session.NewSession())

		// Initialize the redshift client
		e.client = redshiftdataapiservice.New(sess, aws.NewConfig().WithRegion(e.config.AWSRegion))
	}

	return nil
}

// Extract collects metadata from the source. Metadata is collected through the emitter
func (e *Extractor) Extract(_ context.Context, emit plugins.Emit) error {
	listDB, err := e.GetDBList()
	if err != nil {
		return err
	}

	for _, database := range listDB {
		tables, err := e.GetTables(database)
		if err != nil {
			e.logger.Error("failed to get tables, skipping database", "error", err)
			continue
		}

		for _, tableName := range tables {
			result, err := e.getTableMetadata(database, tableName)
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

// GetDBList returns the list of databases in a cluster
func (e *Extractor) GetDBList() ([]string, error) {
	res, err := e.client.ListDatabases(&redshiftdataapiservice.ListDatabasesInput{
		ClusterIdentifier: aws.String(e.config.ClusterID),
		Database:          aws.String(e.config.DBName),
		DbUser:            aws.String(e.config.DBUser),
		MaxResults:        nil,
		NextToken:         nil,
		SecretArn:         nil,
	})
	if err != nil {
		return nil, err
	}

	var dbs []string
	for _, db := range res.Databases {
		dbs = append(dbs, aws.StringValue(db))
	}

	return dbs, nil
}

// GetTables return the list of tables name
func (e *Extractor) GetTables(dbName string) ([]string, error) {
	res, err := e.client.ListTables(&redshiftdataapiservice.ListTablesInput{
		ClusterIdentifier: aws.String(e.config.ClusterID),
		ConnectedDatabase: aws.String(dbName),
		Database:          aws.String(e.config.DBName),
		DbUser:            aws.String(e.config.DBUser),
		SchemaPattern:     nil,
		MaxResults:        nil,
		NextToken:         nil,
		SecretArn:         nil, // required when authenticating through secret manager
		TablePattern:      nil,
	})
	if err != nil {
		return nil, err
	}

	var tbls []string
	for _, table := range res.Tables {
		tbls = append(tbls, aws.StringValue(table.Name))
	}

	return tbls, nil
}

// getTableMetadata prepares the list of tables and the attached metadata
func (e *Extractor) getTableMetadata(dbName, tableName string) (*meteorv1beta1.Entity, error) {
	colMetadata, err := e.GetColumn(dbName, tableName)
	if err != nil {
		return nil, err
	}

	columns, err := e.getColumnMetadata(colMetadata)
	if err != nil {
		return nil, err
	}

	return models.NewEntity(
		models.NewURN("redshift", e.config.ClusterID, "table", fmt.Sprintf("%s.%s.%s", e.config.ClusterID, dbName, tableName)),
		"table", tableName, "redshift",
		map[string]any{"columns": columns},
	), nil
}

// GetColumn returns the column metadata of particular table in a database
func (e *Extractor) GetColumn(dbName, tableName string) ([]*redshiftdataapiservice.ColumnMetadata, error) {
	descTable, err := e.client.DescribeTable(&redshiftdataapiservice.DescribeTableInput{
		ClusterIdentifier: aws.String(e.config.ClusterID),
		ConnectedDatabase: aws.String(e.config.DBName),
		Database:          aws.String(dbName),
		DbUser:            aws.String(e.config.DBName),
		Table:             aws.String(tableName),
		Schema:            nil,
		MaxResults:        nil,
		NextToken:         nil,
		SecretArn:         nil,
	})
	if err != nil {
		return nil, err
	}

	return descTable.ColumnList, nil
}

// getColumnMetadata prepares the list of columns and the attached metadata
func (*Extractor) getColumnMetadata(columnMetadata []*redshiftdataapiservice.ColumnMetadata) ([]any, error) {
	var cols []any
	for _, column := range columnMetadata {
		col := map[string]any{
			"name":        aws.StringValue(column.Name),
			"data_type":   aws.StringValue(column.TypeName),
			"is_nullable": isNullable(aws.Int64Value(column.Nullable)),
		}
		if desc := aws.StringValue(column.Label); desc != "" {
			col["description"] = desc
		}
		if length := aws.Int64Value(column.Length); length != 0 {
			col["length"] = length
		}
		cols = append(cols, col)
	}
	return cols, nil
}

// Convert nullable int to a boolean
func isNullable(value int64) bool {
	return value == 1
}

// Register the extractor to catalog
func init() {
	if err := registry.Extractors.Register("redshift", func() plugins.Extractor {
		return New(plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}
