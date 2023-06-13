package redshift

import (
	"context"
	_ "embed" // used to print the embedded assets
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/redshiftdataapiservice"
	"github.com/aws/aws-sdk-go/service/redshiftdataapiservice/redshiftdataapiserviceiface"
	"github.com/goto/meteor/models"
	v1beta2 "github.com/goto/meteor/models/gotocompany/assets/v1beta2"
	"github.com/goto/meteor/plugins"
	"github.com/goto/meteor/registry"
	"github.com/goto/salt/log"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"
)

//go:embed README.md
var summary string

// Config holds the set of configuration for the metabase extractor
type Config struct {
	ClusterID string `mapstructure:"cluster_id" validate:"required"`
	DBName    string `mapstructure:"db_name" validate:"required"`
	DBUser    string `mapstructure:"db_user" validate:"required"`
	AWSRegion string `mapstructure:"aws_region" validate:"required"`
	Exclude   string `mapstructure:"exclude"`
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
func (e *Extractor) getTableMetadata(dbName, tableName string) (*v1beta2.Asset, error) {
	colMetadata, err := e.GetColumn(dbName, tableName)
	if err != nil {
		return nil, err
	}

	columns, err := e.getColumnMetadata(colMetadata)
	if err != nil {
		return nil, err
	}

	data, err := anypb.New(&v1beta2.Table{
		Columns:    columns,
		Attributes: &structpb.Struct{}, // ensure attributes don't get overwritten if present
	})
	if err != nil {
		err = fmt.Errorf("create Any struct: %w", err)
		return nil, err
	}

	return &v1beta2.Asset{
		Urn:     models.NewURN("redshift", e.config.ClusterID, "table", fmt.Sprintf("%s.%s.%s", e.config.ClusterID, dbName, tableName)),
		Name:    tableName,
		Type:    "table",
		Service: "redshift",
		Data:    data,
	}, nil
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
func (*Extractor) getColumnMetadata(columnMetadata []*redshiftdataapiservice.ColumnMetadata) ([]*v1beta2.Column, error) {
	var cols []*v1beta2.Column
	for _, column := range columnMetadata {
		cols = append(cols, &v1beta2.Column{
			Name:        aws.StringValue(column.Name),
			Description: aws.StringValue(column.Label),
			DataType:    aws.StringValue(column.TypeName),
			IsNullable:  isNullable(aws.Int64Value(column.Nullable)),
			Length:      aws.Int64Value(column.Length),
		})
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
