package redshift

import (
	"context"
	_ "embed" // used to print the embedded assets
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/redshiftdataapiservice"
	"github.com/aws/aws-sdk-go/service/redshiftdataapiservice/redshiftdataapiserviceiface"
	"github.com/odpf/meteor/models"
	commonv1beta1 "github.com/odpf/meteor/models/odpf/assets/common/v1beta1"
	facetsv1beta1 "github.com/odpf/meteor/models/odpf/assets/facets/v1beta1"
	assetsv1beta1 "github.com/odpf/meteor/models/odpf/assets/v1beta1"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/registry"
	"github.com/odpf/meteor/utils"
	"github.com/odpf/salt/log"
	"strings"
)

//The URL for the Amazon Redshift Data API is: https://redshift-data.[aws-region].amazonaws.com
//AWS IAM User
// 1.Access Key ID
// 2. Secret Access Key ID
// 3. Attached AmazonRedshiftDataFullAccess permission
//An API client
//An available Amazon Redshift cluster in your aws-region

// 2 ways to authenticate
// https://docs.aws.amazon.com/redshift/latest/mgmt/data-api.html#data-api-calling-considerations-authentication
// 1. AwS IAM Temporary Credentials
// 2. AWS Secrets Manager Secret
//* Secrets Manager - when connecting to a cluster, specify the Amazon Resource
//Name (ARN) of the secret, the database name, and the cluster identifier
//that matches the cluster in the secret. When connecting to a serverless
//endpoint, specify the Amazon Resource Name (ARN) of the secret and the
//database name.
//
//* Temporary credentials - when connecting to a cluster, specify the cluster
//identifier, the database name, and the database user name. Also, permission
//to call the redshift:GetClusterCredentials operation is required. When
//connecting to a serverless endpoint, specify the database name.

// Permission to call GetClusterCredentials :
// https://docs.aws.amazon.com/redshift/latest/mgmt/generating-iam-credentials-role-permissions.html

//go:embed README.md
var summary string

var defaultExcludes = []string{"information_schema", "pg_catalog", "pg_internal", "public"}

// Config holds the set of configuration for the metabase extractor
type Config struct {
	ClusterID string `mapstructure:"cluster_id"`
	DbName    string `mapstructure:"db_name"`
	DbUser    string `mapstructure:"db_user"`
	AwsRegion string `mapstructure:"aws_region"`
	Exclude   string `mapstructure:"exclude"`

	//IamRole         string `json:"iam_role"`
	//AccessKeyID     string `json:"access_key_id"`
	//SecretAccessKey string `json:"secret_access_key"`
}

var sampleConfig = `
cluster_id: 1234567
db_name: testDB
db_user: testUser
aws_region: us-east-1
exclude: secondaryDB
`

// Option provides extension abstraction to Extractor constructor
type Option func(*Extractor)

// WithClient assign custom client to the Extractor constructor
func WithClient(redshiftClient redshiftdataapiserviceiface.RedshiftDataAPIServiceAPI) Option {
	return func(e *Extractor) {
		e.apiClient = redshiftClient
	}
}

type Extractor struct {
	config    Config
	logger    log.Logger
	apiClient redshiftdataapiserviceiface.RedshiftDataAPIServiceAPI
}

// New returns a pointer to an initialized Extractor Object
func New(logger log.Logger, opts ...Option) *Extractor {
	e := &Extractor{
		logger: logger,
	}
	for _, opt := range opts {
		opt(e)
	}

	return e
}

// Info returns the brief information about the extractor
func (e *Extractor) Info() plugins.Info {
	return plugins.Info{
		Description:  "Table metadata from Redshift server.",
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
func (e *Extractor) Init(_ context.Context, config map[string]interface{}) (err error) {
	// Build and validate config received from recipe
	if err = utils.BuildConfig(config, &e.config); err != nil {
		return plugins.InvalidConfigError{}
	}

	if e.apiClient != nil {
		// Create session
		var sess = session.Must(session.NewSession())

		// Initialize the redshift client
		e.apiClient = redshiftdataapiservice.New(sess, aws.NewConfig().WithRegion(e.config.AwsRegion))
	}

	return
}

// Extract collects metadata from the source. Metadata is collected through the emitter
func (e *Extractor) Extract(_ context.Context, emit plugins.Emit) error {
	// The Data API uses either credentials stored in AWS Secrets Manager or temporary database credentials.
	// auth through IAM -> get key -> access list db -> iterate through each db to list tables
	excludeList := append(defaultExcludes, strings.Split(e.config.Exclude, ",")...)

	listDB, err := e.GetDBList()
	if err != nil {
		return err
	}
	for _, database := range listDB {
		if exclude(excludeList, database) {
			continue
		}

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
func (e *Extractor) GetDBList() (list []string, err error) {
	listDbOutput, err := e.apiClient.ListDatabases(&redshiftdataapiservice.ListDatabasesInput{
		ClusterIdentifier: aws.String(e.config.ClusterID),
		Database:          aws.String(e.config.DbName),
		DbUser:            aws.String(e.config.DbUser),
		MaxResults:        nil,
		NextToken:         nil,
		SecretArn:         nil,
	})
	if err != nil {
		return nil, err
	}

	for _, db := range listDbOutput.Databases {
		list = append(list, aws.StringValue(db))
	}

	return list, nil
}

// GetTables return the list of table
func (e *Extractor) GetTables(dbName string) (list []string, err error) {
	listTbOutput, err := e.apiClient.ListTables(&redshiftdataapiservice.ListTablesInput{
		ClusterIdentifier: aws.String(e.config.ClusterID),
		ConnectedDatabase: aws.String(dbName),
		Database:          aws.String(e.config.DbName),
		DbUser:            aws.String(e.config.DbUser),
		MaxResults:        nil,
		NextToken:         nil,
		SchemaPattern:     aws.String("information_schema"),
		SecretArn:         nil, // required when authenticating through secret manager
		TablePattern:      nil,
	})
	if err != nil {
		return nil, err
	}

	for _, table := range listTbOutput.Tables {
		list = append(list, aws.StringValue(table.Name))
	}

	return list, nil
}

// getTableMetadata prepares the list of tables and the attached metadata
func (e *Extractor) getTableMetadata(dbName string, tableName string) (result *assetsv1beta1.Table, err error) {
	var columns []*facetsv1beta1.Column
	colMetadata, err := e.GetColumn(dbName, tableName)
	if err != nil {
		return result, nil
	}
	columns, err = e.GetColumnMetadata(colMetadata)
	if err != nil {
		return result, nil
	}

	result = &assetsv1beta1.Table{
		Resource: &commonv1beta1.Resource{
			Urn:     models.TableURN("redshift", e.config.AwsRegion, dbName, tableName),
			Name:    tableName,
			Service: "redshift",
		},
		Schema: &facetsv1beta1.Columns{
			Columns: columns,
		},
	}

	return
}

// GetColumn returns the column metadata of particular table in a database
func (e *Extractor) GetColumn(dbName string, tableName string) (result []*redshiftdataapiservice.ColumnMetadata, err error) {
	descTable, err := e.apiClient.DescribeTable(&redshiftdataapiservice.DescribeTableInput{
		ClusterIdentifier: aws.String(e.config.ClusterID),
		ConnectedDatabase: aws.String(e.config.DbName),
		Database:          aws.String(dbName),
		DbUser:            aws.String(e.config.DbName),
		MaxResults:        nil,
		NextToken:         nil,
		Schema:            aws.String("information_schema"),
		SecretArn:         nil,
		Table:             aws.String(tableName),
	})
	if err != nil {
		return nil, err
	}

	return descTable.ColumnList, nil
}

// GetColumnMetadata returns the
func (e *Extractor) GetColumnMetadata(columnMetadata []*redshiftdataapiservice.ColumnMetadata) (result []*facetsv1beta1.Column, err error) {
	var tempresults []*facetsv1beta1.Column
	for _, column := range columnMetadata {
		var tempresult facetsv1beta1.Column
		tempresult.Name = aws.StringValue(column.Name)
		tempresult.Description = aws.StringValue(column.Label)
		tempresult.DataType = aws.StringValue(column.TypeName)
		//tempresult.IsNullable
		//tempresult.Length = column.Length
		//tempresult.Profile
		//tempresult.Properties
		tempresults = append(tempresults, &tempresult)
	}
	return tempresults, nil
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
	if err := registry.Extractors.Register("redshift", func() plugins.Extractor {
		return New(plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}

// IMP Links :
// https://docs.aws.amazon.com/redshift/latest/mgmt/data-api.html
// https://aws.amazon.com/blogs/big-data/using-the-amazon-redshift-data-api-to-interact-with-amazon-redshift-clusters/
