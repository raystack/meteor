package redshift

import (
	"bytes"
	"database/sql"
	_ "embed" // used to print the embedded assets
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/redshift/redshiftiface"
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
	"github.com/pkg/errors"
	"io/ioutil"
	"net/http"
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

//go:embed README.md
var summary string

var defaultExcludes = []string{"information_schema", "pg_catalog", "pg_internal", "public"}

// Config holds the set of configuration for the metabase extractor
type Config struct {
	ClusterID       string `json:"cluster_id"`
	DbName          string `json:"db_name"`
	DbUser          string `json:"db_user"`
	IamRole         string `json:"iam_role"`
	AwsRegion       string `json:"aws_region"`
	AccessKeyID     string `json:"access_key_id"`
	SecretAccessKey string `json:"secret_access_key"`
	Exclude         string `mapstructure:"exclude"`
}

var sampleConfig = ``

type Extractor struct {
	config   Config
	logger   log.Logger
	rsClient redshiftiface.RedshiftAPI
	//apiClient redshiftdata.Client
	apiClient redshiftdataapiserviceiface.RedshiftDataAPIServiceAPI
	client    *http.Client
	sqlClient *sql.DB
}

// New returns a pointer to an initialized Extractor Object
func New(client redshiftdataapiserviceiface.RedshiftDataAPIServiceAPI, rsClient redshiftiface.RedshiftAPI, logger log.Logger) *Extractor {
	return &Extractor{
		rsClient:  rsClient,
		apiClient: client,
		logger:    logger,
	}
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

func (e *Extractor) Init(config map[string]interface{}) error {
	// Create session
	var sess = session.Must(session.NewSession())

	//e.rsClient = redshift.New(sess)
	//e.rsClient.GetClusterCredentials()

	// Initialize the redshift client
	e.apiClient = redshiftdataapiservice.New(sess)

	// Build and validate config received from recipe
	if err := utils.BuildConfig(config, &e.config); err != nil {
		return plugins.InvalidConfigError{}
	}
}

func (e *Extractor) Extract(emit plugins.Emit) error {

	// The Data API uses either credentials stored in AWS Secrets Manager or temporary database credentials.
	// auth through IAM -> get key -> access list db -> iterate through each db to list tables

	//output, err := e.apiClient.ListDatabases(&redshiftdataapiservice.ListDatabasesInput{
	//	ClusterIdentifier: nil,
	//	Database:          nil,
	//	DbUser:            nil,
	//	MaxResults:        1,
	//	NextToken:         nil,
	//	SecretArn:         nil,
	//})
	//if err != nil {
	//	return err
	//}
	//listDB := output.Databases
	//
	//for _, db := range listDB {
	//	// iterate through each db to list tables
	//}

	excludeList := append(defaultExcludes, strings.Split(e.config.Exclude, ",")...)

	listDB, err := e.getDatabaseList()
	if err != nil {
		return err
	}
	for _, database := range listDB {
		if exclude(excludeList, aws.StringValue(database)) {
			continue
		}

		tables, err := e.listTables(aws.StringValue(database))
		//tables, err := e.getTables(db, database)
		if err != nil {
			e.logger.Error("failed to get tables, skipping database", "error", err)
			continue
		}

		for _, table := range tables {
			result, err := e.getTableMetadata(aws.StringValue(database), table.Name)
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

func (e *Extractor) getDatabaseList() (listDB []*string, err error) {
	payload := map[string]interface{}{
		"ClusterIdentifier": e.config.ClusterID,
		"Database":          e.config.DbName,
		"DbUser":            e.config.DbUser,
		"MaxResults":        1,
		"NextToken":         "",
	}
	type responseToken struct {
		ListDB []*string `json:"Databases"`
	}
	var data responseToken
	if err = e.makeRequest("POST", fmt.Sprintf("https://redshift-data.%s.amazonaws.com", e.config.AwsRegion), payload, &data); err != nil {
		return nil, errors.Wrap(err, "failed to fetch data")
	}
	return data.ListDB, nil
}

func (e *Extractor) listTables(dbList string) (listTables []Table, err error) {
	payload := map[string]interface{}{
		"ClusterIdentifier": e.config.ClusterID,
		"ConnectedDatabase": e.config.DbName,
		"Database":          dbList,
		"DbUser":            e.config.DbUser,
		"MaxResults":        nil,
		"NextToken":         "",
		"SchemaPattern":     "information_schema",
		"SecretArn":         nil, // required when authenticating through secret manager
		"TablePattern":      nil,
	}
	type responseTable struct {
		//NextToken string  `json:"NextToken"`
		Tables []Table `json:"Tables"`
	}
	var data responseTable
	if err = e.makeRequest("POST", fmt.Sprintf("https://redshift-data.%s.amazonaws.com", e.config.AwsRegion), payload, &data); err != nil {
		return nil, errors.Wrap(err, "failed to fetch data")
	}
	return data.Tables, nil
}

func (e *Extractor) executeCommand(query string) string {
	execstmtReq, execstmtErr := e.apiClient.ExecuteStatement(&redshiftdataapiservice.ExecuteStatementInput{
		ClusterIdentifier: aws.String(e.config.ClusterID),
		DbUser:            aws.String(e.config.DbUser),
		Database:          aws.String(e.config.DbName),
		Sql:               aws.String("query"),
	})
	if execstmtErr != nil {
		// logs error and exists
		e.logger.Fatal("", "error", execstmtErr)
	}

	descstmtReq, descstmtErr := e.apiClient.DescribeStatement(&redshiftdataapiservice.DescribeStatementInput{
		Id: execstmtReq.Id,
	})
	query_status := aws.StringValue(descstmtReq.Status)
	if descstmtErr != nil {
		// logs error and exists
		e.logger.Fatal("", "error", descstmtErr)
	}
	return query_status
}

// Prepares the list of tables and the attached metadata
func (e *Extractor) getTableMetadata(dbName string, tableName string) (result *assetsv1beta1.Table, err error) {
	var columns []*facetsv1beta1.Column
	columns, err = e.listColumn(dbName, tableName)
	if err != nil {
		return result, nil
	}

	result = &assetsv1beta1.Table{
		Resource: &commonv1beta1.Resource{
			Urn:     models.TableURN("redshift", "", dbName, tableName),
			Name:    tableName,
			Service: "redshift",
		},
		Schema: &facetsv1beta1.Columns{
			Columns: columns,
		},
	}

	return
}

func (e *Extractor) listColumn(dbName string, tableName string) (result []*facetsv1beta1.Column, err error) {
	payload := map[string]interface{}{
		"ClusterIdentifier": e.config.ClusterID,
		"ConnectedDatabase": e.config.DbName,
		"Database":          dbName,
		"DbUser":            e.config.DbUser,
		"MaxResults":        nil,
		"NextToken":         "",
		"SchemaPattern":     "information_schema",
		"SecretArn":         nil, // required when authenticating through secret manager
		"TablePattern":      nil,
	}
	type responseToken struct {
		ColumnList []ColumnList `json:"ColumnList"`
		NextToken  string       `json:"NextToken"`
		TableName  string       `json:"TableName"`
	}
	var data responseToken
	if err = e.makeRequest("POST", fmt.Sprintf("https://redshift-data.%s.amazonaws.com", e.config.AwsRegion), payload, &data); err != nil {
		return nil, errors.Wrap(err, "failed to fetch data")
	}
	//return data.ColumnList, nil
	var tempresults []*facetsv1beta1.Column
	for _, column := range data.ColumnList {
		var tempresult facetsv1beta1.Column
		tempresult.Name = column.Name
		tempresult.Description = column.Label
		tempresult.DataType = column.TypeName
		//tempresult.IsNullable
		//tempresult.Length = column.Length
		//tempresult.Profile
		//tempresult.Properties
		tempresults = append(tempresults, &tempresult)
	}
	return tempresults, nil
}

// makeRequest helper function to avoid rewriting a request
func (e *Extractor) makeRequest(method, url string, payload interface{}, data interface{}) (err error) {
	jsonifyPayload, err := json.Marshal(payload)
	if err != nil {
		return errors.Wrap(err, "failed to encode the payload JSON")
	}
	body := bytes.NewBuffer(jsonifyPayload)
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return errors.Wrap(err, "failed to create request")
	}

	var bearer = "Bearer " + e.config.AccessKeyID
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "RedshiftData.ListDatabases") // to list database (diff for all api, rest part is same)
	req.Header.Set("X-Requested-With", "XMLHttpRequest")

	req.Header.Set("Authorization", bearer)
	req.Header.Set("X-SecretKey", e.config.SecretAccessKey)
	//req.Header.Set("X-CSRFToken", e.csrfToken)
	req.Header.Set("Referer", url)

	res, err := e.client.Do(req)
	if err != nil {
		return errors.Wrap(err, "failed to generate response")
	}
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return errors.Wrapf(err, "response failed with status code: %d", res.StatusCode)
	}
	b, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return errors.Wrap(err, "failed to read response body")
	}
	if err = json.Unmarshal(b, &data); err != nil {
		return errors.Wrapf(err, "failed to parse: %s", string(b))
	}
	return
}

// Convert nullable string to a boolean
func isNullable(value string) bool {
	return value == "YES"
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
	if err := registry.Extractors.Register("metabase", func() plugins.Extractor {
		return New(redshiftdataapiservice.New(), plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}

// IMP Links :
// https://docs.aws.amazon.com/redshift/latest/mgmt/data-api.html
// https://aws.amazon.com/blogs/big-data/using-the-amazon-redshift-data-api-to-interact-with-amazon-redshift-clusters/

// each db -> tables -> each table through (describe table) to get column metadata
// TODO: make request is diff for each api (specifically headers)