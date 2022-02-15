package redshift

import (
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/redshiftdataapiservice"
	"github.com/aws/aws-sdk-go/service/redshiftdataapiservice/redshiftdataapiserviceiface"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/registry"
	"github.com/odpf/salt/log"
)

//The URL for the Amazon Redshift Data API is: https://redshift-data.[aws-region].amazonaws.com

// Config holds the set of configuration for the metabase extractor
type Config struct {
	ClusterID string `json:"cluster_id"`
	Database  string `json:"database"`
	User      string `json:"user"`
	IamRole   string `json:"iam_role"`
}

type Extractor struct {
	config Config
	logger log.Logger
	//rsClient redshiftiface.RedshiftAPI
	//apiClient redshiftdata.Client
	apiClient redshiftdataapiserviceiface.RedshiftDataAPIServiceAPI
}

// New returns a pointer to an initialized Extractor Object
func New(client redshiftdataapiserviceiface.RedshiftDataAPIServiceAPI, logger log.Logger) *Extractor {
	return &Extractor{
		apiClient: client,
		logger:    logger,
	}
}

func (e *Extractor) Init() error {
	// Create session
	var sess = session.Must(session.NewSession())
	// Initialize the redshift client
	e.apiClient = redshiftdataapiservice.New(sess)

}

func (e *Extractor) Extract() error {

	// The Data API uses either credentials stored in AWS Secrets Manager or temporary database credentials.
	// auth through IAM -> get key -> access list db -> iterate through each db to list tables
	output, err := e.apiClient.ListDatabases(&redshiftdataapiservice.ListDatabasesInput{
		ClusterIdentifier: nil,
		Database:          nil,
		DbUser:            nil,
		MaxResults:        nil,
		NextToken:         nil,
		SecretArn:         nil,
	})
	if err != nil {
		return err
	}
	listDB := output.Databases

	for _, db := range listDB {
		// iterate through each db to list tables
	}
	return nil
}

// Register the extractor to catalog
func init() {
	if err := registry.Extractors.Register("metabase", func() plugins.Extractor {
		return New(redshiftdataapiservice.New(), plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}
