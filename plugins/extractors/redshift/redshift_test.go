package redshift_test

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/redshiftdataapiservice"
	"github.com/aws/aws-sdk-go/service/redshiftdataapiservice/redshiftdataapiserviceiface"
	"github.com/odpf/meteor/plugins/extractors/redshift"
	"github.com/stretchr/testify/assert"
	"testing"
)

// Define a mock struct to be used in your unit tests of myFunc.
type mockRedshiftDataAPIServiceClient struct {
	redshiftdataapiserviceiface.RedshiftDataAPIServiceAPI
	ListDatabasesOutput redshiftdataapiservice.ListDatabasesOutput
}

//func (m *mockRedshiftDataAPIServiceClient) ListDatabases(input *redshiftdataapiservice.ListDatabasesInput) (out *redshiftdataapiservice.ListDatabasesOutput, err error) {
//	input = &redshiftdataapiservice.ListDatabasesInput{
//		ClusterIdentifier: nil,
//		Database:          nil,
//		DbUser:            nil,
//		MaxResults:        nil,
//		NextToken:         nil,
//		SecretArn:         nil,
//	}
//
//	out = &redshiftdataapiservice.ListDatabasesOutput{
//		Databases: nil,
//		NextToken: nil,
//	}
//
//	return out, nil
//}

func (m *mockRedshiftDataAPIServiceClient) ListDatabases(*redshiftdataapiservice.ListDatabasesInput) (*redshiftdataapiservice.ListDatabasesOutput, error) {
	return &m.ListDatabasesOutput, nil
}

var (
	listDatabaseOutputNonEmptyDB = redshiftdataapiservice.ListDatabasesOutput{
		Databases: []*string{aws.String("dev"), aws.String("test")},
		NextToken: nil,
	}

	listDatabaseOutputEmptyDB = redshiftdataapiservice.ListDatabasesOutput{
		Databases: []*string{aws.String("dev"), aws.String("test")},
		NextToken: nil,
	}
)

func TestExtractor_GetDBList(t *testing.T) {
	mockSvc := &mockRedshiftDataAPIServiceClient{}
	mockExtractor := redshift.New(mockSvc, nil)
	cases := []struct {
		Name     string
		Resp     redshiftdataapiservice.ListDatabasesOutput
		Expected []string
	}{
		{
			Name:     "NonEmptyDatabaseListOutput",
			Resp:     listDatabaseOutputNonEmptyDB,
			Expected: []string{"dev", "test"},
		},
		{
			Name:     "EmptyDatabaseListOutput",
			Resp:     listDatabaseOutputEmptyDB,
			Expected: []string{},
		},
	}

	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			out, err := mockExtractor.GetDBList()

			assert.Equal(t, c.Expected, out)

			assert.NoError(t, err)
		})
	}
}

//func TestExtractor_GetDBList(t *testing.T) {
//	mockSvc := &mockRedshiftDataAPIServiceClient{}
//	mockExtractor := redshift.New(mockSvc, nil)
//
//	listDB, err := mockExtractor.GetDBList()
//	if err != nil {
//		t.Error(err)
//	}
//	assert.Equal(t, listDB, "")
//}

func TestExtractor_GetTables(t *testing.T) {
	mockSvc := &mockRedshiftDataAPIServiceClient{}
	mockExtractor := redshift.New(mockSvc, nil)
	dbName := ""
	list, err := mockExtractor.GetTables(dbName)
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, list, "")
}

func TestExtractor_GetColumn(t *testing.T) {
	mockSvc := &mockRedshiftDataAPIServiceClient{}
	mockExtractor := redshift.New(mockSvc, nil)
	dbName := ""
	tableName := ""
	list, err := mockExtractor.GetColumn(dbName, tableName)
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, list, "")
}
