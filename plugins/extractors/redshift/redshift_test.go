//go:build plugins
// +build plugins

package redshift_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/redshiftdataapiservice"
	"github.com/aws/aws-sdk-go/service/redshiftdataapiservice/redshiftdataapiserviceiface"
	"github.com/goto/meteor/plugins"
	"github.com/goto/meteor/plugins/extractors/redshift"
	"github.com/goto/meteor/test/mocks"
	"github.com/goto/meteor/test/utils"
	"github.com/stretchr/testify/assert"
)

// Define a mock struct to be used in your unit tests of myFunc.
type mockRedshiftDataAPIClient struct {
	redshiftdataapiserviceiface.RedshiftDataAPIServiceAPI
	ListDatabasesOutput redshiftdataapiservice.ListDatabasesOutput
	ListTablesOutput    redshiftdataapiservice.ListTablesOutput
	DescribeTableOutput redshiftdataapiservice.DescribeTableOutput
}

func (m mockRedshiftDataAPIClient) ListDatabases(*redshiftdataapiservice.ListDatabasesInput) (*redshiftdataapiservice.ListDatabasesOutput, error) {
	// Only need to return mocked response output
	return &m.ListDatabasesOutput, nil
}

func (m mockRedshiftDataAPIClient) ListTables(input *redshiftdataapiservice.ListTablesInput) (*redshiftdataapiservice.ListTablesOutput, error) {
	return &m.ListTablesOutput, nil
}

func (m mockRedshiftDataAPIClient) DescribeTable(input *redshiftdataapiservice.DescribeTableInput) (*redshiftdataapiservice.DescribeTableOutput, error) {
	return &m.DescribeTableOutput, nil
}

func TestExtractor_GetDBList(t *testing.T) {
	// Define each output to mock as a return value.
	var (
		listDatabaseOutputNonEmptyDB = redshiftdataapiservice.ListDatabasesOutput{
			Databases: []*string{aws.String("dev"), aws.String("test")},
			NextToken: nil,
		}

		listDatabaseOutputEmptyDB = redshiftdataapiservice.ListDatabasesOutput{
			Databases: nil,
			NextToken: nil,
		}
	)
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
			Expected: nil,
		},
	}

	for _, c := range cases {
		mockSvc := &mockRedshiftDataAPIClient{
			RedshiftDataAPIServiceAPI: nil,
			ListDatabasesOutput:       c.Resp,
		}
		extractor := redshift.New(nil, redshift.WithClient(mockSvc))

		t.Run(c.Name, func(t *testing.T) {
			output, err := extractor.GetDBList()

			assert.Equal(t, c.Expected, output)
			assert.NoError(t, err)
		})
	}
}

func TestExtractor_GetTables(t *testing.T) {
	// Define each output to mock as a return value.
	var (
		listSingleTableOutput = redshiftdataapiservice.ListTablesOutput{
			NextToken: nil,
			Tables: []*redshiftdataapiservice.TableMember{
				{
					Name:   aws.String("sql_features"),
					Schema: aws.String("information_schema"),
					Type:   aws.String("SYSTEM TABLE"),
				},
			},
		}

		listMultipleTablesOutput = redshiftdataapiservice.ListTablesOutput{
			NextToken: nil,
			Tables: []*redshiftdataapiservice.TableMember{
				{
					Name:   aws.String("sql_features"),
					Schema: aws.String("information_schema"),
					Type:   aws.String("SYSTEM TABLE"),
				},
				{
					Name:   aws.String("sql_features_info"),
					Schema: aws.String("information_schema"),
					Type:   aws.String("SYSTEM TABLE"),
				},
			},
		}

		listTablesOutputEmptyDB = redshiftdataapiservice.ListTablesOutput{
			NextToken: nil,
			Tables:    nil,
		}
	)
	cases := []struct {
		Name     string
		Resp     redshiftdataapiservice.ListTablesOutput
		Expected []string
	}{
		{
			Name:     "NonEmptyDatabaseListOutput",
			Resp:     listSingleTableOutput,
			Expected: []string{"sql_features"},
		},
		{
			Name:     "NonEmptyDatabaseListOutput",
			Resp:     listMultipleTablesOutput,
			Expected: []string{"sql_features", "sql_features_info"},
		},
		{
			Name:     "EmptyDatabaseListOutput",
			Resp:     listTablesOutputEmptyDB,
			Expected: nil,
		},
	}

	for _, c := range cases {
		mockSvc := &mockRedshiftDataAPIClient{
			ListTablesOutput: c.Resp,
		}
		extractor := redshift.New(nil, redshift.WithClient(mockSvc))
		dbName := "dev"
		t.Run(c.Name, func(t *testing.T) {
			output, err := extractor.GetTables(dbName)

			assert.Equal(t, c.Expected, output)
			assert.NoError(t, err)
		})
	}
}

func TestExtractor_GetColumn(t *testing.T) {
	// Define each output to mock as a return value.
	var (
		// Table with single column
		describeTableOutputSingleColumn = redshiftdataapiservice.DescribeTableOutput{
			ColumnList: []*redshiftdataapiservice.ColumnMetadata{
				{
					Label:      aws.String("description"),
					Length:     aws.Int64(123),
					Name:       aws.String("column_name"),
					SchemaName: aws.String("information_schema"),
					TableName:  aws.String("table_name"),
					TypeName:   aws.String("character_data"),
				},
			},
			NextToken: nil,
			TableName: nil,
		}

		// Table with multiple column
		describeTableOutputMultipleColumn = redshiftdataapiservice.DescribeTableOutput{
			ColumnList: []*redshiftdataapiservice.ColumnMetadata{
				{
					Label:      aws.String("description"),
					Length:     aws.Int64(123),
					Name:       aws.String("column_name"),
					SchemaName: aws.String("information_schema"),
					TableName:  aws.String("table_name"),
					TypeName:   aws.String("character_data"),
				},
				{
					Label:      aws.String("description"),
					Length:     aws.Int64(456),
					Name:       aws.String("column_name_2"),
					SchemaName: aws.String("information_schema"),
					TableName:  aws.String("table_name"),
					TypeName:   aws.String("char_data"),
				},
			},
			NextToken: nil,
			TableName: nil,
		}

		// table with no column
		listTablesOutputEmptyDB = redshiftdataapiservice.DescribeTableOutput{
			ColumnList: nil,
			NextToken:  nil,
			TableName:  nil,
		}
	)
	cases := []struct {
		Name     string
		Resp     redshiftdataapiservice.DescribeTableOutput
		Expected []*redshiftdataapiservice.ColumnMetadata
	}{
		{
			Name: "NonEmptyDatabaseListOutput",
			Resp: describeTableOutputSingleColumn,
			Expected: []*redshiftdataapiservice.ColumnMetadata{
				{
					Label:      aws.String("description"),
					Length:     aws.Int64(123),
					Name:       aws.String("column_name"),
					SchemaName: aws.String("information_schema"),
					TableName:  aws.String("table_name"),
					TypeName:   aws.String("character_data"),
				},
			},
		},
		{
			Name: "EmptyDatabaseListOutput",
			Resp: describeTableOutputMultipleColumn,
			Expected: []*redshiftdataapiservice.ColumnMetadata{
				{
					Label:      aws.String("description"),
					Length:     aws.Int64(123),
					Name:       aws.String("column_name"),
					SchemaName: aws.String("information_schema"),
					TableName:  aws.String("table_name"),
					TypeName:   aws.String("character_data"),
				},
				{
					Label:      aws.String("description"),
					Length:     aws.Int64(456),
					Name:       aws.String("column_name_2"),
					SchemaName: aws.String("information_schema"),
					TableName:  aws.String("table_name"),
					TypeName:   aws.String("char_data"),
				},
			},
		},
		{
			Name:     "EmptyDatabaseListOutput",
			Resp:     listTablesOutputEmptyDB,
			Expected: nil,
		},
	}

	for _, c := range cases {
		mockSvc := &mockRedshiftDataAPIClient{
			DescribeTableOutput: c.Resp,
		}
		extractor := redshift.New(nil, redshift.WithClient(mockSvc))
		dbName := "dev"
		tableName := "table_name"
		t.Run(c.Name, func(t *testing.T) {
			output, err := extractor.GetColumn(dbName, tableName)

			assert.Equal(t, c.Expected, output)
			assert.NoError(t, err)
		})
	}
}

func TestInit(t *testing.T) {
	t.Run("should return error for invalid config", func(t *testing.T) {
		extractor := redshift.New(nil)

		err := extractor.Init(context.Background(), plugins.Config{
			URNScope:  "test-redshift",
			RawConfig: map[string]interface{}{},
		})
		assert.ErrorAs(t, err, &plugins.InvalidConfigError{})
	})
	t.Run("should return no error", func(t *testing.T) {
		mockSvc := &mockRedshiftDataAPIClient{
			RedshiftDataAPIServiceAPI: nil,
			ListDatabasesOutput: redshiftdataapiservice.ListDatabasesOutput{
				Databases: []*string{aws.String("dev"), aws.String("test")},
				NextToken: nil,
			},
		}
		extractor := redshift.New(nil, redshift.WithClient(mockSvc))

		err := extractor.Init(context.Background(), plugins.Config{
			URNScope: "test-redshift",
			RawConfig: map[string]interface{}{
				"cluster_id": "some-cluster-id",
				"db_name":    "some-db-name",
				"db_user":    "some-user",
				"aws_region": "google-project-id",
			},
		})
		assert.NoError(t, err)
	})
}

func TestExtract(t *testing.T) {
	t.Run("should return no error", func(t *testing.T) {
		mockSvc := &mockRedshiftDataAPIClient{
			RedshiftDataAPIServiceAPI: nil,
			ListDatabasesOutput: redshiftdataapiservice.ListDatabasesOutput{
				Databases: []*string{aws.String("dev"), aws.String("test")},
				NextToken: nil,
			},
			ListTablesOutput: redshiftdataapiservice.ListTablesOutput{
				NextToken: nil,
				Tables: []*redshiftdataapiservice.TableMember{
					{
						Name:   aws.String("sql_features"),
						Schema: aws.String("information_schema"),
						Type:   aws.String("SYSTEM TABLE"),
					},
					{
						Name:   aws.String("sql_features_info"),
						Schema: aws.String("information_schema"),
						Type:   aws.String("SYSTEM TABLE"),
					},
				},
			},
			DescribeTableOutput: redshiftdataapiservice.DescribeTableOutput{
				ColumnList: []*redshiftdataapiservice.ColumnMetadata{
					{
						Label:      aws.String("description"),
						Length:     aws.Int64(123),
						Name:       aws.String("column_name"),
						SchemaName: aws.String("information_schema"),
						TableName:  aws.String("table_name"),
						TypeName:   aws.String("character_data"),
					},
				},
			},
		}
		extractor := redshift.New(nil, redshift.WithClient(mockSvc))

		ctx := context.Background()

		err := extractor.Init(ctx, plugins.Config{
			URNScope: "test-redshift",
			RawConfig: map[string]interface{}{
				"cluster_id": "some-cluster-id",
				"db_name":    "some-db-name",
				"db_user":    "some-user",
				"aws_region": "google-project-id",
			},
		})
		assert.NoError(t, err)

		emitter := mocks.NewEmitter()
		err = extractor.Extract(ctx, emitter.Push)
		assert.NoError(t, err)

		actual := emitter.GetAllData()
		utils.AssertProtosWithJSONFile(t, "testdata/expected-assets.json", actual)
	})
}
