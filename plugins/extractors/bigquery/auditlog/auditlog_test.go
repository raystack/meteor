//go:build plugins
// +build plugins

package auditlog

import (
	"context"
	"testing"

	"cloud.google.com/go/logging/logadmin"
	"github.com/goto/meteor/plugins"
	"github.com/goto/meteor/test/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	auditpb "google.golang.org/genproto/googleapis/cloud/audit"
	loggingpb "google.golang.org/genproto/googleapis/cloud/bigquery/logging/v1"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestInit(t *testing.T) {
	t.Run("should return error if config is wrong to init client", func(t *testing.T) {
		la := New(utils.Logger)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		err := la.Init(ctx,
			InitWithConfig(Config{
				ProjectID:          "---",
				ServiceAccountJSON: "---",
			}),
		)

		assert.EqualError(t, err, "failed to create logadmin client: client is nil, failed initiating client")
	})

	t.Run("should not return error invalid config if config is not wrong", func(t *testing.T) {
		la := New(utils.Logger)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		err := la.Init(ctx)

		assert.NotEqual(t, plugins.InvalidConfigError{}, err)
	})

	t.Run("should return no error init succeed", func(t *testing.T) {
		la := New(utils.Logger)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		err := la.Init(ctx, InitWithClient(&logadmin.Client{}))

		assert.Nil(t, err)
	})
}

func TestBuildFilter(t *testing.T) {
	var (
		la      = &AuditLog{}
		tableID = "table-id"
	)

	filterQuery := la.buildFilter(tableID)

	assert.Contains(t, filterQuery, tableID)
}

func TestParsePayload(t *testing.T) {
	t.Run("should parse with service data if service data exists", func(t *testing.T) {
		loggingData, err := anypb.New(&loggingpb.AuditData{
			JobCompletedEvent: &loggingpb.JobCompletedEvent{
				EventName: "",
				Job: &loggingpb.Job{
					JobStatistics: &loggingpb.JobStatistics{
						ReferencedTables: []*loggingpb.TableName{
							{
								ProjectId: "project_id",
							},
						},
					},
					JobStatus: &loggingpb.JobStatus{
						State: "DONE",
					},
				},
			},
		})
		require.Nil(t, err)

		auditLog := &auditpb.AuditLog{
			ServiceData: loggingData,
		}

		ld, err := parsePayload(auditLog)

		assert.Nil(t, err)
		assert.NotNil(t, ld.AuditData)
		assert.NotEmpty(t, ld.AuditData)
	})

	t.Run("should parse with metadata if service data not exists and metadata exist", func(t *testing.T) {
		loggingData, err := structpb.NewStruct(map[string]interface{}{
			"jobCompletedEvent": map[string]interface{}{
				"event_name": "name",
				"job": map[string]interface{}{
					"job_statistics": map[string]interface{}{
						"referenced_tables": []interface{}{map[string]interface{}{
							"project_id": "project_id",
						}},
					},
					"job_status": map[string]interface{}{
						"state": "DONE",
					},
				},
			},
		})
		require.Nil(t, err)

		auditLog := &auditpb.AuditLog{
			Metadata: loggingData,
		}

		ld, err := parsePayload(auditLog)

		assert.Nil(t, err)
		assert.NotNil(t, ld.AuditData)
		assert.NotEmpty(t, ld.AuditData)
	})

	t.Run("should return error if neither service data nor metadata field exist", func(t *testing.T) {
		auditLog := &auditpb.AuditLog{}

		ld, err := parsePayload(auditLog)

		assert.EqualError(t, err, "failed to get audit data from metadata: metadata field is nil")
		assert.Nil(t, ld)
	})
}
