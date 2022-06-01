//go:build plugins
// +build plugins

package auditlog

import (
	"context"
	"testing"

	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/test/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	auditpb "google.golang.org/genproto/googleapis/cloud/audit"
	loggingpb "google.golang.org/genproto/googleapis/cloud/bigquery/logging/v1"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestInit(t *testing.T) {
	t.Run("should return error if failed to init client", func(t *testing.T) {
		la := New(utils.Logger)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		err := la.Init(ctx, Config{
			ProjectID:          "---",
			ServiceAccountJSON: "---",
		})

		assert.EqualError(t, err, "failed to create logadmin client: client is nil, failed initiating client")
	})

	t.Run("should not return error if init client is success", func(t *testing.T) {
		la := New(utils.Logger)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		err := la.Init(ctx, Config{})

		assert.NotEqual(t, plugins.InvalidConfigError{}, err)
	})
}

func TestGetAuditData(t *testing.T) {
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

		auditData := &loggingpb.AuditData{}
		err = getAuditData(auditLog, auditData)
		assert.Nil(t, err)
		assert.NotNil(t, auditData)
		assert.NotEmpty(t, auditData)
	})

	t.Run("should parse with metadata if service data not exists and metadata exist", func(t *testing.T) {
		loggingData, err := structpb.NewStruct(map[string]interface{}{
			"jobCompletedEvent": nil,
		})

		require.Nil(t, err)

		auditLog := &auditpb.AuditLog{
			Metadata: loggingData,
		}

		auditData := &loggingpb.AuditData{}
		err = getAuditData(auditLog, auditData)
		assert.Nil(t, err)
		assert.NotNil(t, auditData)
		assert.NotEmpty(t, auditData)
	})

	t.Run("should return error if neither service data nor metadata field exist", func(t *testing.T) {
		auditLog := &auditpb.AuditLog{}

		auditData := &loggingpb.AuditData{}
		err := getAuditData(auditLog, auditData)
		assert.EqualError(t, err, "metadata field is nil")
		assert.Empty(t, auditData)
	})
}
