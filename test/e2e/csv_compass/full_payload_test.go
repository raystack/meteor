//go:build integration
// +build integration

package csv_compass_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/raystack/meteor/models"
	meteorv1beta1 "github.com/raystack/meteor/models/raystack/meteor/v1beta1"
	"github.com/raystack/meteor/plugins"
	"github.com/raystack/meteor/plugins/sinks/compass"
	testutils "github.com/raystack/meteor/test/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestFullPayload verifies that the compass sink correctly sends entities with
// lineage (upstreams/downstreams) and ownership edges to Compass v2.
func TestFullPayload(t *testing.T) {
	mock := newMockCompassServer()
	server := httptest.NewServer(mock)
	defer server.Close()

	compassSink := compass.New(&http.Client{}, testutils.Logger)
	err := compassSink.Init(context.TODO(), plugins.Config{RawConfig: map[string]interface{}{
		"host": server.URL,
		"headers": map[string]string{
			"Compass-User-UUID": "test@raystack.io",
		},
	}})
	require.NoError(t, err)

	entity := models.NewEntity(
		"urn:bigquery:myproject:table:dataset.users",
		"table",
		"users",
		"bigquery",
		map[string]interface{}{
			"description": "User accounts table",
			"url":         "https://console.cloud.google.com/bigquery?p=myproject&d=dataset&t=users",
			"columns": []interface{}{
				map[string]interface{}{"name": "id", "data_type": "INT64", "description": "Primary key"},
				map[string]interface{}{"name": "email", "data_type": "STRING", "description": "User email"},
				map[string]interface{}{"name": "created_at", "data_type": "TIMESTAMP", "description": "Account creation time"},
			},
			"attributes": map[string]interface{}{
				"partition_field": "created_at",
				"clustering":     "id",
			},
			"labels": map[string]interface{}{
				"team": "platform",
				"pii":  "true",
			},
		},
	)
	entity.Description = "User accounts table"

	edges := []*meteorv1beta1.Edge{
		models.OwnerEdge("urn:bigquery:myproject:table:dataset.users", "urn:user:alice@company.com", "meteor"),
		models.OwnerEdge("urn:bigquery:myproject:table:dataset.users", "urn:user:bob@company.com", "meteor"),
		models.LineageEdge("urn:airflow:prod:task:dag_users.extract", "urn:bigquery:myproject:table:dataset.users", "meteor"),
		models.LineageEdge("urn:bigquery:myproject:table:dataset.users", "urn:metabase:prod:dashboard:user_analytics", "meteor"),
		models.LineageEdge("urn:bigquery:myproject:table:dataset.users", "urn:bigquery:myproject:table:dataset.user_summary", "meteor"),
	}

	err = compassSink.Sink(context.TODO(), []models.Record{models.NewRecord(entity, edges...)})
	require.NoError(t, err)

	entities := mock.Entities()
	edgeResults := mock.Edges()

	// === Verify Entity ===
	require.Len(t, entities, 1)
	entityResult := entities[0]

	assert.Equal(t, "urn:bigquery:myproject:table:dataset.users", entityResult.URN)
	assert.Equal(t, "table", entityResult.Type)
	assert.Equal(t, "users", entityResult.Name)
	assert.Equal(t, "User accounts table", entityResult.Description)
	assert.Equal(t, "bigquery", entityResult.Source)

	// Lineage sent inline.
	assert.Equal(t, []string{"urn:airflow:prod:task:dag_users.extract"}, entityResult.Upstreams)
	assert.Equal(t, []string{
		"urn:metabase:prod:dashboard:user_analytics",
		"urn:bigquery:myproject:table:dataset.user_summary",
	}, entityResult.Downstreams)

	// Properties should contain flattened data, labels, and URL.
	props := entityResult.Properties
	assert.Equal(t, "https://console.cloud.google.com/bigquery?p=myproject&d=dataset&t=users", props["url"])
	labels, ok := props["labels"].(map[string]interface{})
	require.True(t, ok, "labels should be in properties")
	assert.Equal(t, "platform", labels["team"])
	assert.Equal(t, "true", labels["pii"])

	// Columns should be flattened from data.
	columns, ok := props["columns"].([]interface{})
	require.True(t, ok, "columns should be in properties")
	assert.Len(t, columns, 3)

	// Attributes should be flattened from data.
	attrs, ok := props["attributes"].(map[string]interface{})
	require.True(t, ok, "attributes should be in properties")
	assert.Equal(t, "created_at", attrs["partition_field"])

	// === Verify Ownership Edges ===
	require.Len(t, edgeResults, 2, "expected 2 owned_by edges")

	assert.Equal(t, "urn:bigquery:myproject:table:dataset.users", edgeResults[0].SourceURN)
	assert.Equal(t, "urn:user:alice@company.com", edgeResults[0].TargetURN)
	assert.Equal(t, "owned_by", edgeResults[0].Type)
	assert.Equal(t, "meteor", edgeResults[0].Source)

	assert.Equal(t, "urn:user:bob@company.com", edgeResults[1].TargetURN)
	assert.Equal(t, "owned_by", edgeResults[1].Type)

	// Print for visibility.
	fmt.Println("\n=== Full Payload E2E Summary ===")
	fmt.Printf("Entity: %s (%s)\n", entityResult.Name, entityResult.URN)
	fmt.Printf("  Source:      %s\n", entityResult.Source)
	fmt.Printf("  Upstreams:   %v\n", entityResult.Upstreams)
	fmt.Printf("  Downstreams: %v\n", entityResult.Downstreams)
	fmt.Printf("  Properties:  %d keys\n", len(props))

	propsJSON, _ := json.MarshalIndent(props, "  ", "  ")
	fmt.Printf("  %s\n", propsJSON)

	fmt.Printf("Edges: %d\n", len(edgeResults))
	for _, e := range edgeResults {
		fmt.Printf("  %s -[%s]-> %s\n", e.SourceURN, e.Type, e.TargetURN)
	}
}
