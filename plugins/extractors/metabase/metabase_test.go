//go:build integration
// +build integration

package metabase_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/odpf/meteor/models"
	testutils "github.com/odpf/meteor/test/utils"
	"github.com/odpf/meteor/utils"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/odpf/meteor/models/odpf/assets"
	"github.com/odpf/meteor/models/odpf/assets/common"
	"github.com/odpf/meteor/models/odpf/assets/facets"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/plugins/extractors/metabase"
	"github.com/odpf/meteor/test/mocks"
	"github.com/stretchr/testify/assert"
)

const (
	instanceLabel = "my-meta"
)

func TestInit(t *testing.T) {
	t.Run("should return error for invalid config", func(t *testing.T) {
		err := metabase.New(testutils.Logger).Init(context.TODO(), map[string]interface{}{
			"username": "user",
			"host":     host,
		})

		assert.Equal(t, plugins.InvalidConfigError{}, err)
	})
}

func TestExtract(t *testing.T) {
	t.Run("should return dashboard model", func(t *testing.T) {
		ctx := context.TODO()
		extr := metabase.New(testutils.Logger)
		err := extr.Init(ctx, map[string]interface{}{
			"host":       host,
			"username":   email,
			"password":   pass,
			"label":      instanceLabel,
			"session_id": session_id,
		})
		if err != nil {
			t.Fatal(err)
		}

		emitter := mocks.NewEmitter()
		err = extr.Extract(ctx, emitter.Push)
		assert.NoError(t, err)

		expected := expectedData()
		records := emitter.Get()
		var actuals []models.Metadata
		for _, r := range records {
			actuals = append(actuals, r.Data())
		}

		assert.Len(t, actuals, len(expected))
		assertJSON(t, expected, actuals)
	})
}

func expectedData() (records []*assets.Dashboard) {
	for _, d := range populatedDashboards {
		createdAt, _ := d.CreatedAt()
		updatedAt, _ := d.UpdatedAt()
		cards := dashboardCards[d.ID]

		dashboardUrn := fmt.Sprintf("metabase::%s/dashboard/%d", instanceLabel, d.ID)
		var charts []*assets.Chart
		for _, card := range cards {
			charts = append(charts, &assets.Chart{
				Urn:          fmt.Sprintf("metabase::%s/card/%d", instanceLabel, card.ID),
				DashboardUrn: dashboardUrn,
				Source:       "metabase",
				Name:         card.Name,
				Description:  card.Description,
				Properties: &facets.Properties{
					Attributes: utils.TryParseMapToProto(map[string]interface{}{
						"id":                     card.ID,
						"collection_id":          card.CollectionID,
						"creator_id":             card.CreatorID,
						"database_id":            card.DatabaseID,
						"table_id":               card.TableID,
						"query_average_duration": card.QueryAverageDuration,
						"display":                card.Display,
						"archived":               card.Archived,
					}),
				},
			})
		}

		records = append(records, &assets.Dashboard{
			Resource: &common.Resource{
				Urn:         dashboardUrn,
				Name:        d.Name,
				Service:     "metabase",
				Description: d.Description,
			},
			Properties: &facets.Properties{
				Attributes: utils.TryParseMapToProto(map[string]interface{}{
					"id":            d.ID,
					"collection_id": d.CollectionID,
					"creator_id":    d.CreatorID,
				}),
			},
			Charts: charts,
			Timestamps: &common.Timestamp{
				CreateTime: timestamppb.New(createdAt),
				UpdateTime: timestamppb.New(updatedAt),
			},
		})
	}

	return
}

func assertJSON(t *testing.T, expected interface{}, actual interface{}) {
	actualBytes, err := json.Marshal(actual)
	if err != nil {
		t.Fatal(err)
	}
	expectedBytes, err := json.Marshal(expected)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, string(expectedBytes), string(actualBytes))
}
