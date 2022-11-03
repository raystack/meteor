//go:build plugins
// +build plugins

package structmap

import (
	"testing"
	"time"

	v1beta2 "github.com/odpf/meteor/models/odpf/assets/v1beta2"
	testutils "github.com/odpf/meteor/test/utils"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestAssetWrapper_AsMap(t *testing.T) {
	cases := []struct {
		name     string
		w        AssetWrapper
		expected map[string]interface{}
	}{
		{
			name: "AssetWithFeatureTable",
			w: AssetWrapper{A: &v1beta2.Asset{
				Urn:     "urn:caramlstore:test-caramlstore:feature_table:avg_dispatch_arrival_time_10_mins",
				Name:    "avg_dispatch_arrival_time_10_mins",
				Service: "caramlstore",
				Type:    "feature_table",
				Data: testutils.BuildAny(t, &v1beta2.FeatureTable{
					Namespace: "sauron",
					Entities: []*v1beta2.FeatureTable_Entity{
						{Name: "merchant_uuid", Labels: map[string]string{
							"description": "merchant uuid",
							"value_type":  "STRING",
						}},
					},
					Features: []*v1beta2.Feature{
						{Name: "ongoing_placed_and_waiting_acceptance_orders", DataType: "INT64"},
						{Name: "ongoing_orders", DataType: "INT64"},
						{Name: "merchant_avg_dispatch_arrival_time_10m", DataType: "FLOAT"},
						{Name: "ongoing_accepted_orders", DataType: "INT64"},
					},
					CreateTime: timestamppb.New(time.Date(2022, time.September, 19, 22, 42, 0o4, 0, time.UTC)),
					UpdateTime: timestamppb.New(time.Date(2022, time.September, 21, 13, 23, 0o2, 0, time.UTC)),
				}),
				Lineage: &v1beta2.Lineage{
					Upstreams: []*v1beta2.Resource{
						{
							Urn:     "urn:kafka:int-dagstream-kafka.yonkou.io:topic:GO_FOOD-delay-allocation-merchant-feature-10m-log",
							Service: "kafka",
							Type:    "topic",
						},
					},
				},
			}},
			expected: map[string]interface{}{
				"data": map[string]interface{}{
					"@type":       "type.googleapis.com/odpf.assets.v1beta2.FeatureTable",
					"create_time": "2022-09-19T22:42:04Z",
					"entities": []interface{}{
						map[string]interface{}{
							"labels": map[string]interface{}{"description": "merchant uuid", "value_type": "STRING"},
							"name":   "merchant_uuid",
						},
					},
					"features": []interface{}{
						map[string]interface{}{"data_type": "INT64", "name": "ongoing_placed_and_waiting_acceptance_orders"},
						map[string]interface{}{"data_type": "INT64", "name": "ongoing_orders"},
						map[string]interface{}{"data_type": "FLOAT", "name": "merchant_avg_dispatch_arrival_time_10m"},
						map[string]interface{}{"data_type": "INT64", "name": "ongoing_accepted_orders"},
					},
					"namespace":   "sauron",
					"update_time": "2022-09-21T13:23:02Z",
				},
				"lineage": map[string]interface{}{
					"upstreams": []interface{}{
						map[string]interface{}{
							"service": "kafka",
							"type":    "topic",
							"urn":     "urn:kafka:int-dagstream-kafka.yonkou.io:topic:GO_FOOD-delay-allocation-merchant-feature-10m-log",
						},
					},
				},
				"name":    "avg_dispatch_arrival_time_10_mins",
				"service": "caramlstore",
				"type":    "feature_table",
				"urn":     "urn:caramlstore:test-caramlstore:feature_table:avg_dispatch_arrival_time_10_mins",
			},
		},
		{
			name: "AssetWithTable",
			w: AssetWrapper{A: &v1beta2.Asset{
				Urn:     "urn:cassandra:test-cassandra:table:cassandra_meteor_test.applicant",
				Name:    "applicant",
				Type:    "table",
				Service: "cassandra",
				Data: testutils.BuildAny(t, &v1beta2.Table{
					Columns: []*v1beta2.Column{
						{Name: "applicantid", DataType: "int"},
						{Name: "first_name", DataType: "text"},
						{Name: "last_name", DataType: "text"},
					},
					Attributes: &structpb.Struct{
						Fields: map[string]*structpb.Value{
							"id":   structpb.NewStringValue("test-id"),
							"name": structpb.NewStringValue("test-name"),
						},
					},
				}),
			}},
			expected: map[string]interface{}{
				"data": map[string]interface{}{
					"@type": "type.googleapis.com/odpf.assets.v1beta2.Table",
					"columns": []interface{}{
						map[string]interface{}{"data_type": "int", "name": "applicantid"},
						map[string]interface{}{"data_type": "text", "name": "first_name"},
						map[string]interface{}{"data_type": "text", "name": "last_name"},
					},
					"attributes": map[string]interface{}{
						"id":   "test-id",
						"name": "test-name",
					},
				},
				"name":    "applicant",
				"service": "cassandra",
				"type":    "table",
				"urn":     "urn:cassandra:test-cassandra:table:cassandra_meteor_test.applicant",
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res, err := tc.w.AsMap()
			assert.NoError(t, err)
			assert.Equal(t, tc.expected, res)
		})
	}
}

func TestAssetWrapper_OverwriteWith(t *testing.T) {
	cases := []struct {
		name        string
		w           AssetWrapper
		input       map[string]interface{}
		expected    *v1beta2.Asset
		expectedErr bool
	}{
		{
			name: "AssetWithFeatureTable",
			w: AssetWrapper{A: &v1beta2.Asset{
				Data: &anypb.Any{TypeUrl: "type.googleapis.com/odpf.assets.v1beta2.FeatureTable"},
			}},
			input: map[string]interface{}{
				"data": map[string]interface{}{
					"@type":       "type.googleapis.com/odpf.assets.v1beta2.FeatureTable",
					"create_time": "2022-09-19T22:42:04Z",
					"entities": []interface{}{
						map[string]interface{}{
							"labels": map[string]interface{}{"description": "merchant uuid", "value_type": "STRING"},
							"name":   "merchant_uuid",
						},
					},
					"features": []interface{}{
						map[string]interface{}{"data_type": "INT64", "name": "ongoing_placed_and_waiting_acceptance_orders"},
						map[string]interface{}{"data_type": "INT64", "name": "ongoing_orders"},
						map[string]interface{}{"data_type": "FLOAT", "name": "merchant_avg_dispatch_arrival_time_10m"},
						map[string]interface{}{"data_type": "INT64", "name": "ongoing_accepted_orders"},
					},
					"namespace":   "sauron",
					"update_time": "2022-09-21T13:23:02Z",
				},
				"lineage": map[string]interface{}{
					"upstreams": []interface{}{
						map[string]interface{}{
							"service": "kafka",
							"type":    "topic",
							"urn":     "urn:kafka:int-dagstream-kafka.yonkou.io:topic:GO_FOOD-delay-allocation-merchant-feature-10m-log",
						},
					},
				},
				"create_time": "2022-10-19T22:42:04Z",
				"name":        "avg_dispatch_arrival_time_10_mins",
				"service":     "caramlstore",
				"type":        "feature_table",
				"urn":         "urn:caramlstore:test-caramlstore:feature_table:avg_dispatch_arrival_time_10_mins",
			},
			expected: &v1beta2.Asset{
				Urn:     "urn:caramlstore:test-caramlstore:feature_table:avg_dispatch_arrival_time_10_mins",
				Name:    "avg_dispatch_arrival_time_10_mins",
				Service: "caramlstore",
				Type:    "feature_table",
				Data: testutils.BuildAny(t, &v1beta2.FeatureTable{
					Namespace: "sauron",
					Entities: []*v1beta2.FeatureTable_Entity{
						{
							Name:   "merchant_uuid",
							Labels: map[string]string{"description": "merchant uuid", "value_type": "STRING"},
						},
					},
					Features: []*v1beta2.Feature{
						{Name: "ongoing_placed_and_waiting_acceptance_orders", DataType: "INT64"},
						{Name: "ongoing_orders", DataType: "INT64"},
						{Name: "merchant_avg_dispatch_arrival_time_10m", DataType: "FLOAT"},
						{Name: "ongoing_accepted_orders", DataType: "INT64"},
					},
					CreateTime: timestamppb.New(time.Date(2022, time.September, 19, 22, 42, 4, 0, time.UTC)),
					UpdateTime: timestamppb.New(time.Date(2022, time.September, 21, 13, 23, 2, 0, time.UTC)),
				}),
				Lineage: &v1beta2.Lineage{
					Upstreams: []*v1beta2.Resource{
						{
							Urn:     "urn:kafka:int-dagstream-kafka.yonkou.io:topic:GO_FOOD-delay-allocation-merchant-feature-10m-log",
							Service: "kafka",
							Type:    "topic",
						},
					},
				},
				CreateTime: timestamppb.New(time.Date(2022, time.October, 19, 22, 42, 4, 0, time.UTC)),
			},
		},
		{
			name: "AssetWithTable",
			w: AssetWrapper{A: &v1beta2.Asset{
				Data: &anypb.Any{TypeUrl: "type.googleapis.com/odpf.assets.v1beta2.Table"},
			}},
			input: map[string]interface{}{
				"data": map[string]interface{}{
					"@type": "type.googleapis.com/odpf.assets.v1beta2.Table",
					"columns": []interface{}{
						map[string]interface{}{"data_type": "int", "name": "applicantid"},
						map[string]interface{}{"data_type": "text", "name": "first_name"},
						map[string]interface{}{"data_type": "text", "name": "last_name"},
					},
					"attributes": map[string]interface{}{"id": "test-id", "name": "test-name"},
				},
				"name":    "applicant",
				"service": "cassandra",
				"type":    "table",
				"urn":     "urn:cassandra:test-cassandra:table:cassandra_meteor_test.applicant",
			},
			expected: &v1beta2.Asset{
				Urn:     "urn:cassandra:test-cassandra:table:cassandra_meteor_test.applicant",
				Name:    "applicant",
				Type:    "table",
				Service: "cassandra",
				Data: testutils.BuildAny(t, &v1beta2.Table{
					Columns: []*v1beta2.Column{
						{Name: "applicantid", DataType: "int"},
						{Name: "first_name", DataType: "text"},
						{Name: "last_name", DataType: "text"},
					},
					Attributes: &structpb.Struct{
						Fields: map[string]*structpb.Value{
							"id":   structpb.NewStringValue("test-id"),
							"name": structpb.NewStringValue("test-name"),
						},
					},
				}),
			},
		},
		{
			name: "WithoutData",
			w: AssetWrapper{A: &v1beta2.Asset{
				Data: &anypb.Any{TypeUrl: "type.googleapis.com/odpf.assets.v1beta2.Table"},
			}},
			input: map[string]interface{}{
				"name":    "applicant",
				"service": "cassandra",
				"type":    "table",
				"urn":     "urn:cassandra:test-cassandra:table:cassandra_meteor_test.applicant",
			},
			expected: &v1beta2.Asset{
				Data: &anypb.Any{TypeUrl: "type.googleapis.com/odpf.assets.v1beta2.Table"},
			},
			expectedErr: true,
		},
		{
			name: "UnknownKeys",
			w: AssetWrapper{A: &v1beta2.Asset{
				Data: &anypb.Any{TypeUrl: "type.googleapis.com/odpf.assets.v1beta2.Table"},
			}},
			input: map[string]interface{}{
				"does-not-exist": "value",
				"urn":            "urn:cassandra:test-cassandra:table:cassandra_meteor_test.applicant",
				"type":           "table",
				"data":           map[string]interface{}{},
			},
			expected: &v1beta2.Asset{
				Urn:  "urn:cassandra:test-cassandra:table:cassandra_meteor_test.applicant",
				Type: "table",
				Data: &anypb.Any{TypeUrl: "type.googleapis.com/odpf.assets.v1beta2.Table"},
			},
			expectedErr: true,
		},
		{
			name: "UnknownMessageName",
			w: AssetWrapper{A: &v1beta2.Asset{
				Data: &anypb.Any{TypeUrl: "type.googleapis.com/odpf.assets.v1beta2.DoesNotExist"},
			}},
			input: map[string]interface{}{
				"data": map[string]interface{}{},
			},
			expected: &v1beta2.Asset{
				Data: &anypb.Any{TypeUrl: "type.googleapis.com/odpf.assets.v1beta2.DoesNotExist"},
			},
			expectedErr: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.w.OverwriteWith(tc.input)
			assert.Equalf(t, tc.expectedErr, (err != nil),
				"AssetWrapper.OverwriteWith() err = %v,\nexpectedErr %v", err, tc.expectedErr)
			testutils.AssertEqualProto(t, tc.expected, tc.w.A)
		})
	}
}
