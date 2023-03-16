//go:build plugins
// +build plugins

package structmap

import (
	"testing"
	"time"

	v1beta2 "github.com/goto/meteor/models/gotocompany/assets/v1beta2"
	testutils "github.com/goto/meteor/test/utils"
	"github.com/goto/meteor/utils"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestAsMap(t *testing.T) {
	cases := []struct {
		name        string
		input       interface{}
		expected    interface{}
		expectedErr bool
	}{
		{
			name:     "MapStringToString",
			input:    map[string]string{"key": "value"},
			expected: map[string]interface{}{"key": "value"},
		},
		{
			name:     "MapIntToStringSlice",
			input:    map[int][]string{1: {"s1", "s2"}},
			expected: map[string]interface{}{"1": []interface{}{"s1", "s2"}},
		},
		{
			name:     "StringSlice",
			input:    []string{"s1", "s2"},
			expected: []interface{}{"s1", "s2"},
		},
		{
			name: "ProtoMessage",
			input: &v1beta2.Job{
				Attributes: utils.TryParseMapToProto(map[string]interface{}{
					"id":   "test-id",
					"name": "test-name",
				}),
				CreateTime: timestamppb.New(time.Date(
					2022, time.September, 19, 22, 42, 0o4, 0, time.UTC,
				)),
			},
			expected: map[string]interface{}{
				"attributes": map[string]interface{}{
					"id":   "test-id",
					"name": "test-name",
				},
				"create_time": "2022-09-19T22:42:04Z",
			},
		},
		{
			name: "AssetWithFeatureTable",
			input: &v1beta2.Asset{
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
			},
			expected: map[string]interface{}{
				"data": map[string]interface{}{
					"@type":       "type.googleapis.com/gotocompany.assets.v1beta2.FeatureTable",
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
			input: &v1beta2.Asset{
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
			expected: map[string]interface{}{
				"data": map[string]interface{}{
					"@type": "type.googleapis.com/gotocompany.assets.v1beta2.Table",
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
		{
			name:        "MarshalFailure",
			input:       make(chan int),
			expectedErr: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res, err := AsMap(tc.input)
			assert.Equalf(t, tc.expectedErr, (err != nil),
				"AsMap() err = %v,\nexpectedErr %v", err, tc.expectedErr)
			assert.Equal(t, tc.expected, res)
		})
	}
}

func TestAsStructWithTag(t *testing.T) {
	type V struct {
		Duration time.Duration `myspltag:"duration"`
	}
	input := map[string]interface{}{"duration": "5s"}
	var v V
	err := AsStructWithTag("myspltag", input, &v)
	assert.NoError(t, err)
	assert.Equal(t, V{Duration: time.Second * 5}, v)
}

func TestAsStruct(t *testing.T) {
	cases := []struct {
		name        string
		input       interface{}
		output      interface{}
		expected    interface{}
		expectedErr bool
	}{
		{
			name:     "MapStringToString",
			input:    map[string]interface{}{"key": "value"},
			output:   map[string]string{},
			expected: map[string]string{"key": "value"},
		},
		{
			name:     "MapIntToStringSlice",
			input:    map[string]interface{}{"1": []interface{}{"s1", "s2"}},
			output:   map[int][]string{},
			expected: map[int][]string{1: {"s1", "s2"}},
		},
		{
			name:     "StringSlice",
			input:    []interface{}{"s1", "s2"},
			output:   []string{},
			expected: []string{"s1", "s2"},
		},
		{
			name:        "MismatchedType",
			input:       []interface{}{"s1"},
			output:      map[string]interface{}{},
			expected:    map[string]interface{}{},
			expectedErr: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := AsStruct(tc.input, &tc.output)
			assert.Equalf(t, tc.expectedErr, (err != nil),
				"AsStruct() err = %v,\nexpectedErr %v", err, tc.expectedErr)
			assert.Equal(t, tc.expected, tc.output)
		})
	}

	protoCases := []struct {
		name        string
		input       interface{}
		output      proto.Message
		expected    proto.Message
		expectedErr string
	}{
		{
			name: "WithProtoMessage",
			input: map[string]interface{}{
				"attributes": map[string]interface{}{
					"id":   "test-id",
					"name": "test-name",
				},
				"create_time": "2022-09-19T22:42:04Z",
			},
			output: &v1beta2.Job{},
			expected: &v1beta2.Job{
				Attributes: &structpb.Struct{
					Fields: map[string]*structpb.Value{
						"id":   structpb.NewStringValue("test-id"),
						"name": structpb.NewStringValue("test-name"),
					},
				},
				CreateTime: timestamppb.New(time.Date(
					2022, time.September, 19, 22, 42, 0o4, 0, time.UTC,
				)),
			},
		},
		{
			name:   "AssetWithFeatureTable",
			output: &v1beta2.Asset{},
			input: map[string]interface{}{
				"data": map[string]interface{}{
					"@type":       "type.googleapis.com/gotocompany.assets.v1beta2.FeatureTable",
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
			name:   "AssetWithTable",
			output: &v1beta2.Asset{},
			input: map[string]interface{}{
				"data": map[string]interface{}{
					"@type": "type.googleapis.com/gotocompany.assets.v1beta2.Table",
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
			name:   "WithoutData",
			output: &v1beta2.Asset{},
			input: map[string]interface{}{
				"name":    "applicant",
				"service": "cassandra",
				"type":    "table",
				"urn":     "urn:cassandra:test-cassandra:table:cassandra_meteor_test.applicant",
			},
			expected:    &v1beta2.Asset{},
			expectedErr: "mapstructure check asset data: unexpected type: <nil>",
		},
		{
			name:   "UnknownKeys",
			output: &v1beta2.Asset{},
			input: map[string]interface{}{
				"does-not-exist": "value",
				"urn":            "urn:cassandra:test-cassandra:table:cassandra_meteor_test.applicant",
				"type":           "table",
				"data":           map[string]interface{}{},
			},
			expected:    &v1beta2.Asset{},
			expectedErr: "invalid keys: does-not-exist",
		},
		{
			name:   "UnknownMessageName",
			output: &v1beta2.Asset{},
			input: map[string]interface{}{
				"data": map[string]interface{}{
					"@type": "type.googleapis.com/gotocompany.assets.v1beta2.DoesNotExist",
				},
			},
			expected:    &v1beta2.Asset{},
			expectedErr: "mapstructure map to anypb hook: resolve type: proto",
		},
	}
	for _, tc := range protoCases {
		t.Run(tc.name, func(t *testing.T) {
			err := AsStruct(tc.input, &tc.output)
			if tc.expectedErr == "" {
				assert.NoError(t, err)
			} else {
				assert.ErrorContains(t, err, tc.expectedErr)
			}

			testutils.AssertEqualProto(t, tc.expected, tc.output)
		})
	}
}
