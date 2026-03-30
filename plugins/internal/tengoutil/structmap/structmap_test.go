//go:build plugins
// +build plugins

package structmap

import (
	"testing"
	"time"

	meteorv1beta1 "github.com/raystack/meteor/models/raystack/meteor/v1beta1"
	testutils "github.com/raystack/meteor/test/utils"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestAsMap(t *testing.T) {
	cases := []struct {
		name        string
		input       any
		expected    any
		expectedErr bool
	}{
		{
			name:     "MapStringToString",
			input:    map[string]string{"key": "value"},
			expected: map[string]any{"key": "value"},
		},
		{
			name:     "MapIntToStringSlice",
			input:    map[int][]string{1: {"s1", "s2"}},
			expected: map[string]any{"1": []any{"s1", "s2"}},
		},
		{
			name:     "StringSlice",
			input:    []string{"s1", "s2"},
			expected: []any{"s1", "s2"},
		},
		{
			name: "Entity",
			input: &meteorv1beta1.Entity{
				Urn:    "urn:caramlstore:test-caramlstore:feature_table:avg_dispatch_arrival_time_10_mins",
				Name:   "avg_dispatch_arrival_time_10_mins",
				Source: "caramlstore",
				Type:   "feature_table",
				Properties: func() *structpb.Struct {
					s, _ := structpb.NewStruct(map[string]any{
						"namespace": "sauron",
					})
					return s
				}(),
				CreateTime: timestamppb.New(time.Date(2022, time.September, 19, 22, 42, 0o4, 0, time.UTC)),
				UpdateTime: timestamppb.New(time.Date(2022, time.September, 21, 13, 23, 0o2, 0, time.UTC)),
			},
			expected: map[string]any{
				"properties": map[string]any{
					"namespace": "sauron",
				},
				"name":        "avg_dispatch_arrival_time_10_mins",
				"source":      "caramlstore",
				"type":        "feature_table",
				"urn":         "urn:caramlstore:test-caramlstore:feature_table:avg_dispatch_arrival_time_10_mins",
				"create_time": "2022-09-19T22:42:04Z",
				"update_time": "2022-09-21T13:23:02Z",
			},
		},
		{
			name: "EntityWithTable",
			input: &meteorv1beta1.Entity{
				Urn:    "urn:cassandra:test-cassandra:table:cassandra_meteor_test.applicant",
				Name:   "applicant",
				Type:   "table",
				Source: "cassandra",
				Properties: func() *structpb.Struct {
					s, _ := structpb.NewStruct(map[string]any{
						"id":   "test-id",
						"name": "test-name",
					})
					return s
				}(),
			},
			expected: map[string]any{
				"properties": map[string]any{
					"id":   "test-id",
					"name": "test-name",
				},
				"name":   "applicant",
				"source": "cassandra",
				"type":   "table",
				"urn":    "urn:cassandra:test-cassandra:table:cassandra_meteor_test.applicant",
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
	input := map[string]any{"duration": "5s"}
	var v V
	err := AsStructWithTag("myspltag", input, &v)
	assert.NoError(t, err)
	assert.Equal(t, V{Duration: time.Second * 5}, v)
}

func TestAsStruct(t *testing.T) {
	cases := []struct {
		name        string
		input       any
		output      any
		expected    any
		expectedErr bool
	}{
		{
			name:     "MapStringToString",
			input:    map[string]any{"key": "value"},
			output:   map[string]string{},
			expected: map[string]string{"key": "value"},
		},
		{
			name:     "MapIntToStringSlice",
			input:    map[string]any{"1": []any{"s1", "s2"}},
			output:   map[int][]string{},
			expected: map[int][]string{1: {"s1", "s2"}},
		},
		{
			name:     "StringSlice",
			input:    []any{"s1", "s2"},
			output:   []string{},
			expected: []string{"s1", "s2"},
		},
		{
			name:        "MismatchedType",
			input:       []any{"s1"},
			output:      map[string]any{},
			expected:    map[string]any{},
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
		input       any
		output      proto.Message
		expected    proto.Message
		expectedErr string
	}{
		{
			name: "EntityBasic",
			input: map[string]any{
				"urn":    "urn:test:scope:table:myentity",
				"name":   "myentity",
				"source": "test",
				"type":   "table",
				"properties": map[string]any{
					"id":   "test-id",
					"name": "test-name",
				},
				"create_time": "2022-09-19T22:42:04Z",
			},
			output: &meteorv1beta1.Entity{},
			expected: &meteorv1beta1.Entity{
				Urn:    "urn:test:scope:table:myentity",
				Name:   "myentity",
				Source: "test",
				Type:   "table",
				Properties: &structpb.Struct{
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
			name:   "EntityWithProperties",
			output: &meteorv1beta1.Entity{},
			input: map[string]any{
				"urn":    "urn:cassandra:test-cassandra:table:cassandra_meteor_test.applicant",
				"name":   "applicant",
				"source": "cassandra",
				"type":   "table",
				"properties": map[string]any{
					"id":   "test-id",
					"name": "test-name",
				},
			},
			expected: &meteorv1beta1.Entity{
				Urn:    "urn:cassandra:test-cassandra:table:cassandra_meteor_test.applicant",
				Name:   "applicant",
				Type:   "table",
				Source: "cassandra",
				Properties: &structpb.Struct{
					Fields: map[string]*structpb.Value{
						"id":   structpb.NewStringValue("test-id"),
						"name": structpb.NewStringValue("test-name"),
					},
				},
			},
		},
		{
			name:   "UnknownKeys",
			output: &meteorv1beta1.Entity{},
			input: map[string]any{
				"does-not-exist": "value",
				"urn":            "urn:cassandra:test-cassandra:table:cassandra_meteor_test.applicant",
				"type":           "table",
			},
			expected:    &meteorv1beta1.Entity{},
			expectedErr: "invalid keys: does-not-exist",
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
