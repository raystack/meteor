//go:build plugins
// +build plugins

package structmap

import (
	"testing"
	"time"

	v1beta2 "github.com/odpf/meteor/models/odpf/assets/v1beta2"
	"github.com/odpf/meteor/utils"
	"github.com/stretchr/testify/assert"
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
			name: "WithProtoMessage",
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
}
