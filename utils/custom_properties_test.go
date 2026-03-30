package utils

import (
	"testing"

	meteorv1beta1 "github.com/raystack/meteor/models/raystack/meteor/v1beta1"
	testutils "github.com/raystack/meteor/test/utils"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestGetAttributes(t *testing.T) {
	cases := []struct {
		name     string
		entity   *meteorv1beta1.Entity
		expected map[string]interface{}
	}{
		{
			name: "EntityWithProperties",
			entity: &meteorv1beta1.Entity{
				Properties: TryParseMapToProto(map[string]interface{}{
					"a": 1,
					"b": "2",
					"c": map[string]interface{}{
						"d": true,
					},
				}),
			},
			expected: map[string]interface{}{
				"a": (float64)(1),
				"b": "2",
				"c": map[string]interface{}{
					"d": true,
				},
			},
		},
		{
			name:     "EntityWithoutProperties",
			entity:   &meteorv1beta1.Entity{},
			expected: map[string]interface{}{},
		},
		{
			name: "EntityWithNilProperties",
			entity: &meteorv1beta1.Entity{
				Properties: TryParseMapToProto(nil),
			},
			expected: map[string]interface{}{},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, GetAttributes(tc.entity))
		})
	}
}

func TestSetAttributes(t *testing.T) {
	cases := []struct {
		name         string
		entity       *meteorv1beta1.Entity
		customFields map[string]interface{}
		expected     *meteorv1beta1.Entity
		expectedErr  string
	}{
		{
			name:   "EntityWithProperties",
			entity: &meteorv1beta1.Entity{},
			customFields: map[string]interface{}{
				"a": 1,
				"b": "2",
				"c": map[string]interface{}{
					"d": true,
				},
			},
			expected: &meteorv1beta1.Entity{
				Properties: func() *structpb.Struct {
					s, _ := structpb.NewStruct(map[string]interface{}{
						"a": 1,
						"b": "2",
						"c": map[string]interface{}{
							"d": true,
						},
					})
					return s
				}(),
			},
		},
		{
			name: "EntityWithExistingProperties",
			entity: &meteorv1beta1.Entity{
				Properties: TryParseMapToProto(map[string]interface{}{
					"d": map[string]interface{}{
						"e": true,
					},
				}),
			},
			customFields: map[string]interface{}{
				"a": 1,
				"b": "2",
				"c": map[string]interface{}{
					"d": true,
				},
			},
			expected: &meteorv1beta1.Entity{
				Properties: func() *structpb.Struct {
					s, _ := structpb.NewStruct(map[string]interface{}{
						"a": 1,
						"b": "2",
						"c": map[string]interface{}{
							"d": true,
						},
					})
					return s
				}(),
			},
		},
		{
			name:   "FunkyCustomFields",
			entity: &meteorv1beta1.Entity{},
			customFields: map[string]interface{}{
				"unsupported": map[string]string{"test": "fail"},
			},
			expectedErr: "proto",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			actual, err := SetAttributes(tc.entity, tc.customFields)
			if tc.expectedErr != "" {
				assert.ErrorContains(t, err, tc.expectedErr)
				return
			}

			assert.NoError(t, err)
			testutils.AssertEqualProto(t, tc.expected, actual)
		})
	}
}
