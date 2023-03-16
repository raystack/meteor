package utils

import (
	"testing"

	v1beta2 "github.com/goto/meteor/models/gotocompany/assets/v1beta2"
	testutils "github.com/goto/meteor/test/utils"
	"github.com/stretchr/testify/assert"
)

func TestGetAttributes(t *testing.T) {
	cases := []struct {
		name     string
		asset    *v1beta2.Asset
		expected map[string]interface{}
	}{
		{
			name: "Table",
			asset: &v1beta2.Asset{
				Data: testutils.BuildAny(t, &v1beta2.Table{
					Attributes: TryParseMapToProto(map[string]interface{}{
						"a": 1,
						"b": "2",
						"c": map[string]interface{}{
							"d": true,
						},
					}),
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
			name: "Topic",
			asset: &v1beta2.Asset{
				Data: testutils.BuildAny(t, &v1beta2.Topic{
					Attributes: TryParseMapToProto(map[string]interface{}{
						"a": 1,
						"b": "2",
						"c": map[string]interface{}{
							"d": true,
						},
					}),
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
			name: "Dashboard",
			asset: &v1beta2.Asset{
				Data: testutils.BuildAny(t, &v1beta2.Dashboard{
					Attributes: TryParseMapToProto(map[string]interface{}{
						"a": 1,
						"b": "2",
						"c": map[string]interface{}{
							"d": true,
						},
					}),
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
			name: "Job",
			asset: &v1beta2.Asset{
				Data: testutils.BuildAny(t, &v1beta2.Job{
					Attributes: TryParseMapToProto(map[string]interface{}{
						"a": 1,
						"b": "2",
						"c": map[string]interface{}{
							"d": true,
						},
					}),
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
			name: "User",
			asset: &v1beta2.Asset{
				Data: testutils.BuildAny(t, &v1beta2.User{
					Attributes: TryParseMapToProto(map[string]interface{}{
						"a": 1,
						"b": "2",
						"c": map[string]interface{}{
							"d": true,
						},
					}),
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
			name: "Bucket",
			asset: &v1beta2.Asset{
				Data: testutils.BuildAny(t, &v1beta2.Bucket{
					Attributes: TryParseMapToProto(map[string]interface{}{
						"a": 1,
						"b": "2",
						"c": map[string]interface{}{
							"d": true,
						},
					}),
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
			name: "Group",
			asset: &v1beta2.Asset{
				Data: testutils.BuildAny(t, &v1beta2.Group{
					Attributes: TryParseMapToProto(map[string]interface{}{
						"a": 1,
						"b": "2",
						"c": map[string]interface{}{
							"d": true,
						},
					}),
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
			name: "Model",
			asset: &v1beta2.Asset{
				Data: testutils.BuildAny(t, &v1beta2.Model{
					Attributes: TryParseMapToProto(map[string]interface{}{
						"a": 1,
						"b": "2",
						"c": map[string]interface{}{
							"d": true,
						},
					}),
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
			name: "Experiment",
			asset: &v1beta2.Asset{
				Data: testutils.BuildAny(t, &v1beta2.Experiment{
					Attributes: TryParseMapToProto(map[string]interface{}{
						"a": 1,
						"b": "2",
						"c": map[string]interface{}{
							"d": true,
						},
					}),
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
			name: "Metric",
			asset: &v1beta2.Asset{
				Data: testutils.BuildAny(t, &v1beta2.Metric{
					Attributes: TryParseMapToProto(map[string]interface{}{
						"a": 1,
						"b": "2",
						"c": map[string]interface{}{
							"d": true,
						},
					}),
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
			name: "Application",
			asset: &v1beta2.Asset{
				Data: testutils.BuildAny(t, &v1beta2.Application{}),
			},
			expected: map[string]interface{}{},
		},
		{
			name: "FeatureTable",
			asset: &v1beta2.Asset{
				Data: testutils.BuildAny(t, &v1beta2.FeatureTable{}),
			},
			expected: map[string]interface{}{},
		},
		{
			name: "TableWithoutAttributes",
			asset: &v1beta2.Asset{
				Data: testutils.BuildAny(t, &v1beta2.Table{}),
			},
			expected: map[string]interface{}{},
		},
		{
			name:     "AssetWithoutData",
			asset:    &v1beta2.Asset{},
			expected: map[string]interface{}{},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, GetAttributes(tc.asset))
		})
	}
}

func TestSetAttributes(t *testing.T) {
	cases := []struct {
		name         string
		asset        *v1beta2.Asset
		customFields map[string]interface{}
		expected     *v1beta2.Asset
		expectedErr  string
	}{
		{
			name: "Table",
			asset: &v1beta2.Asset{
				Data: testutils.BuildAny(t, &v1beta2.Table{}),
			},
			customFields: map[string]interface{}{
				"a": 1,
				"b": "2",
				"c": map[string]interface{}{
					"d": true,
				},
			},
			expected: &v1beta2.Asset{
				Data: testutils.BuildAny(t, &v1beta2.Table{
					Attributes: TryParseMapToProto(map[string]interface{}{
						"a": 1,
						"b": "2",
						"c": map[string]interface{}{
							"d": true,
						},
					}),
				}),
			},
		},
		{
			name: "Topic",
			asset: &v1beta2.Asset{
				Data: testutils.BuildAny(t, &v1beta2.Topic{}),
			},
			customFields: map[string]interface{}{
				"a": (float64)(1),
				"b": "2",
				"c": map[string]interface{}{
					"d": true,
				},
			},
			expected: &v1beta2.Asset{
				Data: testutils.BuildAny(t, &v1beta2.Topic{
					Attributes: TryParseMapToProto(map[string]interface{}{
						"a": 1,
						"b": "2",
						"c": map[string]interface{}{
							"d": true,
						},
					}),
				}),
			},
		},
		{
			name: "Dashboard",
			asset: &v1beta2.Asset{
				Data: testutils.BuildAny(t, &v1beta2.Dashboard{}),
			},
			customFields: map[string]interface{}{
				"a": (float64)(1),
				"b": "2",
				"c": map[string]interface{}{
					"d": true,
				},
			},
			expected: &v1beta2.Asset{
				Data: testutils.BuildAny(t, &v1beta2.Dashboard{
					Attributes: TryParseMapToProto(map[string]interface{}{
						"a": 1,
						"b": "2",
						"c": map[string]interface{}{
							"d": true,
						},
					}),
				}),
			},
		},
		{
			name: "Job",
			asset: &v1beta2.Asset{
				Data: testutils.BuildAny(t, &v1beta2.Job{}),
			},
			customFields: map[string]interface{}{
				"a": (float64)(1),
				"b": "2",
				"c": map[string]interface{}{
					"d": true,
				},
			},
			expected: &v1beta2.Asset{
				Data: testutils.BuildAny(t, &v1beta2.Job{
					Attributes: TryParseMapToProto(map[string]interface{}{
						"a": 1,
						"b": "2",
						"c": map[string]interface{}{
							"d": true,
						},
					}),
				}),
			},
		},
		{
			name: "User",
			asset: &v1beta2.Asset{
				Data: testutils.BuildAny(t, &v1beta2.User{}),
			},
			customFields: map[string]interface{}{
				"a": (float64)(1),
				"b": "2",
				"c": map[string]interface{}{
					"d": true,
				},
			},
			expected: &v1beta2.Asset{
				Data: testutils.BuildAny(t, &v1beta2.User{
					Attributes: TryParseMapToProto(map[string]interface{}{
						"a": 1,
						"b": "2",
						"c": map[string]interface{}{
							"d": true,
						},
					}),
				}),
			},
		},
		{
			name: "Bucket",
			asset: &v1beta2.Asset{
				Data: testutils.BuildAny(t, &v1beta2.Bucket{}),
			},
			customFields: map[string]interface{}{
				"a": (float64)(1),
				"b": "2",
				"c": map[string]interface{}{
					"d": true,
				},
			},
			expected: &v1beta2.Asset{
				Data: testutils.BuildAny(t, &v1beta2.Bucket{
					Attributes: TryParseMapToProto(map[string]interface{}{
						"a": 1,
						"b": "2",
						"c": map[string]interface{}{
							"d": true,
						},
					}),
				}),
			},
		},
		{
			name: "Group",
			asset: &v1beta2.Asset{
				Data: testutils.BuildAny(t, &v1beta2.Group{}),
			},
			customFields: map[string]interface{}{
				"a": (float64)(1),
				"b": "2",
				"c": map[string]interface{}{
					"d": true,
				},
			},
			expected: &v1beta2.Asset{
				Data: testutils.BuildAny(t, &v1beta2.Group{
					Attributes: TryParseMapToProto(map[string]interface{}{
						"a": 1,
						"b": "2",
						"c": map[string]interface{}{
							"d": true,
						},
					}),
				}),
			},
		},
		{
			name: "Model",
			asset: &v1beta2.Asset{
				Data: testutils.BuildAny(t, &v1beta2.Model{}),
			},
			customFields: map[string]interface{}{
				"a": (float64)(1),
				"b": "2",
				"c": map[string]interface{}{
					"d": true,
				},
			},
			expected: &v1beta2.Asset{
				Data: testutils.BuildAny(t, &v1beta2.Model{
					Attributes: TryParseMapToProto(map[string]interface{}{
						"a": 1,
						"b": "2",
						"c": map[string]interface{}{
							"d": true,
						},
					}),
				}),
			},
		},
		{
			name: "Experiment",
			asset: &v1beta2.Asset{
				Data: testutils.BuildAny(t, &v1beta2.Experiment{}),
			},
			customFields: map[string]interface{}{
				"a": (float64)(1),
				"b": "2",
				"c": map[string]interface{}{
					"d": true,
				},
			},
			expected: &v1beta2.Asset{
				Data: testutils.BuildAny(t, &v1beta2.Experiment{
					Attributes: TryParseMapToProto(map[string]interface{}{
						"a": 1,
						"b": "2",
						"c": map[string]interface{}{
							"d": true,
						},
					}),
				}),
			},
		},
		{
			name: "Metric",
			asset: &v1beta2.Asset{
				Data: testutils.BuildAny(t, &v1beta2.Metric{}),
			},
			customFields: map[string]interface{}{
				"a": (float64)(1),
				"b": "2",
				"c": map[string]interface{}{
					"d": true,
				},
			},
			expected: &v1beta2.Asset{
				Data: testutils.BuildAny(t, &v1beta2.Metric{
					Attributes: TryParseMapToProto(map[string]interface{}{
						"a": 1,
						"b": "2",
						"c": map[string]interface{}{
							"d": true,
						},
					}),
				}),
			},
		},
		{
			name: "Application",
			asset: &v1beta2.Asset{
				Data: testutils.BuildAny(t, &v1beta2.Application{}),
			},
			customFields: map[string]interface{}{
				"a": (float64)(1),
				"b": "2",
				"c": map[string]interface{}{
					"d": true,
				},
			},
			expected: &v1beta2.Asset{
				Data: testutils.BuildAny(t, &v1beta2.Application{
					Attributes: TryParseMapToProto(map[string]interface{}{
						"a": 1,
						"b": "2",
						"c": map[string]interface{}{
							"d": true,
						},
					}),
				}),
			},
		},
		{
			name: "FeatureTable",
			asset: &v1beta2.Asset{
				Data: testutils.BuildAny(t, &v1beta2.FeatureTable{}),
			},
			customFields: map[string]interface{}{
				"a": (float64)(1),
				"b": "2",
				"c": map[string]interface{}{
					"d": true,
				},
			},
			expected: &v1beta2.Asset{
				Data: testutils.BuildAny(t, &v1beta2.FeatureTable{
					Attributes: TryParseMapToProto(map[string]interface{}{
						"a": 1,
						"b": "2",
						"c": map[string]interface{}{
							"d": true,
						},
					}),
				}),
			},
		},
		{
			name: "TableWithAttrs",
			asset: &v1beta2.Asset{
				Data: testutils.BuildAny(t, &v1beta2.Table{
					Attributes: TryParseMapToProto(map[string]interface{}{
						"d": map[string]interface{}{
							"e": true,
						},
					}),
				}),
			},
			customFields: map[string]interface{}{
				"a": 1,
				"b": "2",
				"c": map[string]interface{}{
					"d": true,
				},
			},
			expected: &v1beta2.Asset{
				Data: testutils.BuildAny(t, &v1beta2.Table{
					Attributes: TryParseMapToProto(map[string]interface{}{
						"a": 1,
						"b": "2",
						"c": map[string]interface{}{
							"d": true,
						},
					}),
				}),
			},
		},
		{
			name:        "AssetWithoutData",
			asset:       &v1beta2.Asset{},
			expectedErr: "unmarshal asset data",
		},
		{
			name: "FunkyCustomFields",
			asset: &v1beta2.Asset{
				Data: testutils.BuildAny(t, &v1beta2.Metric{}),
			},
			customFields: map[string]interface{}{
				"unsupported": map[string]string{"test": "fail"},
			},
			expectedErr: "error transforming map into structpb",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			actual, err := SetAttributes(tc.asset, tc.customFields)
			if tc.expectedErr != "" {
				assert.ErrorContains(t, err, tc.expectedErr)
				return
			}

			assert.NoError(t, err)
			testutils.AssertEqualProto(t, tc.expected, actual)
		})
	}
}
