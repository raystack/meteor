//go:build plugins
// +build plugins

package http

import (
	"testing"

	"github.com/d5/tengo/v2"
	v1beta2 "github.com/goto/meteor/models/gotocompany/assets/v1beta2"
	"github.com/goto/meteor/plugins/internal/tengoutil/structmap"
	testutils "github.com/goto/meteor/test/utils"
	"github.com/stretchr/testify/assert"
)

func TestNewAsset(t *testing.T) {
	cases := []struct {
		typ      string
		expected *v1beta2.Asset
	}{
		{
			typ: "bucket",
			expected: &v1beta2.Asset{
				Type: "bucket",
				Data: testutils.BuildAny(t, &v1beta2.Bucket{}),
			},
		},
		{
			typ: "dashboard",
			expected: &v1beta2.Asset{
				Type: "dashboard",
				Data: testutils.BuildAny(t, &v1beta2.Dashboard{}),
			},
		},
		{
			typ: "experiment",
			expected: &v1beta2.Asset{
				Type: "experiment",
				Data: testutils.BuildAny(t, &v1beta2.Experiment{}),
			},
		},
		{
			typ: "feature_table",
			expected: &v1beta2.Asset{
				Type: "feature_table",
				Data: testutils.BuildAny(t, &v1beta2.FeatureTable{}),
			},
		},
		{
			typ: "group",
			expected: &v1beta2.Asset{
				Type: "group",
				Data: testutils.BuildAny(t, &v1beta2.Group{}),
			},
		},
		{
			typ: "job",
			expected: &v1beta2.Asset{
				Type: "job",
				Data: testutils.BuildAny(t, &v1beta2.Job{}),
			},
		},
		{
			typ: "metric",
			expected: &v1beta2.Asset{
				Type: "metric",
				Data: testutils.BuildAny(t, &v1beta2.Metric{}),
			},
		},
		{
			typ: "model",
			expected: &v1beta2.Asset{
				Type: "model",
				Data: testutils.BuildAny(t, &v1beta2.Model{}),
			},
		},
		{
			typ: "application",
			expected: &v1beta2.Asset{
				Type: "application",
				Data: testutils.BuildAny(t, &v1beta2.Application{}),
			},
		},
		{
			typ: "table",
			expected: &v1beta2.Asset{
				Type: "table",
				Data: testutils.BuildAny(t, &v1beta2.Table{}),
			},
		},
		{
			typ: "topic",
			expected: &v1beta2.Asset{
				Type: "topic",
				Data: testutils.BuildAny(t, &v1beta2.Topic{}),
			},
		},
		{
			typ: "user",
			expected: &v1beta2.Asset{
				Type: "user",
				Data: testutils.BuildAny(t, &v1beta2.User{}),
			},
		},
	}
	for _, tc := range cases {
		typeURLs := knownTypeURLs()
		t.Run(tc.typ, func(t *testing.T) {
			obj, err := newAsset(typeURLs, tc.typ)
			assert.NoError(t, err)

			var ast *v1beta2.Asset
			err = structmap.AsStruct(tengo.ToInterface(obj), &ast)
			assert.NoError(t, err)

			testutils.AssertEqualProto(t, tc.expected, ast)
		})
	}
}
