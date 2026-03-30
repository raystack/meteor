//go:build plugins
// +build plugins

package http

import (
	"testing"

	"github.com/d5/tengo/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewAsset(t *testing.T) {
	cases := []struct {
		typ string
	}{
		{typ: "bucket"},
		{typ: "dashboard"},
		{typ: "experiment"},
		{typ: "feature_table"},
		{typ: "group"},
		{typ: "job"},
		{typ: "metric"},
		{typ: "model"},
		{typ: "application"},
		{typ: "table"},
		{typ: "topic"},
		{typ: "user"},
	}
	for _, tc := range cases {
		knownTypes := knownEntityTypes()
		t.Run(tc.typ, func(t *testing.T) {
			obj, err := newAsset(knownTypes, tc.typ)
			require.NoError(t, err)

			m, ok := obj.(*tengo.Map)
			require.True(t, ok, "expected *tengo.Map")

			typObj, ok := m.Value["type"]
			require.True(t, ok, "expected 'type' key in map")

			typStr, ok := typObj.(*tengo.String)
			require.True(t, ok, "expected type to be *tengo.String")
			assert.Equal(t, tc.typ, typStr.Value)
		})
	}
}
