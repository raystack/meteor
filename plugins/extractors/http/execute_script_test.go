//go:build plugins
// +build plugins

package http

import (
	"testing"

	"github.com/d5/tengo/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewEntity(t *testing.T) {
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
		{typ: "custom_type"},
	}
	for _, tc := range cases {
		t.Run(tc.typ, func(t *testing.T) {
			obj, err := newEntity(tc.typ)
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

func TestNewEntityEmptyType(t *testing.T) {
	_, err := newEntity("")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "type must not be empty")
}
