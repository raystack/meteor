//go:build plugins
// +build plugins

package plugins

import (
	"testing"

	"github.com/MakeNowJust/heredoc"
	"github.com/stretchr/testify/assert"
)

func TestInvalidConfigError(t *testing.T) {
	cases := []struct {
		name     string
		err      InvalidConfigError
		expected string
	}{
		{
			name: "WithType",
			err: InvalidConfigError{
				Type:       "extractor",
				PluginName: "caramlstore",
				Errors: []ConfigError{
					{Key: "engine", Message: "validation for field 'engine' failed on the 'oneof' tag"},
					{Key: "script", Message: "validation for field 'script' failed on the 'required' tag"},
				},
			},
			expected: heredoc.Doc(`
				invalid extractor config:
					 * validation for field 'engine' failed on the 'oneof' tag
					 * validation for field 'script' failed on the 'required' tag`),
		},
		{
			name: "WithoutType",
			err: InvalidConfigError{
				PluginName: "caramlstore",
				Errors: []ConfigError{
					{Key: "engine", Message: "validation for field 'engine' failed on the 'oneof' tag"},
				},
			},
			expected: heredoc.Doc(`
				invalid config:
					 * validation for field 'engine' failed on the 'oneof' tag`),
		},
		{
			name: "WithoutErrors",
			err: InvalidConfigError{
				Type:       "extractor",
				PluginName: "caramlstore",
			},
			expected: "invalid extractor config",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, tc.err.Error())
		})
	}
}
