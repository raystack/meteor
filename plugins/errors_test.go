//go:build plugins
// +build plugins

package plugins

import (
	"fmt"
	"testing"

	"github.com/MakeNowJust/heredoc"
	"github.com/stretchr/testify/assert"
)

func TestInvalidConfigError(t *testing.T) {
	cases := []struct {
		name     string
		err      InvalidConfigError
		expected string
		hasError bool
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
			hasError: true,
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
			hasError: true,
		},
		{
			name: "WithoutErrors",
			err: InvalidConfigError{
				Type:       "extractor",
				PluginName: "caramlstore",
			},
			expected: "invalid extractor config",
			hasError: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, tc.err.Error())
			assert.Equal(t, tc.hasError, tc.err.HasError())
		})
	}
}

func TestNotFoundError(t *testing.T) {
	type fields struct {
		Type PluginType
		Name string
	}
	tests := []struct {
		name     string
		fields   fields
		expected string
	}{
		{
			name: "WithType",
			fields: fields{
				Type: "extractor",
				Name: "caramlstore",
			},
			expected: "could not find extractor \"caramlstore\"",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := NotFoundError{
				Type: tt.fields.Type,
				Name: tt.fields.Name,
			}
			assert.EqualError(t, err, tt.expected)
		})
	}
}

func TestRetryError(t *testing.T) {
	type fields struct {
		Err error
	}
	tests := []struct {
		name              string
		fields            fields
		expected          string
		expectedErr       error
		expectedUnwrapped error
	}{
		{
			name: "WithRetryError",
			fields: fields{
				Err: fmt.Errorf("some error"),
			},
			expected:          "some error",
			expectedErr:       RetryError{Err: fmt.Errorf("some error")},
			expectedUnwrapped: fmt.Errorf("some error"),
		},
		{
			name: "WithNilError",
			fields: fields{
				Err: nil,
			},
			expectedErr:       nil,
			expectedUnwrapped: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := NewRetryError(tt.fields.Err)
			assert.Equal(t, e, tt.expectedErr, e)
			if e != nil {
				assert.Equal(t, tt.expected, e.Error())
				assert.Equal(t, tt.expectedUnwrapped, e.(RetryError).Unwrap())
			}
		})
	}
}
