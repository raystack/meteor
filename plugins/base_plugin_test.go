//go:build plugins
// +build plugins

package plugins_test

import (
	"testing"

	"github.com/goto/meteor/plugins"
	"github.com/stretchr/testify/assert"
)

func TestNewBasePlugin(t *testing.T) {
	t.Run("should assign info and return base plugin", func(t *testing.T) {
		info := plugins.Info{
			Description:  "test-description",
			SampleConfig: "sample-config",
			Summary:      "test-summary",
			Tags:         []string{"test", "plugin"},
		}
		actual := plugins.NewBasePlugin(info, nil)

		assert.Equal(t, info, actual.Info())
	})
}

func TestBasePluginInfo(t *testing.T) {
	t.Run("should return info", func(t *testing.T) {
		info := plugins.Info{
			Description:  "test-description",
			SampleConfig: "sample-config",
			Summary:      "test-summary",
			Tags:         []string{"test", "plugin"},
		}
		basePlugin := plugins.NewBasePlugin(info, nil)
		actual := basePlugin.Info()

		assert.Equal(t, info, actual)
	})
}

func TestBasePluginValidate(t *testing.T) {
	t.Run("should not return error if config is nil", func(t *testing.T) {
		basePlugin := plugins.NewBasePlugin(plugins.Info{}, nil)
		err := basePlugin.Validate(plugins.Config{
			URNScope:  "test-scope",
			RawConfig: map[string]interface{}{},
		})

		assert.NoError(t, err)
	})

	t.Run("should return InvalidConfigError if config is invalid", func(t *testing.T) {
		invalidConfig := struct {
			FieldA string `mapstructure:"field_a" validate:"required"`
			FieldB string `mapstructure:"field_b" validate:"url"`
			Nested struct {
				FieldC string `mapstructure:"field_c" validate:"required"`
			} `mapstructure:"nested"`
		}{}

		basePlugin := plugins.NewBasePlugin(plugins.Info{}, &invalidConfig)
		err := basePlugin.Validate(plugins.Config{
			URNScope:  "test-scope",
			RawConfig: map[string]interface{}{},
		})

		assert.Equal(t, err, plugins.InvalidConfigError{
			Errors: []plugins.ConfigError{
				{Key: "field_a", Message: "validation for field 'field_a' failed on the 'required' tag"},
				{Key: "field_b", Message: "validation for field 'field_b' failed on the 'url' tag"},
				{Key: "nested.field_c", Message: "validation for field 'nested.field_c' failed on the 'required' tag"},
			},
		})
	})

	t.Run("should return no error if config is valid", func(t *testing.T) {
		validConfig := struct {
			FieldA string `validate:"required"`
		}{}

		basePlugin := plugins.NewBasePlugin(plugins.Info{}, &validConfig)
		err := basePlugin.Validate(plugins.Config{
			URNScope: "test-scope",
			RawConfig: map[string]interface{}{
				"FieldA": "test-value",
			},
		})

		assert.NoError(t, err)
	})
}

func TestBasePluginInit(t *testing.T) {
	t.Run("should return InvalidConfigError if config is invalid", func(t *testing.T) {
		invalidConfig := struct {
			FieldA string `validate:"required"`
		}{}

		basePlugin := plugins.NewBasePlugin(plugins.Info{}, &invalidConfig)
		err := basePlugin.Validate(plugins.Config{
			URNScope:  "test-scope",
			RawConfig: map[string]interface{}{},
		})

		assert.ErrorAs(t, err, &plugins.InvalidConfigError{})
	})

	t.Run("should return populate config and return no error if config is valid", func(t *testing.T) {
		validConfig := struct {
			FieldA string `validate:"required"`
		}{}

		basePlugin := plugins.NewBasePlugin(plugins.Info{}, &validConfig)
		err := basePlugin.Validate(plugins.Config{
			URNScope: "test-scope",
			RawConfig: map[string]interface{}{
				"FieldA": "test-value",
			},
		})

		assert.NoError(t, err)
		assert.Equal(t, "test-value", validConfig.FieldA)
	})
}
