package utils

import (
	"errors"
	"reflect"
	"strings"

	"github.com/go-playground/validator/v10"
	"github.com/mcuadros/go-defaults"
	"github.com/mitchellh/mapstructure"
	"github.com/odpf/meteor/plugins"
)

var validate *validator.Validate

func init() {
	validate = validator.New()
	validate.RegisterTagNameFunc(func(fld reflect.StructField) string {
		configName := strings.SplitN(fld.Tag.Get("mapstructure"), ",", 2)[0]

		if configName == "-" {
			return ""
		}
		return configName
	})
}

// BuildConfig builds a config struct from a map
func BuildConfig(configMap map[string]interface{}, c interface{}) error {
	defaults.SetDefaults(c)

	if err := mapstructure.Decode(configMap, c); err != nil {
		return plugins.InvalidConfigError{}
	}

	var configErrors []plugins.ConfigError
	if err := validate.Struct(c); err != nil {
		if errors.As(err, &validator.ValidationErrors{}) {
			validationErrors := err.(validator.ValidationErrors)
			for _, fieldErr := range validationErrors {
				key := strings.TrimPrefix(fieldErr.Namespace(), "Config.")
				configErrors = append(configErrors, plugins.ConfigError{
					Key:     key,
					Message: fieldErr.Error(),
				})
			}
		}
	}

	icErr := plugins.InvalidConfigError{
		Errors: configErrors,
	}
	return icErr
}
