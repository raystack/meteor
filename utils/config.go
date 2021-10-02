package utils

import (
	"github.com/go-playground/validator/v10"
	"github.com/mcuadros/go-defaults"
	"github.com/mitchellh/mapstructure"
)

var validate *validator.Validate

func init() {
	validate = validator.New()
}

// BuildConfig builds a config struct from a map
func BuildConfig(configMap map[string]interface{}, c interface{}) (err error) {
	defaults.SetDefaults(c)

	err = mapstructure.Decode(configMap, c)
	if err != nil {
		return err
	}

	err = validate.Struct(c)
	if err != nil {
		return err
	}

	return
}
