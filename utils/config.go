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

	if err = mapstructure.Decode(configMap, c); err != nil {
		return err
	}
	if err = validate.Struct(c); err != nil {
		return err
	}

	return
}
