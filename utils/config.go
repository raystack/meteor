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

func BuildConfig(configMap map[string]interface{}, c interface{}) (err error) {
	err = mapstructure.Decode(configMap, c)
	if err != nil {
		return err
	}

	defaults.SetDefaults(c)

	err = validate.Struct(c)
	if err != nil {
		return err
	}

	return
}
