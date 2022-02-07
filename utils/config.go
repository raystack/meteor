package utils

import (
	"errors"
	"fmt"
	"github.com/go-playground/validator/v10"
	"github.com/mcuadros/go-defaults"
	"github.com/mitchellh/mapstructure"
	"github.com/odpf/meteor/plugins"
	"reflect"
	"strings"
)

var validate *validator.Validate

func init() {
	validate = validator.New()
}

// BuildConfig builds a config struct from a map
func BuildConfig(configMap map[string]interface{}, c interface{}) plugins.InvalidConfigError {
	defaults.SetDefaults(c)

	if err := mapstructure.Decode(configMap, c); err != nil {
		return plugins.InvalidConfigError{}
	}
	if err := validate.Struct(c); err != nil {
		var fErr plugins.InvalidConfigError
		var typeError2 validator.ValidationErrors
		if errors.As(err, &typeError2) {
			//fmt.Println("hello2")
			validate = validator.New()
			validate.RegisterTagNameFunc(func(fld reflect.StructField) string {
				name := strings.SplitN(fld.Tag.Get("mapstructure"), ",", 2)[0]

				if name == "-" {
					return ""
				}
				return name
			})
			validateErr := validate.Struct(c)
			fieldErrs := validateErr.(validator.ValidationErrors)
			for _, fieldErr := range fieldErrs {
				key := strings.TrimPrefix(fieldErr.Namespace(), "Config.")
				fmt.Println("key: ", key)
				fmt.Println("e: ", fieldErr)

				fErr = plugins.InvalidConfigError{Key: key}
				//errs = append(errs, err)
				//fmt.Println("err:", err)
				//fmt.Println("Config:", e.Namespace(), "Key:", e.StructNamespace())
			}
		}
		fmt.Println("Last Err: ", err)
		return fErr
	}
	return plugins.InvalidConfigError{}
}

// BuildConfig builds a config struct from a map
func BuildConfigTest(configMap map[string]interface{}, c interface{}) (errs []error) {
	defaults.SetDefaults(c)

	if err := mapstructure.Decode(configMap, c); err != nil {
		errs = append(errs, err)
	}
	if err := validate.Struct(c); err != nil {
		//var errs error
		var typeError2 validator.ValidationErrors
		if errors.As(err, &typeError2) {
			//fmt.Println("hello2")
			validate = validator.New()
			validate.RegisterTagNameFunc(func(fld reflect.StructField) string {
				name := strings.SplitN(fld.Tag.Get("mapstructure"), ",", 2)[0]

				if name == "-" {
					return ""
				}
				return name
			})
			validateErr := validate.Struct(c)
			fieldErrs := validateErr.(validator.ValidationErrors)
			fmt.Println("fieldErrs:", fieldErrs)
			for _, fieldErr := range fieldErrs {
				key := strings.TrimPrefix(fieldErr.Namespace(), "Config.")
				fmt.Println("key: ", key)
				fmt.Println("e: ", fieldErr)

				err = plugins.InvalidConfigError{Key: key}
				errs = append(errs, err)
				//fmt.Println("err:", err)
				//fieldErr := &plugins.InvalidConfigError{Key: key}
				//fmt.Println("Config:", e.Namespace(), "Key:", e.StructNamespace())
			}
		}
		//fmt.Println("Last Err: ", err)
		return errs
	}
	return
}
