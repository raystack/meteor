package api

import (
	"errors"

	"github.com/go-openapi/loads"
	"github.com/go-openapi/spec"
	"github.com/go-openapi/strfmt"
	val "github.com/go-openapi/validate"
)

func validate(schemaName string, data interface{}) (err error) {
	schema, err := getSchema(schemaName)
	if err != nil {
		return err
	}

	return val.AgainstSchema(&schema, data, strfmt.Default)
}

func getSchema(schemaName string) (schema spec.Schema, err error) {
	doc, err := loads.Spec("./swagger.yaml")
	if err != nil {
		return
	}

	schema, ok := doc.Spec().Definitions[schemaName]
	if !ok {
		return schema, errors.New("could not find schema for validating.")
	}

	return
}
