package api

import (
	_ "embed"
	"errors"
	"log"

	"github.com/go-openapi/loads"
	"github.com/go-openapi/spec"
	"github.com/go-openapi/strfmt"
	val "github.com/go-openapi/validate"
)

//go:embed swagger.yaml
var swaggerFile []byte
var swagger *loads.Document

func init() {
	var err error

	version := "" // using library default version
	swagger, err = loads.Analyzed(swaggerFile, version)
	if err != nil {
		log.Fatal(err)
	}
}

func validate(schemaName string, data interface{}) (err error) {
	schema, err := getSchema(schemaName)
	if err != nil {
		return err
	}

	return val.AgainstSchema(&schema, data, strfmt.Default)
}

func getSchema(schemaName string) (schema spec.Schema, err error) {
	schema, ok := swagger.Spec().Definitions[schemaName]
	if !ok {
		return schema, errors.New("could not find schema for validating.")
	}

	return
}
