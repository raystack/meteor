package api

import (
	"encoding/json"
	"errors"
	"io"

	"github.com/go-openapi/loads"
	"github.com/go-openapi/spec"
	"github.com/go-openapi/strfmt"
	val "github.com/go-openapi/validate"
	"github.com/mitchellh/mapstructure"
)

func decodeAndValidate(reader io.Reader, schemaName string, model interface{}) (err error) {
	var payload map[string]interface{}
	err = json.NewDecoder(reader).Decode(&payload)
	if err != nil {
		return errors.New("invalid json format")
	}
	err = validate(schemaName, payload)
	if err != nil {
		return
	}
	err = mapPayloadToStruct(payload, model)
	if err != nil {
		return
	}

	return
}

func validate(schemaName string, data interface{}) (err error) {
	schema, err := getSchema(schemaName)
	if err != nil {
		return err
	}

	return val.AgainstSchema(&schema, data, strfmt.Default)
}

func mapPayloadToStruct(input map[string]interface{}, model interface{}) (err error) {
	cfg := &mapstructure.DecoderConfig{
		Metadata: nil,
		Result:   model,
		TagName:  "json",
	}
	decoder, err := mapstructure.NewDecoder(cfg)
	if err != nil {
		return
	}
	err = decoder.Decode(input)
	if err != nil {
		return
	}

	return
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
