package utils

import (
	"github.com/odpf/meteor/proto/odpf/entities/facets"
	"github.com/odpf/meteor/proto/odpf/entities/resources"
	"google.golang.org/protobuf/types/known/structpb"
)

func GetCustomProperties(data interface{}) map[string]interface{} {
	var customProps *facets.Properties
	switch data := data.(type) {
	case resources.Table:
		customProps = data.Properties
	case resources.Topic:
		customProps = data.Properties
	case resources.Dashboard:
		customProps = data.Properties
	default:
		// skip process if data's type is not defined
		return nil
	}

	// if data's custom facet is nil, return new empty custom properties
	if customProps == nil {
		return make(map[string]interface{})
	}

	// return custom fields as map
	return parseToMap(customProps.Fields)
}

func SetCustomProperties(data interface{}, customFields map[string]interface{}) (res interface{}, err error) {
	protoStruct, err := parseMapToProto(customFields)
	if err != nil {
		return
	}

	switch data := data.(type) {
	case resources.Table:
		data.Properties = createOrGetCustomFacet(data.Properties)
		data.Properties.Fields = protoStruct
		res = data
	case resources.Topic:
		data.Properties = createOrGetCustomFacet(data.Properties)
		data.Properties.Fields = protoStruct
		res = data
	case resources.Dashboard:
		data.Properties = createOrGetCustomFacet(data.Properties)
		data.Properties.Fields = protoStruct
		res = data
	default:
		res = data
	}

	return
}

func createOrGetCustomFacet(facet *facets.Properties) *facets.Properties {
	if facet == nil {
		return &facets.Properties{
			Fields: &structpb.Struct{},
		}
	}

	return facet
}

func parseToMap(src *structpb.Struct) map[string]interface{} {
	if src == nil {
		return nil
	}

	return src.AsMap()
}

func parseMapToProto(src map[string]interface{}) (*structpb.Struct, error) {
	if src == nil {
		return nil, nil
	}

	return structpb.NewStruct(src)
}

func TryParseMapToProto(src map[string]interface{}) *structpb.Struct {
	res, err := parseMapToProto(src)
	if err != nil {
		panic(err)
	}

	return res
}
