package utils

import (
	"github.com/odpf/meteor/proto/odpf/assets"
	"github.com/odpf/meteor/proto/odpf/assets/facets"
	"google.golang.org/protobuf/types/known/structpb"
)

// GetCustomProperties returns custom properties of the given asset
func GetCustomProperties(data interface{}) map[string]interface{} {
	var customProps *facets.Properties
	switch data := data.(type) {
	case assets.Table:
		customProps = data.Properties
	case assets.Topic:
		customProps = data.Properties
	case assets.Dashboard:
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
	return parseToMap(customProps.Attributes)
}

// SetCustomProperties sets custom properties of the given asset
func SetCustomProperties(data interface{}, customFields map[string]interface{}) (res interface{}, err error) {
	protoStruct, err := parseMapToProto(customFields)
	if err != nil {
		return
	}

	switch data := data.(type) {
	case assets.Table:
		data.Properties = createOrGetCustomFacet(data.Properties)
		data.Properties.Attributes = protoStruct
		res = data
	case assets.Topic:
		data.Properties = createOrGetCustomFacet(data.Properties)
		data.Properties.Attributes = protoStruct
		res = data
	case assets.Dashboard:
		data.Properties = createOrGetCustomFacet(data.Properties)
		data.Properties.Attributes = protoStruct
		res = data
	default:
		res = data
	}

	return
}

func createOrGetCustomFacet(facet *facets.Properties) *facets.Properties {
	if facet == nil {
		return &facets.Properties{
			Attributes: &structpb.Struct{},
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

// TryParseMapToProto parses given map to proto struct
func TryParseMapToProto(src map[string]interface{}) *structpb.Struct {
	res, err := parseMapToProto(src)
	if err != nil {
		panic(err)
	}

	return res
}
