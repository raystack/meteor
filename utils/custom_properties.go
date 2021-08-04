package utils

import (
	"github.com/odpf/meteor/proto/odpf/meta"
	"github.com/odpf/meteor/proto/odpf/meta/facets"
)

func GetCustomProperties(data interface{}) map[string]string {
	var customFacet *facets.Custom
	switch data := data.(type) {
	case meta.Table:
		customFacet = data.Custom
	case meta.Topic:
		customFacet = data.Custom
	case meta.Dashboard:
		customFacet = data.Custom
	default:
		// skip process if data's type is not defined
		return nil
	}

	// if data's custom facet is nil, return new empty custom properties
	if customFacet == nil {
		return make(map[string]string)
	}

	// return a new copy to ensure immutability
	return copyCustomProperties(customFacet.CustomProperties)
}

func SetCustomProperties(data interface{}, customProps map[string]string) interface{} {
	switch data := data.(type) {
	case meta.Table:
		data.Custom = createOrGetCustomFacet(data.Custom)
		data.Custom.CustomProperties = customProps
		return data
	case meta.Topic:
		data.Custom = createOrGetCustomFacet(data.Custom)
		data.Custom.CustomProperties = customProps
		return data
	case meta.Dashboard:
		data.Custom = createOrGetCustomFacet(data.Custom)
		data.Custom.CustomProperties = customProps
		return data
	}

	return data
}

func createOrGetCustomFacet(facet *facets.Custom) *facets.Custom {
	if facet == nil {
		return &facets.Custom{
			CustomProperties: make(map[string]string),
		}
	}

	return facet
}

func copyCustomProperties(src map[string]string) (dest map[string]string) {
	if src == nil {
		return
	}

	dest = make(map[string]string)
	for k, v := range src {
		dest[k] = v
	}

	return
}
