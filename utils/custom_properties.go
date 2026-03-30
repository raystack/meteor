package utils

import (
	meteorv1beta1 "github.com/raystack/meteor/models/raystack/meteor/v1beta1"
	"google.golang.org/protobuf/types/known/structpb"
)

// GetAttributes returns custom properties of the given entity.
func GetAttributes(entity *meteorv1beta1.Entity) map[string]interface{} {
	if entity.GetProperties() == nil {
		return make(map[string]interface{})
	}

	return entity.GetProperties().AsMap()
}

// SetAttributes sets custom properties of the given entity.
func SetAttributes(entity *meteorv1beta1.Entity, customFields map[string]interface{}) (*meteorv1beta1.Entity, error) {
	newProps, err := structpb.NewStruct(customFields)
	if err != nil {
		return nil, err
	}

	entity.Properties = newProps
	return entity, nil
}

