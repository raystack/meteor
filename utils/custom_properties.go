package utils

import (
	"fmt"

	v1beta2 "github.com/odpf/meteor/models/odpf/assets/v1beta2"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"
)

// GetAttributes returns custom properties of the given asset
func GetAttributes(asset *v1beta2.Asset) map[string]interface{} {
	dataAny := asset.GetData()

	var table v1beta2.Table
	if err := anypb.UnmarshalTo(dataAny, &table, proto.UnmarshalOptions{}); err == nil {
		return getMap(&table)
	}
	var topic v1beta2.Topic
	if err := anypb.UnmarshalTo(dataAny, &topic, proto.UnmarshalOptions{}); err == nil {
		return getMap(&topic)
	}
	var dashboard v1beta2.Dashboard
	if err := anypb.UnmarshalTo(dataAny, &dashboard, proto.UnmarshalOptions{}); err == nil {
		return getMap(&dashboard)
	}
	var job v1beta2.Job
	if err := anypb.UnmarshalTo(dataAny, &job, proto.UnmarshalOptions{}); err == nil {
		return getMap(&job)
	}
	var user v1beta2.User
	if err := anypb.UnmarshalTo(dataAny, &user, proto.UnmarshalOptions{}); err == nil {
		return getMap(&user)
	}
	var bucket v1beta2.Bucket
	if err := anypb.UnmarshalTo(dataAny, &bucket, proto.UnmarshalOptions{}); err == nil {
		return getMap(&bucket)
	}
	var group v1beta2.Group
	if err := anypb.UnmarshalTo(dataAny, &group, proto.UnmarshalOptions{}); err == nil {
		return getMap(&group)
	}

	return make(map[string]interface{})
}

// SetAttributes sets custom properties of the given asset
func SetAttributes(asset *v1beta2.Asset, customFields map[string]interface{}) (*v1beta2.Asset, error) {
	dataAny := asset.GetData()

	attr, err := structpb.NewStruct(customFields)
	if err != nil {
		return asset, fmt.Errorf("error transforming map into structpb: %w", err)
	}

	var table v1beta2.Table
	if err := anypb.UnmarshalTo(dataAny, &table, proto.UnmarshalOptions{}); err == nil {
		table.Attributes = attr

		data, err := anypb.New(&table)
		if err != nil {
			return asset, fmt.Errorf("error transforming table into anypb: %w", err)
		}
		asset.Data = data

		return asset, nil
	}
	var topic v1beta2.Topic
	if err := anypb.UnmarshalTo(dataAny, &topic, proto.UnmarshalOptions{}); err == nil {
		topic.Attributes = attr

		data, err := anypb.New(&topic)
		if err != nil {
			return asset, fmt.Errorf("error transforming topic into anypb: %w", err)
		}
		asset.Data = data

		return asset, nil
	}
	var dashboard v1beta2.Dashboard
	if err := anypb.UnmarshalTo(dataAny, &dashboard, proto.UnmarshalOptions{}); err == nil {
		dashboard.Attributes = attr

		data, err := anypb.New(&dashboard)
		if err != nil {
			return asset, fmt.Errorf("error transforming table dashboard anypb: %w", err)
		}
		asset.Data = data

		return asset, nil
	}
	var job v1beta2.Job
	if err := anypb.UnmarshalTo(dataAny, &job, proto.UnmarshalOptions{}); err == nil {
		job.Attributes = attr

		data, err := anypb.New(&job)
		if err != nil {
			return asset, fmt.Errorf("error job table into anypb: %w", err)
		}
		asset.Data = data

		return asset, nil
	}
	var user v1beta2.User
	if err := anypb.UnmarshalTo(dataAny, &user, proto.UnmarshalOptions{}); err == nil {
		user.Attributes = attr

		data, err := anypb.New(&user)
		if err != nil {
			return asset, fmt.Errorf("error transforming user into anypb: %w", err)
		}
		asset.Data = data

		return asset, nil
	}
	var bucket v1beta2.Bucket
	if err := anypb.UnmarshalTo(dataAny, &bucket, proto.UnmarshalOptions{}); err == nil {
		bucket.Attributes = attr

		data, err := anypb.New(&bucket)
		if err != nil {
			return asset, fmt.Errorf("error transforming bucket into anypb: %w", err)
		}
		asset.Data = data

		return asset, nil
	}
	var group v1beta2.Group
	if err := anypb.UnmarshalTo(dataAny, &group, proto.UnmarshalOptions{}); err == nil {
		group.Attributes = attr

		data, err := anypb.New(&group)
		if err != nil {
			return asset, fmt.Errorf("error transforming group into anypb: %w", err)
		}
		asset.Data = data

		return asset, nil
	}

	return asset, errors.New("could not find matching model")
}

// TryParseMapToProto parses given map to proto struct
func TryParseMapToProto(src map[string]interface{}) *structpb.Struct {
	res, err := parseMapToProto(src)
	if err != nil {
		panic(err)
	}

	return res
}

type hasAttributes interface {
	GetAttributes() *structpb.Struct
}

func getMap(model hasAttributes) map[string]interface{} {
	attr := model.GetAttributes()
	if attr == nil {
		return make(map[string]interface{})
	}

	return attr.AsMap()
}

func parseMapToProto(src map[string]interface{}) (*structpb.Struct, error) {
	if src == nil {
		return nil, nil
	}

	return structpb.NewStruct(src)
}
