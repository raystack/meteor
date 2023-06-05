package structmap

import (
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	v1beta2 "github.com/goto/meteor/models/gotocompany/assets/v1beta2"
	"github.com/mitchellh/mapstructure"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func AsMap(v interface{}) (interface{}, error) {
	// Cannot use mapstructure here because of
	// 1. https://github.com/mitchellh/mapstructure/issues/249
	// 2. Handling for fields with type *timestamp.Timestamp
	var (
		data []byte
		err  error
	)
	if m, ok := v.(proto.Message); ok {
		data, err = protojson.MarshalOptions{UseProtoNames: true}.Marshal(m)
	} else {
		data, err = json.Marshal(v)
	}
	if err != nil {
		return nil, fmt.Errorf("structmap: %T as map: marshal: %w", v, err)
	}

	var res interface{}
	if err := json.Unmarshal(data, &res); err != nil {
		return nil, fmt.Errorf("structmap: %T as map: unmarshal: %w", v, err)
	}

	return res, nil
}

func AsStruct(input, output interface{}) error {
	return AsStructWithTag("json", input, output)
}

func AsStructWithTag(tagName string, input, output interface{}) error {
	dec, err := mapstructure.NewDecoder(&mapstructure.DecoderConfig{
		DecodeHook: mapstructure.ComposeDecodeHookFunc(
			checkAssetDataHookFunc(),
			stringToTimestampHookFunc(time.RFC3339),
			timeToTimestampHookFunc(),
			mapstructure.StringToTimeHookFunc(time.RFC3339),
			mapstructure.StringToTimeDurationHookFunc(),
			mapToAttributesHookFunc(),
			mapToAnyPBHookFunc(),
		),
		WeaklyTypedInput: true,
		ErrorUnused:      true,
		ZeroFields:       true,
		Result:           output,
		TagName:          tagName,
	})
	if err != nil {
		return fmt.Errorf("structmap: decode into %T: create decoder: %w", output, err)
	}

	if err := dec.Decode(input); err != nil {
		return fmt.Errorf("structmap: decode as struct: %w", err)
	}

	return nil
}

func checkAssetDataHookFunc() mapstructure.DecodeHookFuncType {
	return func(_, t reflect.Type, data interface{}) (interface{}, error) {
		if t != reflect.TypeOf(v1beta2.Asset{}) && t != reflect.TypeOf(&v1beta2.Asset{}) {
			return data, nil
		}

		m, ok := data.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("mapstructure check asset: unexpected type: %T", data)
		}

		if _, ok := m["data"].(map[string]interface{}); !ok {
			return nil, fmt.Errorf("mapstructure check asset data: unexpected type: %T", m["data"])
		}

		return data, nil
	}
}

// stringToTimestampHookFunc returns a DecodeHookFunc that converts
// strings to timestamppb.Timestamp.
func stringToTimestampHookFunc(layout string) mapstructure.DecodeHookFuncType {
	return func(_, t reflect.Type, data interface{}) (interface{}, error) {
		s, ok := data.(string)
		if !ok {
			return data, nil
		}
		if t != reflect.TypeOf(timestamppb.Timestamp{}) && t != reflect.TypeOf(&timestamppb.Timestamp{}) {
			return data, nil
		}

		// Convert it by parsing
		ts, err := time.Parse(layout, s)
		if err != nil {
			return nil, fmt.Errorf("mapstructure string to timestamp hook: %w", err)
		}

		return timestamppb.New(ts), nil
	}
}

func timeToTimestampHookFunc() mapstructure.DecodeHookFuncType {
	return func(_, t reflect.Type, data interface{}) (interface{}, error) {
		ts, ok := data.(time.Time)
		if !ok {
			return data, nil
		}
		if t != reflect.TypeOf(timestamppb.Timestamp{}) && t != reflect.TypeOf(&timestamppb.Timestamp{}) {
			return data, nil
		}

		return timestamppb.New(ts), nil
	}
}

func mapToAttributesHookFunc() mapstructure.DecodeHookFuncType {
	return func(_, t reflect.Type, data interface{}) (interface{}, error) {
		m, ok := data.(map[string]interface{})
		if !ok {
			return data, nil
		}

		if t != reflect.TypeOf(&structpb.Struct{}) && t != reflect.TypeOf(structpb.Struct{}) {
			return data, nil
		}

		return structpb.NewStruct(m)
	}
}

func mapToAnyPBHookFunc() mapstructure.DecodeHookFuncType {
	failure := func(step string, err error) (interface{}, error) {
		return nil, fmt.Errorf("mapstructure map to anypb hook: %s: %w", step, err)
	}

	return func(_, t reflect.Type, data interface{}) (interface{}, error) {
		m, ok := data.(map[string]interface{})
		if !ok {
			return data, nil
		}

		if t != reflect.TypeOf(anypb.Any{}) && t != reflect.TypeOf(&anypb.Any{}) {
			return data, nil
		}

		typ, ok := m["@type"].(string)
		if !ok {
			return data, nil
		}

		msgtyp, err := protoregistry.GlobalTypes.FindMessageByURL(typ)
		if err != nil {
			return failure("resolve type", err)
		}

		msg := msgtyp.New().Interface()
		delete(m, "@type")
		if err := AsStruct(m, &msg); err != nil {
			return failure("decode", err)
		}

		dataAny, err := anypb.New(msg)
		if err != nil {
			return failure("marshal as any", err)
		}

		return dataAny, nil
	}
}
