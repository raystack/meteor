package structmap

import (
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/mitchellh/mapstructure"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
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
	dec, err := mapstructure.NewDecoder(&mapstructure.DecoderConfig{
		DecodeHook: mapstructure.ComposeDecodeHookFunc(
			stringToTimestampHookFunc(time.RFC3339),
			timeToTimestampHookFunc(),
			mapstructure.StringToTimeHookFunc(time.RFC3339),
			mapToAttributesHookFunc(),
		),
		WeaklyTypedInput: true,
		ErrorUnused:      true,
		ZeroFields:       true,
		Result:           output,
		TagName:          "json",
	})
	if err != nil {
		return fmt.Errorf("structmap: decode into %T: create decoder: %w", output, err)
	}

	if err := dec.Decode(input); err != nil {
		return fmt.Errorf("structmap: decode into %T: %w", output, err)
	}

	return nil
}

// stringToTimestampHookFunc returns a DecodeHookFunc that converts
// strings to timestamppb.Timestamp.
func stringToTimestampHookFunc(layout string) mapstructure.DecodeHookFuncType {
	return func(_ reflect.Type, t reflect.Type, data interface{}) (interface{}, error) {
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
			return nil, fmt.Errorf("structmap: mapstructure string to timestamp hook: %w", err)
		}

		return timestamppb.New(ts), nil
	}
}

func timeToTimestampHookFunc() mapstructure.DecodeHookFuncType {
	return func(_ reflect.Type, t reflect.Type, data interface{}) (interface{}, error) {
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
	return func(_ reflect.Type, t reflect.Type, data interface{}) (interface{}, error) {
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
