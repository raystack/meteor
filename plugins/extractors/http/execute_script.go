package http

import (
	"context"
	"errors"
	"fmt"

	"github.com/d5/tengo/v2"
	"github.com/odpf/meteor/models"
	v1beta2 "github.com/odpf/meteor/models/odpf/assets/v1beta2"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/plugins/internal/tengoutil"
	"github.com/odpf/meteor/plugins/internal/tengoutil/structmap"
	"google.golang.org/protobuf/proto"
)

func (e *Extractor) executeScript(ctx context.Context, res interface{}, emit plugins.Emit) error {
	s := tengoutil.NewSecureScript(([]byte)(e.config.Script.Source))
	if err := declareGlobals(s, res, emit); err != nil {
		return fmt.Errorf("declare globals: %w", err)
	}

	c, err := s.Compile()
	if err != nil {
		return fmt.Errorf("compile: %w", err)
	}

	if err := c.RunContext(ctx); err != nil && !errors.Is(err, errUserExit) {
		return fmt.Errorf("run: %w", err)
	}

	return nil
}

func declareGlobals(s *tengo.Script, res interface{}, emit plugins.Emit) error {
	for name, v := range map[string]interface{}{
		"response": res,
		"new_asset": &tengo.UserFunction{
			Name:  "new_asset",
			Value: newAssetWrapper(),
		},
		"emit": &tengo.UserFunction{
			Name:  "emit",
			Value: emitWrapper(emit),
		},
		"exit": &tengo.UserFunction{
			Name: "exit",
			Value: func(...tengo.Object) (tengo.Object, error) {
				return nil, errUserExit
			},
		},
	} {
		if err := s.Add(name, v); err != nil {
			return fmt.Errorf("declare script globals: %w", err)
		}
	}
	return nil
}

func newAssetWrapper() tengo.CallableFunc {
	typeURLs := knownTypeURLs()
	return func(args ...tengo.Object) (tengo.Object, error) {
		if len(args) != 1 {
			return nil, tengo.ErrWrongNumArguments
		}

		typ, ok := tengo.ToString(args[0])
		if !ok {
			return nil, tengo.ErrInvalidArgumentType{
				Name:     "typ",
				Expected: "string(compatible)",
				Found:    args[0].TypeName(),
			}
		}

		return newAsset(typeURLs, typ)
	}
}

func emitWrapper(emit plugins.Emit) tengo.CallableFunc {
	return func(args ...tengo.Object) (tengo.Object, error) {
		if len(args) != 1 {
			return nil, tengo.ErrWrongNumArguments
		}

		m, ok := tengo.ToInterface(args[0]).(map[string]interface{})
		if !ok {
			return nil, tengo.ErrInvalidArgumentType{
				Name:     "asset",
				Expected: "Map",
				Found:    args[0].TypeName(),
			}
		}

		var ast v1beta2.Asset
		if err := structmap.AsStruct(m, &ast); err != nil {
			return nil, fmt.Errorf("emit asset: %w", err)
		}

		emit(models.NewRecord(&ast))

		return tengo.UndefinedValue, nil
	}
}

func newAsset(typeURLs map[string]string, typ string) (tengo.Object, error) {
	u, ok := typeURLs[typ]
	if !ok {
		return nil, fmt.Errorf("new asset: unexpected type: %s", typ)
	}

	return &tengo.Map{
		Value: map[string]tengo.Object{
			"type": &tengo.String{Value: typ},
			"data": &tengo.Map{
				Value: map[string]tengo.Object{
					"@type": &tengo.String{Value: u},
				},
			},
		},
	}, nil
}

func knownTypeURLs() map[string]string {
	typeURLs := make(map[string]string, 12)
	for _, typ := range []string{
		"bucket", "dashboard", "experiment", "feature_table", "group",
		"job", "metric", "model", "application", "table", "topic", "user",
	} {
		typeURLs[typ] = typeURL(typ)
	}
	return typeURLs
}

func typeURL(typ string) string {
	const prefix = "type.googleapis.com/"

	var msg proto.Message
	switch typ {
	case "bucket":
		msg = &v1beta2.Bucket{}
	case "dashboard":
		msg = &v1beta2.Dashboard{}
	case "experiment":
		msg = &v1beta2.Experiment{}
	case "feature_table":
		msg = &v1beta2.FeatureTable{}
	case "group":
		msg = &v1beta2.Group{}
	case "job":
		msg = &v1beta2.Job{}
	case "metric":
		msg = &v1beta2.Metric{}
	case "model":
		msg = &v1beta2.Model{}
	case "application":
		msg = &v1beta2.Application{}
	case "table":
		msg = &v1beta2.Table{}
	case "topic":
		msg = &v1beta2.Topic{}
	case "user":
		msg = &v1beta2.User{}
	default:
		panic(fmt.Errorf("unexpected type name: %s", typ))
	}

	return prefix + (string)(msg.ProtoReflect().Descriptor().FullName())
}
