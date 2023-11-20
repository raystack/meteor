package http

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/d5/tengo/v2"
	v1beta2 "github.com/goto/meteor/models/gotocompany/assets/v1beta2"
	"github.com/goto/meteor/plugins/internal/tengoutil"
	"github.com/goto/meteor/plugins/internal/tengoutil/structmap"
)

var errUserExit = errors.New("user exit")

func (s *Sink) executeScript(ctx context.Context, url string, asset *v1beta2.Asset) error {
	scriptCfg := s.config.Script
	script, err := tengoutil.NewSecureScript(
		([]byte)(scriptCfg.Source), s.scriptGlobals(ctx, url),
	)
	if err != nil {
		return err
	}

	c, err := script.Compile()
	if err != nil {
		return fmt.Errorf("compile: %w", err)
	}

	assetMap, err := structmap.AsMap(asset)
	if err != nil {
		return fmt.Errorf("convert asset to map: %w", err)
	}

	if err := c.Set("asset", assetMap); err != nil {
		return fmt.Errorf("set asset into vm: %w", err)
	}

	if err := c.RunContext(ctx); err != nil && !errors.Is(err, errUserExit) {
		return fmt.Errorf("run: %w", err)
	}

	return nil
}

func (s *Sink) scriptGlobals(ctx context.Context, url string) map[string]interface{} {
	return map[string]interface{}{
		"asset": map[string]interface{}{},
		"sink": &tengo.UserFunction{
			Name:  "sink",
			Value: s.executeRequestWrapper(ctx, url),
		},
		"exit": &tengo.UserFunction{
			Name: "exit",
			Value: func(...tengo.Object) (tengo.Object, error) {
				return nil, errUserExit
			},
		},
	}
}

func (s *Sink) executeRequestWrapper(ctx context.Context, url string) tengo.CallableFunc {
	return func(args ...tengo.Object) (tengo.Object, error) {
		if len(args) != 1 {
			return nil, fmt.Errorf("execute request: invalid number of arguments of sink function, expected 1, got %d", len(args))
		}
		payloadObj, ok := args[0].(*tengo.Map)
		if !ok {
			return nil, fmt.Errorf("execute request: invalid type of argument of sink function, expected map, got %T", args[0])
		}
		payload, err := json.Marshal(tengo.ToInterface(payloadObj))
		if err != nil {
			return nil, fmt.Errorf("execute request: marshal payload: %w", err)
		}
		return nil, s.makeRequest(ctx, url, payload)
	}
}
