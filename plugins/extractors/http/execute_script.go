package http

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"

	"github.com/d5/tengo/v2"
	"github.com/go-playground/validator/v10"
	"github.com/goto/meteor/models"
	v1beta2 "github.com/goto/meteor/models/gotocompany/assets/v1beta2"
	"github.com/goto/meteor/plugins"
	"github.com/goto/meteor/plugins/internal/tengoutil"
	"github.com/goto/meteor/plugins/internal/tengoutil/structmap"
	"github.com/mcuadros/go-defaults"
	"google.golang.org/protobuf/proto"
)

func (e *Extractor) executeScript(ctx context.Context, res interface{}, emit plugins.Emit) error {
	scriptCfg := e.config.Script
	s, err := tengoutil.NewSecureScript(
		([]byte)(scriptCfg.Source), e.scriptGlobals(ctx, res, emit),
	)
	if err != nil {
		return err
	}

	s.SetMaxAllocs(scriptCfg.MaxAllocs)
	s.SetMaxConstObjects(scriptCfg.MaxConstObjects)

	c, err := s.Compile()
	if err != nil {
		return fmt.Errorf("compile: %w", err)
	}

	if err := c.RunContext(ctx); err != nil && !errors.Is(err, errUserExit) {
		return fmt.Errorf("run: %w", err)
	}

	return nil
}

func (e *Extractor) scriptGlobals(ctx context.Context, res interface{}, emit plugins.Emit) map[string]interface{} {
	return map[string]interface{}{
		"recipe_scope": &tengo.String{Value: e.UrnScope},
		"response":     res,
		"new_asset": &tengo.UserFunction{
			Name:  "new_asset",
			Value: newAssetWrapper(),
		},
		"emit": &tengo.UserFunction{
			Name:  "emit",
			Value: emitWrapper(emit),
		},
		"execute_request": &tengo.UserFunction{
			Name:  "execute_request",
			Value: executeRequestWrapper(ctx, e.config.Concurrency, e.executeRequest),
		},
		"exit": &tengo.UserFunction{
			Name: "exit",
			Value: func(...tengo.Object) (tengo.Object, error) {
				return nil, errUserExit
			},
		},
	}
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

func executeRequestWrapper(ctx context.Context, concurrency int, executeRequest executeRequestFunc) tengo.CallableFunc {
	type job struct {
		i      int
		reqCfg RequestConfig
	}
	requestsChan := func(ctx context.Context, reqs []RequestConfig) <-chan job {
		ch := make(chan job)

		go func() {
			defer close(ch)

			for i, r := range reqs {
				select {
				case <-ctx.Done():
					return

				case ch <- job{i, r}:
				}
			}
		}()

		return ch
	}

	type result struct {
		resp interface{}
		err  error
	}
	processJobs := func(ctx context.Context, n int, ch <-chan job) []result {
		var wg sync.WaitGroup
		wg.Add(concurrency)

		results := make([]result, n)
		work := func() {
			defer wg.Done()

			for {
				select {
				case <-ctx.Done():
					return

				case j, ok := <-ch:
					if !ok {
						return
					}

					resp, err := executeRequest(ctx, j.reqCfg)
					if err != nil {
						results[j.i] = result{err: fmt.Errorf("execute request #%d: %w", j.i, err)}
						continue
					}

					results[j.i] = result{resp: resp}
				}
			}
		}

		for i := 0; i < concurrency; i++ {
			go work()
		}

		wg.Wait()
		return results
	}

	validate := validator.New()
	validate.RegisterTagNameFunc(func(fld reflect.StructField) string {
		name := strings.SplitN(fld.Tag.Get("mapstructure"), ",", 2)[0]
		if name == "-" {
			return ""
		}
		return name
	})
	return func(args ...tengo.Object) (tengo.Object, error) {
		if len(args) < 1 {
			return nil, tengo.ErrWrongNumArguments
		}

		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		reqs, err := argsToRequestConfigs(args, validate)
		if err != nil {
			return nil, fmt.Errorf("execute request: %w", err)
		}

		results := processJobs(ctx, len(reqs), requestsChan(ctx, reqs))

		var ret tengo.Array
		for i, res := range results {
			if res.err != nil {
				ret.Value = append(ret.Value, &tengo.Error{
					Value: &tengo.Map{
						Value: map[string]tengo.Object{
							"request": args[i],
							"error":   &tengo.String{Value: res.err.Error()},
						},
					},
				})
				continue
			}

			o, err := tengo.FromInterface(res.resp)
			if err != nil {
				return nil, fmt.Errorf("execute request: translate response: %s: %w", args[i], err)
			}

			ret.Value = append(ret.Value, o)
		}

		return &ret, nil
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

func argsToRequestConfigs(args []tengo.Object, validate *validator.Validate) ([]RequestConfig, error) {
	reqs := make([]RequestConfig, 0, len(args))
	for _, arg := range args {
		var r RequestConfig
		defaults.SetDefaults(&r)
		if err := structmap.AsStructWithTag("mapstructure", tengo.ToInterface(arg), &r); err != nil {
			return nil, fmt.Errorf("map arg to request config: %s: %w", arg, err)
		}

		if err := validate.Struct(r); err != nil {
			return nil, fmt.Errorf("validate request config: %s, %w", arg, err)
		}

		reqs = append(reqs, r)
	}
	return reqs, nil
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
