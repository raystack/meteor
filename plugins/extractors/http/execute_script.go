package http

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"

	"github.com/d5/tengo/v2"
	"github.com/go-playground/validator/v10"
	"github.com/mcuadros/go-defaults"
	"github.com/raystack/meteor/models"
	"github.com/raystack/meteor/plugins"
	"github.com/raystack/meteor/plugins/internal/tengoutil"
	"github.com/raystack/meteor/plugins/internal/tengoutil/structmap"
)

func (e *Extractor) executeScript(ctx context.Context, res any, scriptCfg Script, emit plugins.Emit) error {
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

	err = e.convertTengoObjToRequest(c.Get("request").Value())
	if err != nil {
		return err
	}

	return nil
}

func (e *Extractor) scriptGlobals(ctx context.Context, res any, emit plugins.Emit) map[string]any {
	req, err := e.convertRequestToTengoObj()
	if err != nil {
		e.logger.Error(err.Error())
	}

	return map[string]any{
		"recipe_scope": &tengo.String{Value: e.UrnScope},
		"request":      req,
		"response":     res,
		"new_entity": &tengo.UserFunction{
			Name:  "new_entity",
			Value: newEntityWrapper(),
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

func (e *Extractor) convertTengoObjToRequest(obj any) error {
	r, err := json.Marshal(obj)
	if err != nil {
		return err
	}
	err = json.Unmarshal(r, &e.config.Request)
	if err != nil {
		return err
	}
	return nil
}
func (e *Extractor) convertRequestToTengoObj() (tengo.Object, error) {
	var res map[string]any
	r, err := json.Marshal(e.config.Request)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(r, &res)
	if err != nil {
		return nil, err
	}
	return tengo.FromInterface(res)
}

func newEntityWrapper() tengo.CallableFunc {
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

		return newEntity(typ)
	}
}

func emitWrapper(emit plugins.Emit) tengo.CallableFunc {
	return func(args ...tengo.Object) (tengo.Object, error) {
		if len(args) != 1 {
			return nil, tengo.ErrWrongNumArguments
		}

		m, ok := tengo.ToInterface(args[0]).(map[string]any)
		if !ok {
			return nil, tengo.ErrInvalidArgumentType{
				Name:     "entity",
				Expected: "Map",
				Found:    args[0].TypeName(),
			}
		}

		// Extract known fields from the map to build an Entity
		urn, _ := m["urn"].(string)
		typ, _ := m["type"].(string)
		name, _ := m["name"].(string)
		source, _ := m["source"].(string)

		// Build properties from the map. If a "properties" key exists, merge
		// its contents directly (avoid nesting properties.properties).
		props := make(map[string]any)
		if p, ok := m["properties"].(map[string]any); ok {
			for k, v := range p {
				props[k] = v
			}
		}
		for k, v := range m {
			switch k {
			case "urn", "type", "name", "source", "description", "properties":
				// already handled
			default:
				props[k] = v
			}
		}

		entity := models.NewEntity(urn, typ, name, source, props)

		// Set description if present
		if desc, ok := m["description"].(string); ok && desc != "" {
			entity.Description = desc
		}

		emit(models.NewRecord(entity))

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
		resp any
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

func newEntity(typ string) (tengo.Object, error) {
	if typ == "" {
		return nil, fmt.Errorf("new entity: type must not be empty")
	}

	return &tengo.Map{
		Value: map[string]tengo.Object{
			"type":       &tengo.String{Value: typ},
			"properties": &tengo.Map{Value: map[string]tengo.Object{}},
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
