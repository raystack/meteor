package tengoutil

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/d5/tengo/v2"
	"github.com/d5/tengo/v2/stdlib"
)

const (
	maxAllocs = 5000
	maxConsts = 500
)

const expectedArgsLength = 2

var defaultTimeout = 5 * time.Second

var httpModule = map[string]tengo.Object{
	"get": httpGetFunction,
}

func NewSecureScript(input []byte, globals map[string]interface{}) (*tengo.Script, error) {
	s := tengo.NewScript(input)

	modules := stdlib.GetModuleMap(
		// `os` is excluded, should *not* be importable from script.
		"math", "text", "times", "rand", "fmt", "json", "base64", "hex", "enum",
	)
	modules.AddBuiltinModule("http", httpModule)
	s.SetImports(modules)
	s.SetMaxAllocs(maxAllocs)
	s.SetMaxConstObjects(maxConsts)

	for name, v := range globals {
		if err := s.Add(name, v); err != nil {
			return nil, fmt.Errorf("new secure script: declare globals: %w", err)
		}
	}

	return s, nil
}

var httpGetFunction = &tengo.UserFunction{
	Name: "get",
	Value: func(args ...tengo.Object) (tengo.Object, error) {
		url, err := extractURL(args)
		if err != nil {
			return nil, err
		}
		headers, err := extractHeaders(args)
		if err != nil {
			return nil, err
		}

		return performGetRequest(url, headers, defaultTimeout)
	},
}

func extractURL(args []tengo.Object) (string, error) {
	if len(args) < 1 {
		return "", errors.New("expected at least 1 argument (URL)")
	}
	url, ok := tengo.ToString(args[0])
	if !ok {
		return "", errors.New("expected argument 1 (URL) to be a string")
	}

	return url, nil
}

func extractHeaders(args []tengo.Object) (map[string]string, error) {
	headers := make(map[string]string)
	if len(args) == expectedArgsLength {
		headerMap, ok := args[1].(*tengo.Map)
		if !ok {
			return nil, fmt.Errorf("expected argument %d (headers) to be a map", expectedArgsLength)
		}
		for key, value := range headerMap.Value {
			strValue, valueOk := tengo.ToString(value)
			if !valueOk {
				return nil, fmt.Errorf("header value for key '%s' must be a string, got %T", key, value)
			}
			headers[key] = strValue
		}
	}

	return headers, nil
}

func performGetRequest(url string, headers map[string]string, timeout time.Duration) (tengo.Object, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	for key, value := range headers {
		req.Header.Add(key, value)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	return &tengo.Map{
		Value: map[string]tengo.Object{
			"body": &tengo.String{Value: string(body)},
			"code": &tengo.Int{Value: int64(resp.StatusCode)},
		},
	}, nil
}
