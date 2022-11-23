package http

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

func (e *Extractor) executeRequest(ctx context.Context) (interface{}, error) {
	cfg := e.config

	ctx, cancel := context.WithTimeout(ctx, cfg.Request.Timeout)
	defer cancel()

	req, err := buildRequest(ctx, cfg)
	if err != nil {
		return nil, err
	}

	resp, err := e.http.Do(req)
	defer drainBody(resp)
	if err != nil {
		return nil, fmt.Errorf("do request: %w", err)
	}

	return handleResponse(cfg, resp)
}

func buildRequest(ctx context.Context, cfg Config) (*http.Request, error) {
	reqCfg := cfg.Request

	body, err := asReader(reqCfg.Body)
	if err != nil {
		return nil, fmt.Errorf("encode request body: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, reqCfg.Method, reqCfg.URL, body)
	if err != nil {
		return nil, fmt.Errorf("create HTTP request: %w", err)
	}

	addQueryParams(req, reqCfg.QueryParams)

	for name, v := range reqCfg.Headers {
		req.Header.Set(name, v)
	}
	if req.Body != nil && req.Body != http.NoBody {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Accept", "application/json")

	return req, nil
}

func addQueryParams(req *http.Request, params []QueryParam) {
	if len(params) == 0 {
		return
	}

	q := req.URL.Query()
	// First delete any possible conflicts. Cannot be done in a single loop
	// because params can have multiple entries with the same key.
	for _, p := range params {
		q.Del(p.Key)
	}
	for _, p := range params {
		q.Add(p.Key, p.Value)
	}
	req.URL.RawQuery = q.Encode()
}

func handleResponse(cfg Config, resp *http.Response) (interface{}, error) {
	if !has(cfg.SuccessCodes, resp.StatusCode) {
		return nil, fmt.Errorf("unsuccessful request: response status code: %d", resp.StatusCode)
	}

	var res interface{}
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	return res, nil
}

func asReader(v interface{}) (io.Reader, error) {
	if v == nil {
		return nil, nil
	}

	if body, ok := v.(string); ok {
		return bytes.NewBufferString(body), nil
	}

	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(v); err != nil {
		return nil, err
	}

	return &buf, nil
}

// drainBody drains and closes the response body to avoid the following
// gotcha:
// http://devs.cloudimmunity.com/gotchas-and-common-mistakes-in-go-golang/index.html#close_http_resp_body
func drainBody(resp *http.Response) {
	if resp == nil {
		return
	}

	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
}

func has(haystack []int, needle int) bool {
	for _, n := range haystack {
		if n == needle {
			return true
		}
	}

	return false
}
