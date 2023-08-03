package http

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/goto/meteor/metrics/otelhttpclient"
)

type executeRequestFunc func(ctx context.Context, reqCfg RequestConfig) (map[string]interface{}, error)

func makeRequestExecutor(successCodes []int, httpClient *http.Client) executeRequestFunc {
	return func(ctx context.Context, reqCfg RequestConfig) (map[string]interface{}, error) {
		ctx, cancel := context.WithTimeout(ctx, reqCfg.Timeout)
		defer cancel()

		req, err := buildRequest(ctx, reqCfg)
		if err != nil {
			return nil, err
		}

		resp, err := httpClient.Do(req)
		defer drainBody(resp)
		if err != nil {
			return nil, fmt.Errorf("do request: %w", err)
		}

		return handleResponse(successCodes, resp)
	}
}

func buildRequest(ctx context.Context, reqCfg RequestConfig) (*http.Request, error) {
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

	return otelhttpclient.AnnotateRequest(req, reqCfg.RoutePattern), nil
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

func handleResponse(successCodes []int, resp *http.Response) (map[string]interface{}, error) {
	if !has(successCodes, resp.StatusCode) {
		return nil, fmt.Errorf("unsuccessful request: response status code: %d", resp.StatusCode)
	}

	h := make(map[string]interface{}, len(resp.Header))
	for k := range resp.Header {
		h[strings.ToLower(k)] = resp.Header.Get(k)
	}

	var body interface{}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	return map[string]interface{}{
		"status_code": resp.StatusCode,
		"header":      h,
		"body":        body,
	}, nil
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
