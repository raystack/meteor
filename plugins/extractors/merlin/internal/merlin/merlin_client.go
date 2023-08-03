package merlin

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"time"

	"github.com/goto/meteor/metrics/otelhttpclient"
	"github.com/goto/meteor/plugins/internal/urlbuilder"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
)

var authScopes = []string{"https://www.googleapis.com/auth/userinfo.email"}

type Client struct {
	urlb    urlbuilder.Source
	http    *http.Client
	timeout time.Duration
}

type ClientParams struct {
	BaseURL            string
	ServiceAccountJSON []byte
	Timeout            time.Duration
}

func NewClient(ctx context.Context, params ClientParams) (Client, error) {
	httpClient, err := authenticatedClient(ctx, params.ServiceAccountJSON, authScopes...)
	if err != nil {
		return Client{}, fmt.Errorf("new Merlin client: %w", err)
	}
	httpClient.Transport = otelhttpclient.NewHTTPTransport(httpClient.Transport)

	urlb, err := urlbuilder.NewSource(params.BaseURL)
	if err != nil {
		return Client{}, fmt.Errorf("new Merlin client: %w", err)
	}

	return Client{
		urlb:    urlb,
		http:    httpClient,
		timeout: params.Timeout,
	}, nil
}

func (c Client) Projects(ctx context.Context) ([]Project, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	const projectsRoute = "/v1/projects"
	u := c.urlb.New().Path(projectsRoute).URL()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("merlin client: fetch projects: new request: %w", err)
	}
	req = otelhttpclient.AnnotateRequest(req, projectsRoute)

	var projects []Project
	if err := c.exec(req, &projects); err != nil {
		return nil, fmt.Errorf("merlin client: fetch projects: %w", err)
	}

	return projects, nil
}

func (c Client) Models(ctx context.Context, projectID int64) ([]Model, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	const modelsRoute = "/v1/projects/{projectID}/models"
	u := c.urlb.New().
		Path(modelsRoute).
		PathParamInt("projectID", projectID).
		URL()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("merlin client: project ID '%d': fetch models: new request: %w", projectID, err)
	}
	req = otelhttpclient.AnnotateRequest(req, modelsRoute)

	var models []Model
	if err := c.exec(req, &models); err != nil {
		return nil, fmt.Errorf("merlin client: project ID '%d': fetch models: %w", projectID, err)
	}

	return models, nil
}

func (c Client) ModelVersion(ctx context.Context, modelID, versionID int64) (ModelVersion, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	const modelVersionRoute = "/v1/models/{modelID}/versions/{versionID}"
	u := c.urlb.New().
		Path(modelVersionRoute).
		PathParamInt("modelID", modelID).
		PathParamInt("versionID", versionID).
		URL()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return ModelVersion{}, fmt.Errorf(
			"merlin client: model ID '%d': fetch version '%d': new request: %w", modelID, versionID, err,
		)
	}
	req = otelhttpclient.AnnotateRequest(req, modelVersionRoute)

	var result ModelVersion
	if err := c.exec(req, &result); err != nil {
		return ModelVersion{}, fmt.Errorf(
			"merlin client: project ID '%d': fetch version '%d': %w", modelID, versionID, err,
		)
	}

	return result, nil
}

func (c Client) exec(req *http.Request, result interface{}) error {
	resp, err := c.http.Do(req)
	defer drainBody(resp)
	if err != nil {
		return fmt.Errorf("execute request: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		msg, err := failureMsg(resp)
		if err != nil {
			return err
		}

		return &APIError{
			Method:   req.Method,
			Endpoint: req.URL.String(),
			Status:   resp.StatusCode,
			Msg:      msg,
		}
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}

	return nil
}

func authenticatedClient(ctx context.Context, serviceAccountJSON []byte, scopes ...string) (*http.Client, error) {
	if len(serviceAccountJSON) == 0 {
		return google.DefaultClient(ctx, scopes...)
	}

	creds, err := google.CredentialsFromJSON(ctx, serviceAccountJSON, authScopes...)
	if err != nil {
		return nil, fmt.Errorf("google credentials from JSON: %w", err)
	}

	return oauth2.NewClient(ctx, creds.TokenSource), nil
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

func failureMsg(resp *http.Response) (string, error) {
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("read response body: %w", err)
	}

	if !isJSONContent(resp.Header.Get("Content-Type")) || !json.Valid(data) {
		return (string)(data), nil
	}

	var body struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal(data, &body); err != nil {
		return "", fmt.Errorf("decode failure response: %w", err)
	}

	return body.Error, nil
}

// Source: https://github.com/go-resty/resty/blob/v2.2.0/client.go#L64
var jsonCheck = regexp.MustCompile(`(?i:(application|text)/(json|.*\+json|json\-.*)(;|$))`)

func isJSONContent(ct string) bool { return jsonCheck.MatchString(ct) }
