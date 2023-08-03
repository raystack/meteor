package grafana

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/goto/meteor/metrics/otelhttpclient"
	"github.com/goto/meteor/plugins"
	"github.com/goto/meteor/plugins/internal/urlbuilder"
)

type Client struct {
	urlb       urlbuilder.Source
	httpClient *http.Client
	config     Config
}

func NewClient(httpClient *http.Client, config Config) (*Client, error) {
	httpClient.Transport = otelhttpclient.NewHTTPTransport(httpClient.Transport)

	urlb, err := urlbuilder.NewSource(config.BaseURL)
	if err != nil {
		return nil, err
	}

	return &Client{
		urlb:       urlb,
		httpClient: httpClient,
		config:     config,
	}, nil
}

func (c *Client) SearchAllDashboardUIDs(ctx context.Context) ([]string, error) {
	const searchRoute = "/api/search"
	targetURL := c.urlb.New().
		Path(searchRoute).
		QueryParam("type", "dash-db").
		URL()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, targetURL.String(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Authorization", c.config.APIKey)
	req = otelhttpclient.AnnotateRequest(req, searchRoute)

	res, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer plugins.DrainBody(res)

	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("response with status: %d", res.StatusCode)
	}

	var searchResponses []map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&searchResponses); err != nil {
		return nil, err
	}
	uids := make([]string, len(searchResponses))
	for i, rsp := range searchResponses {
		uid, ok := rsp["uid"].(string)
		if ok {
			uids[i] = uid
		}
	}
	return uids, nil
}

func (c *Client) GetAllDashboardDetails(ctx context.Context, uids []string) ([]DashboardDetail, error) {
	dataSources, err := c.GetAllDatasources(ctx)
	if err != nil {
		return nil, err
	}

	var dashboards []DashboardDetail
	for _, uid := range uids {
		dashboard, err := c.GetDashboardDetail(uid)
		if err != nil {
			return nil, err
		}
		for j, panel := range dashboard.Dashboard.Panels {
			key := "default"
			if panel.DataSource != "" {
				key = panel.DataSource
			}
			dashboard.Dashboard.Panels[j].DataSource = dataSources[key].Type
		}
		dashboard.Meta.URL = c.urlb.New().Path(dashboard.Meta.URL).URL().String()
		dashboards = append(dashboards, dashboard)
	}

	return dashboards, nil
}

func (c *Client) GetDashboardDetail(uid string) (DashboardDetail, error) {
	const getDashboardRoute = "/api/dashboards/uid/{uid}"
	targetURL := c.urlb.New().
		Path(getDashboardRoute).
		PathParam("uid", uid).
		URL()

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, targetURL.String(), nil)
	if err != nil {
		return DashboardDetail{}, err
	}
	req.Header.Add("Authorization", c.config.APIKey)
	req = otelhttpclient.AnnotateRequest(req, getDashboardRoute)

	res, err := c.httpClient.Do(req)
	if err != nil {
		return DashboardDetail{}, err
	}
	defer plugins.DrainBody(res)

	if res.StatusCode != http.StatusOK {
		return DashboardDetail{}, fmt.Errorf("response with status: %d", res.StatusCode)
	}

	var dashboard DashboardDetail
	if err := json.NewDecoder(res.Body).Decode(&dashboard); err != nil {
		return DashboardDetail{}, err
	}

	return dashboard, nil
}

func (c *Client) GetAllDatasources(ctx context.Context) (map[string]DataSource, error) {
	const listDataSourcesRoute = "/api/datasources"
	targetURL := c.urlb.New().Path(listDataSourcesRoute).URL()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, targetURL.String(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Authorization", c.config.APIKey)
	req = otelhttpclient.AnnotateRequest(req, listDataSourcesRoute)

	res, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer plugins.DrainBody(res)

	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("response with status: %d", res.StatusCode)
	}

	var dataSources []DataSource
	if err := json.NewDecoder(res.Body).Decode(&dataSources); err != nil {
		return nil, err
	}

	result := make(map[string]DataSource)
	for _, rsp := range dataSources {
		if rsp.IsDefault {
			result["default"] = rsp
		} else {
			result[rsp.Name] = rsp
		}
	}
	return result, nil
}

type DashboardDetail struct {
	Meta      Meta      `json:"meta"`
	Dashboard Dashboard `json:"dashboard"`
}

type Meta struct {
	Slug string `json:"slug"`
	URL  string `json:"url"`
}

type Dashboard struct {
	UID         string  `json:"uid"`
	Description string  `json:"description"`
	Panels      []Panel `json:"panels"`
}

type Panel struct {
	ID          int      `json:"id"`
	Title       string   `json:"title"`
	Type        string   `json:"type"`
	Description string   `json:"description"`
	DataSource  string   `json:"datasource"`
	Targets     []Target `json:"targets"`
}

type Target struct {
	RawSQL string `json:"rawSQL"`
}

type DataSource struct {
	Name      string `json:"name"`
	Type      string `json:"type"`
	IsDefault bool   `json:"isDefult"`
}
