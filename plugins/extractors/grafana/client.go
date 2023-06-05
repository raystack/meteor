package grafana

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/goto/meteor/plugins"
)

type Client struct {
	httpClient *http.Client
	config     Config
}

func NewClient(httpClient *http.Client, config Config) *Client {
	return &Client{
		httpClient: httpClient,
		config:     config,
	}
}

func (c *Client) SearchAllDashboardUIDs(ctx context.Context) ([]string, error) {
	url := c.getDashboardSearchURL()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Add("Authorization", c.config.APIKey)
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
		dashboard.Meta.URL = c.concatURL(c.config.BaseURL, dashboard.Meta.URL)
		dashboards = append(dashboards, dashboard)
	}

	return dashboards, nil
}

func (c *Client) GetDashboardDetail(uid string) (DashboardDetail, error) {
	url := c.getDashboardDetailURL(uid)
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return DashboardDetail{}, err
	}
	req.Header.Add("Authorization", c.config.APIKey)
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
	url := c.getDataSourceURL()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Add("Authorization", c.config.APIKey)
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

func (c *Client) getDataSourceURL() string {
	return c.concatURL(c.config.BaseURL, "/api/datasources")
}

func (c *Client) getDashboardDetailURL(uid string) string {
	return c.concatURL(c.config.BaseURL, "/api/dashboards/uid/"+uid)
}

func (c *Client) getDashboardSearchURL() string {
	return c.concatURL(c.config.BaseURL, "/api/search?type=dash-db")
}

func (c *Client) concatURL(baseURL, path string) string {
	if strings.HasSuffix(baseURL, "/") {
		return baseURL[:len(baseURL)-1] + path
	}
	return baseURL + path
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
