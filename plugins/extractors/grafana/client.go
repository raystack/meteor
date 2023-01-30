package grafana

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
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

func (c *Client) SearchAllDashboardUIDs() (uids []string, err error) {
	url := c.getDashboardSearchURL()
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return
	}
	req.Header.Add("Authorization", c.config.APIKey)
	response, err := c.httpClient.Do(req)
	if err != nil {
		return
	}
	if response.StatusCode != http.StatusOK {
		err = fmt.Errorf("response with status: %d", response.StatusCode)
		return
	}
	defer response.Body.Close()

	var searchResponses []map[string]interface{}
	err = json.NewDecoder(response.Body).Decode(&searchResponses)
	if err != nil {
		return
	}
	uids = make([]string, len(searchResponses))
	for i, rsp := range searchResponses {
		uid, ok := rsp["uid"].(string)
		if ok {
			uids[i] = uid
		}
	}
	return
}

func (c *Client) GetAllDashboardDetails(uids []string) (dashboardDetails []DashboardDetail, err error) {
	dataSources, err := c.GetAllDatasources()
	if err != nil {
		return
	}
	for _, uid := range uids {
		dashboardDetail, e := c.GetDashboardDetail(uid)
		if e != nil {
			err = e
			return
		}
		for j, panel := range dashboardDetail.Dashboard.Panels {
			key := "default"
			if panel.DataSource != "" {
				key = panel.DataSource
			}
			dashboardDetail.Dashboard.Panels[j].DataSource = dataSources[key].Type
		}
		dashboardDetail.Meta.URL = c.concatURL(c.config.BaseURL, dashboardDetail.Meta.URL)
		dashboardDetails = append(dashboardDetails, dashboardDetail)
	}
	return
}

func (c *Client) GetDashboardDetail(uid string) (dashboardDetail DashboardDetail, err error) {
	url := c.getDashboardDetailURL(uid)
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return
	}
	req.Header.Add("Authorization", c.config.APIKey)
	response, err := c.httpClient.Do(req)
	if err != nil {
		return
	}
	if response.StatusCode != http.StatusOK {
		err = fmt.Errorf("response with status: %d", response.StatusCode)
		return
	}
	defer response.Body.Close()

	err = json.NewDecoder(response.Body).Decode(&dashboardDetail)
	return
}

func (c *Client) GetAllDatasources() (dataSources map[string]DataSource, err error) {
	url := c.getDataSourceURL()
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return
	}
	req.Header.Add("Authorization", c.config.APIKey)
	response, err := c.httpClient.Do(req)
	if err != nil {
		return
	}
	if response.StatusCode != http.StatusOK {
		err = fmt.Errorf("response with status: %d", response.StatusCode)
		return
	}
	defer response.Body.Close()

	var datasourceResponses []DataSource
	err = json.NewDecoder(response.Body).Decode(&datasourceResponses)
	if err != nil {
		return
	}

	dataSources = make(map[string]DataSource)
	for _, rsp := range datasourceResponses {
		if rsp.IsDefault {
			dataSources["default"] = rsp
		} else {
			dataSources[rsp.Name] = rsp
		}
	}
	return
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
