package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"

	"github.com/goto/meteor/metrics/otelhttpclient"
	"github.com/goto/meteor/plugins"
	m "github.com/goto/meteor/plugins/extractors/metabase/models"
	"github.com/goto/meteor/plugins/internal/urlbuilder"
)

type Client interface {
	Authenticate(ctx context.Context, host, username, password, sessionID string) error
	GetDatabase(context.Context, int) (m.Database, error)
	GetTable(context.Context, int) (m.Table, error)
	GetDashboard(context.Context, int) (m.Dashboard, error)
	GetDashboards(context.Context) ([]m.Dashboard, error)
}

type client struct {
	urlb          urlbuilder.Source
	httpClient    *http.Client
	host          string
	username      string
	password      string
	sessionID     string
	databaseCache map[int]m.Database
	tableCache    map[int]m.Table
}

func New() Client {
	return &client{
		httpClient:    &http.Client{},
		databaseCache: map[int]m.Database{},
		tableCache:    map[int]m.Table{},
	}
}

func (c *client) Authenticate(ctx context.Context, host, username, password, sessionID string) error {
	urlb, err := urlbuilder.NewSource(host)
	if err != nil {
		return err
	}

	c.urlb = urlb
	c.host = host
	c.username = username
	c.password = password
	c.sessionID = sessionID
	if c.sessionID != "" {
		return nil
	}

	c.sessionID, err = c.getSessionID(ctx)
	if err != nil {
		return fmt.Errorf("get sessionID: %w", err)
	}

	return nil
}

func (c *client) GetTable(ctx context.Context, id int) (m.Table, error) {
	if table, ok := c.tableCache[id]; ok {
		return table, nil
	}

	const getTableRoute = "/api/table/{id}"
	targetURL := c.urlb.New().
		Path(getTableRoute).
		PathParamInt("id", int64(id)).
		URL()

	var tbl m.Table
	if err := c.makeRequest(ctx, getTableRoute, http.MethodGet, targetURL.String(), nil, &tbl); err != nil {
		return m.Table{}, err
	}

	c.tableCache[id] = tbl
	return tbl, nil
}

func (c *client) GetDatabase(ctx context.Context, id int) (m.Database, error) {
	if db, ok := c.databaseCache[id]; ok {
		return db, nil
	}

	const getDatabaseRoute = "/api/database/{id}"
	targetURL := c.urlb.New().
		Path(getDatabaseRoute).
		PathParam("id", strconv.Itoa(id)).
		URL()

	var db m.Database
	if err := c.makeRequest(ctx, getDatabaseRoute, http.MethodGet, targetURL.String(), nil, &db); err != nil {
		return m.Database{}, err
	}

	c.databaseCache[id] = db
	return db, nil
}

func (c *client) GetDashboard(ctx context.Context, id int) (m.Dashboard, error) {
	const getDashboardRoute = "/api/dashboard/{id}"
	targetURL := c.urlb.New().
		Path(getDashboardRoute).
		PathParam("id", strconv.Itoa(id)).
		URL()

	var d m.Dashboard
	if err := c.makeRequest(ctx, getDashboardRoute, http.MethodGet, targetURL.String(), nil, &d); err != nil {
		return m.Dashboard{}, err
	}
	return d, nil
}

func (c *client) GetDashboards(ctx context.Context) ([]m.Dashboard, error) {
	const getDashboardsRoute = "/api/dashboard"
	targetURL := c.urlb.New().Path(getDashboardsRoute).URL()

	var dd []m.Dashboard
	if err := c.makeRequest(ctx, getDashboardsRoute, http.MethodGet, targetURL.String(), nil, &dd); err != nil {
		return nil, err
	}
	return dd, nil
}

func (c *client) getSessionID(ctx context.Context) (string, error) {
	const createSessionRoute = "/api/session"
	targetURL := c.urlb.New().Path(createSessionRoute).URL()

	payload := map[string]interface{}{
		"username": c.username,
		"password": c.password,
	}

	var data struct {
		ID string `json:"id"`
	}
	if err := c.makeRequest(ctx, createSessionRoute, http.MethodPost, targetURL.String(), payload, &data); err != nil {
		return "", err
	}
	return data.ID, nil
}

//nolint:revive
func (c *client) makeRequest(ctx context.Context, route, method, url string, payload, result interface{}) error {
	jsonBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("encode the payload JSON: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewBuffer(jsonBytes))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Metabase-Session", c.sessionID)
	req = otelhttpclient.AnnotateRequest(req, route)

	res, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("generate response: %w", err)
	}
	defer plugins.DrainBody(res)

	if res.StatusCode >= 300 {
		return fmt.Errorf("response status code %d", res.StatusCode)
	}

	data, err := io.ReadAll(res.Body)
	if err != nil {
		return fmt.Errorf("read response body: %w", err)
	}
	if err = json.Unmarshal(data, &result); err != nil {
		return fmt.Errorf("parse response body: %s", string(data))
	}

	return nil
}
