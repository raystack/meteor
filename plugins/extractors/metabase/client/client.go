package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/raystack/meteor/plugins"
	m "github.com/raystack/meteor/plugins/extractors/metabase/models"
)

type Client interface {
	Authenticate(ctx context.Context, host, username, password, sessionID string) error
	GetDatabase(context.Context, int) (m.Database, error)
	GetTable(context.Context, int) (m.Table, error)
	GetDashboard(context.Context, int) (m.Dashboard, error)
	GetDashboards(context.Context) ([]m.Dashboard, error)
}

type client struct {
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
	c.host = host
	c.username = username
	c.password = password
	c.sessionID = sessionID
	if c.sessionID != "" {
		return nil
	}

	var err error
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

	var tbl m.Table
	url := fmt.Sprintf("%s/api/table/%d", c.host, id)
	if err := c.makeRequest(ctx, "GET", url, nil, &tbl); err != nil {
		return m.Table{}, err
	}

	c.tableCache[id] = tbl
	return tbl, nil
}

func (c *client) GetDatabase(ctx context.Context, id int) (m.Database, error) {
	if db, ok := c.databaseCache[id]; ok {
		return db, nil
	}

	var db m.Database
	url := fmt.Sprintf("%s/api/database/%d", c.host, id)
	if err := c.makeRequest(ctx, "GET", url, nil, &db); err != nil {
		return m.Database{}, err
	}

	c.databaseCache[id] = db
	return db, nil
}

func (c *client) GetDashboard(ctx context.Context, id int) (m.Dashboard, error) {
	var d m.Dashboard
	url := fmt.Sprintf("%s/api/dashboard/%d", c.host, id)
	if err := c.makeRequest(ctx, "GET", url, nil, &d); err != nil {
		return m.Dashboard{}, err
	}

	return d, nil
}

func (c *client) GetDashboards(ctx context.Context) ([]m.Dashboard, error) {
	var dd []m.Dashboard
	url := fmt.Sprintf("%s/api/dashboard", c.host)
	if err := c.makeRequest(ctx, "GET", url, nil, &dd); err != nil {
		return nil, err
	}

	return dd, nil
}

func (c *client) getSessionID(ctx context.Context) (string, error) {
	payload := map[string]interface{}{
		"username": c.username,
		"password": c.password,
	}
	var data struct {
		ID string `json:"id"`
	}
	if err := c.makeRequest(ctx, "POST", c.host+"/api/session", payload, &data); err != nil {
		return "", err
	}

	return data.ID, nil
}

func (c *client) makeRequest(ctx context.Context, method, url string, payload, result interface{}) error {
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
