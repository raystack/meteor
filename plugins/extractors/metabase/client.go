package metabase

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/pkg/errors"
)

type Client interface {
	Authenticate(host, username, password, sessionID string) error
	GetCards() ([]Card, error)
	GetDashboard(int) (Dashboard, error)
	GetDashboards() ([]Dashboard, error)
}

type client struct {
	httpClient *http.Client
	host       string
	username   string
	password   string
	sessionID  string
}

func newClient() *client {
	return &client{
		httpClient: &http.Client{},
	}
}

func (c *client) Authenticate(host, username, password, sessionID string) (err error) {
	c.host = host
	c.username = username
	c.password = password
	c.sessionID = sessionID
	if c.sessionID != "" {
		return nil
	}

	c.sessionID, err = c.getSessionID()
	if err != nil {
		err = errors.Wrap(err, "error getting sessionID")
		return
	}

	return
}

func (c *client) GetCards() (databases []Card, err error) {
	return
}

func (c *client) GetDashboard(id int) (database Dashboard, err error) {
	return
}

func (c *client) GetDashboards() (databases []Dashboard, err error) {
	return
}

func (c *client) fetchDashboard(dashboard_id int) (dashboard Dashboard, err error) {
	url := fmt.Sprintf("%s/api/dashboard/%d", c.host, dashboard_id)
	err = c.makeRequest("GET", url, nil, &dashboard)

	return
}

func (c *client) fetchDashboards() (data []Dashboard, err error) {
	url := fmt.Sprintf("%s/api/dashboard", c.host)
	err = c.makeRequest("GET", url, nil, &data)

	return
}

func (c *client) getSessionID() (sessionID string, err error) {
	payload := map[string]interface{}{
		"username": c.username,
		"password": c.password,
	}
	type responseID struct {
		ID string `json:"id"`
	}
	var data responseID
	err = c.makeRequest("POST", c.host+"/api/session", payload, &data)
	if err != nil {
		return
	}

	return data.ID, nil
}

func (c *client) makeRequest(method, url string, payload interface{}, data interface{}) (err error) {
	jsonBytes, err := json.Marshal(payload)
	if err != nil {
		return errors.Wrap(err, "failed to encode the payload JSON")
	}

	req, err := http.NewRequest(method, url, bytes.NewBuffer(jsonBytes))
	if err != nil {
		return errors.Wrap(err, "failed to create request")
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Metabase-Session", c.sessionID)

	res, err := c.httpClient.Do(req)
	if err != nil {
		return errors.Wrap(err, "failed to generate response")
	}
	if res.StatusCode >= 300 {
		return fmt.Errorf("getting %d status code", res.StatusCode)
	}

	bytes, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return errors.Wrap(err, "failed to read response body")
	}
	if err = json.Unmarshal(bytes, &data); err != nil {
		return errors.Wrapf(err, "failed to parse: %s", string(bytes))
	}

	return
}
