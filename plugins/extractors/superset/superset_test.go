//go:build plugins
// +build plugins

package superset_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"testing"
	"time"

	assetsv1beta1 "github.com/odpf/meteor/models/odpf/assets/v1beta1"
	"github.com/odpf/meteor/test/utils"

	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/plugins/extractors/superset"
	"github.com/odpf/meteor/test/mocks"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

const (
	user           = "admin"
	pass           = "admin"
	port           = "8080"
	provider       = "db"
	dashboardTitle = "random dashboard"
	mockChart      = "random chart"
)

var (
	client = &http.Client{
		Timeout: 4 * time.Second,
	}
	accessToken = ""
	csrfToken   = ""
	chartID     = 0
	dashboardID = 0
	host        = "http://localhost:" + port
)

type responseToken struct {
	JwtToken string `json:"access_token"`
}

type securityToken struct {
	CsrfToken string `json:"csrf_token"`
}

func TestMain(m *testing.M) {
	// setup test
	opts := dockertest.RunOptions{
		Repository:   "apache/superset",
		Tag:          "latest",
		ExposedPorts: []string{"8088"},
		PortBindings: map[docker.Port][]docker.PortBinding{
			"8088": {
				{HostIP: "0.0.0.0", HostPort: "8088"},
			},
		},
	}

	retryFn := func(resource *dockertest.Resource) (err error) {
		res, err := http.Get(host + "/health")
		if err != nil {
			return
		}
		body, err := io.ReadAll(res.Body)
		if err := res.Body.Close(); err != nil {
			return err
		}
		if res.StatusCode != http.StatusOK {
			return errors.Wrapf(err, "Response failed with status code: %d and\nbody: %s\n", res.StatusCode, body)
		}
		return
	}

	// exponential backoff-retry, because the application in the container might not be ready to accept connections yet
	purgeFn, err := utils.CreateContainer(opts, retryFn)
	if err != nil {
		log.Fatal(err)
	}
	if err := setup(); err != nil {
		log.Fatal(err)
	}

	// Run tests
	code := m.Run()

	// Clean tests
	if err := purgeFn(); err != nil {
		log.Fatal(err)
	}
	os.Exit(code)
}

// TestInit tests the configs
func TestInit(t *testing.T) {
	t.Run("should return error for invalid config", func(t *testing.T) {
		err := superset.New(utils.Logger).Init(context.TODO(), map[string]interface{}{
			"user_id": "user",
			"host":    host,
		})
		assert.Equal(t, plugins.InvalidConfigError{}, err)
	})
}

// TestExtract tests that the extractor returns the expected result
func TestExtract(t *testing.T) {
	t.Run("should return dashboard model", func(t *testing.T) {
		ctx := context.TODO()
		extr := superset.New(utils.Logger)
		err := extr.Init(ctx, map[string]interface{}{
			"username": user,
			"password": pass,
			"host":     host,
			"provider": provider,
		})
		if err != nil {
			t.Fatal(err)
		}

		emitter := mocks.NewEmitter()
		err = extr.Extract(ctx, emitter.Push)
		assert.NoError(t, err)

		var urns []string
		fmt.Println(emitter.Get())
		for _, record := range emitter.Get() {
			dashboard := record.Data().(*assetsv1beta1.Dashboard)
			urns = append(urns, dashboard.Resource.Urn)
		}
		assert.Equal(t, 10, len(urns))
	})
}

// setup sets up the test environment
func setup() (err error) {
	err = checkUser()
	if err != nil {
		return
	}
	err = addDashboard()
	if err != nil {
		return
	}
	return
}

// checkUser checks if the user exists
// if so, it login through the given credentials
func checkUser() (err error) {
	payload := map[string]interface{}{
		"password": pass,
		"provider": provider,
		"username": user,
	}
	var data responseToken
	if err = makeRequest("POST", host+"/api/v1/security/login", payload, &data); err != nil {
		return
	}
	accessToken = data.JwtToken
	if err = setCsrfToken(); err != nil {
		return
	}
	return
}

// setCsrfToken sets the CSRF token for the next request
func setCsrfToken() (err error) {
	var data securityToken
	// got the token from here
	err = makeRequest("GET", host+"/api/v1/security/csrf_token", nil, &data)
	if err != nil {
		return
	}
	csrfToken = data.CsrfToken
	return
}

// addDashboard adds a dashboard to the database
func addDashboard() (err error) {
	type responseID struct {
		ID int `json:"id"`
	}
	payload := map[string]interface{}{
		"dashboard_title": dashboardTitle,
	}
	var data responseID
	if err = makeRequest("POST", host+"/api/v1/dashboard", payload, &data); err != nil {
		return
	}
	dashboardID = data.ID
	if err = addChart(dashboardID); err != nil {
		return
	}
	return
}

// addChart adds a chart to the dashboard
func addChart(id int) (err error) {
	payload := map[string]interface{}{
		"dashboards": id,
		"slice_name": mockChart,
	}
	type response struct {
		ID int `json:"id"`
	}
	var data response
	err = makeRequest("POST", host+"/api/v1/chart/", payload, &data)
	if err != nil {
		return
	}
	chartID = data.ID
	return
}

// makeRequest helper function to avoid rewriting a request
func makeRequest(method, url string, payload interface{}, data interface{}) (err error) {
	jsonifyPayload, err := json.Marshal(payload)
	if err != nil {
		return errors.Wrap(err, "failed to encode the payload JSON")
	}
	body := bytes.NewBuffer(jsonifyPayload)
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return errors.Wrap(err, "failed to create request")
	}
	var bearer = "Bearer " + accessToken
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", bearer)
	req.Header.Set("X-CSRFToken", csrfToken)
	req.Header.Set("Referer", url)

	res, err := client.Do(req)
	if err != nil {
		return errors.Wrap(err, "failed to generate response")
	}
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return errors.Wrapf(err, "response failed with status code: %d", res.StatusCode)
	}
	b, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return errors.Wrap(err, "failed to read response body")
	}
	if err = json.Unmarshal(b, &data); err != nil {
		return errors.Wrapf(err, "failed to parse: %s", string(b))
	}
	return
}
