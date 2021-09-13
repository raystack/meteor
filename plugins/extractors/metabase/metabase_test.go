//+build integration

package metabase_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/odpf/meteor/models"
	"github.com/odpf/meteor/models/odpf/assets"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/plugins/extractors/metabase"
	"github.com/odpf/meteor/test"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/assert"
)

const (
	fname                  = "meteor"
	lname                  = "metabase"
	collectionName         = "temp_collection_meteor"
	collection_color       = "#ffffb3"
	collection_description = "Temp Collection for Meteor Metabase Extractor"
	dashboard_name         = "random_dashboard"
	dashboard_description  = "some description"
	email                  = "meteorextractortestuser@gmail.com"
	pass                   = "meteor_pass_1234"
	port                   = "4002"
)

var (
	client = &http.Client{
		Timeout: 4 * time.Second,
	}
	session_id    = ""
	collection_id = 1
	card_id       = 0
	dashboard_id  = 0
	host          = "http://localhost:" + port
)

type responseID struct {
	ID int `json:"id"`
}

type sessionID struct {
	ID string `json:"id"`
}

func TestMain(m *testing.M) {
	// setup test
	opts := dockertest.RunOptions{
		Repository:   "metabase/metabase",
		Tag:          "latest",
		ExposedPorts: []string{port, "3000"},
		PortBindings: map[docker.Port][]docker.PortBinding{
			"3000": {
				{HostIP: "0.0.0.0", HostPort: port},
			},
		},
	}

	retryFn := func(resource *dockertest.Resource) (err error) {
		res, err := http.Get(host + "/api/health")
		if err != nil {
			return
		}
		if res.StatusCode != http.StatusOK {
			return fmt.Errorf("received %d status code", res.StatusCode)
		}
		return
	}

	// Exponential backoff-retry for container to be resy to accept connections
	purgeFn, err := test.CreateContainer(opts, retryFn)
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

func TestExtract(t *testing.T) {

	t.Run("should return error for invalid config", func(t *testing.T) {
		err := newExtractor().Extract(context.TODO(), map[string]interface{}{
			"user_id": "user",
			"host":    host,
		}, make(chan<- models.Record))

		assert.Equal(t, plugins.InvalidConfigError{}, err)
	})

	t.Run("should return dashboard model", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		extractOut := make(chan models.Record)
		go func() {
			err := newExtractor().Extract(ctx, map[string]interface{}{
				"user_id":    email,
				"password":   pass,
				"host":       host,
				"session_id": session_id,
			}, extractOut)
			close(extractOut)

			assert.NoError(t, err)
		}()

		var urns []string
		for val := range extractOut {
			urns = append(urns, val.Data().(*assets.Dashboard).Resource.Urn)
		}

		assert.Equal(t, []string{"metabase.random_dashboard"}, urns)
	})
}

func newExtractor() *metabase.Extractor {
	return metabase.New(test.Logger)
}

func setup() (err error) {
	type responseToken struct {
		Token string `json:"setup-token"`
	}
	var data responseToken
	err = makeRequest("GET", host+"/api/session/properties", nil, &data)
	if err != nil {
		return
	}
	setup_token := data.Token
	err = setUser(setup_token)
	if err != nil {
		return
	}
	err = addMockData(session_id)
	if err != nil {
		return
	}
	return
}

func setUser(setup_token string) (err error) {
	payload := map[string]interface{}{
		"user": map[string]interface{}{
			"first_name": fname,
			"last_name":  lname,
			"email":      email,
			"password":   pass,
			"site_name":  "Unaffiliated",
		},
		"token": setup_token,
		"prefs": map[string]interface{}{
			"site_name":      "Unaffiliated",
			"allow_tracking": "true",
		},
	}
	var data sessionID
	err = makeRequest("POST", host+"/api/setup", payload, &data)
	if err != nil {
		return
	}
	session_id = data.ID
	err = getSessionID()
	return
}

func getSessionID() (err error) {
	payload := map[string]interface{}{
		"username": email,
		"password": pass,
	}
	var data sessionID
	err = makeRequest("POST", host+"/api/session", payload, &data)
	if err != nil {
		return
	}
	session_id = data.ID
	return
}

func addMockData(session_id string) (err error) {
	err = addCollection()
	if err != nil {
		return
	}
	err = addDashboard()
	if err != nil {
		return
	}
	return
}

func addCollection() (err error) {
	payload := map[string]interface{}{
		"name":        collectionName,
		"color":       collection_color,
		"description": collection_description,
	}
	var data responseID
	err = makeRequest("POST", host+"/api/collection", payload, &data)
	if err != nil {
		return
	}
	collection_id = data.ID
	return
}

func addDashboard() (err error) {
	payload := map[string]interface{}{
		"name":          dashboard_name,
		"description":   dashboard_description,
		"collection_id": collection_id,
	}

	var data responseID
	err = makeRequest("POST", host+"/api/dashboard", payload, &data)
	if err != nil {
		return
	}
	dashboard_id = data.ID
	err = addCard(dashboard_id)
	if err != nil {
		return
	}
	return
}

func addCard(id int) (err error) {
	values := map[string]interface{}{
		"id": id,
	}
	x := strconv.Itoa(id)
	type response struct {
		ID int `json:"id"`
	}
	var data response
	err = makeRequest("POST", host+"/api/dashboard/"+x+"/cards", values, &data)
	if err != nil {
		return
	}
	card_id = data.ID
	return
}

func makeRequest(method, url string, payload interface{}, data interface{}) (err error) {
	jsonifyPayload, err := json.Marshal(payload)
	if err != nil {
		return
	}
	body := bytes.NewBuffer(jsonifyPayload)
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if session_id != "" {
		req.Header.Set("X-Metabase-Session", session_id)
	}
	res, err := client.Do(req)
	if err != nil {
		return
	}
	b, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return
	}
	err = json.Unmarshal(b, &data)
	return
}
