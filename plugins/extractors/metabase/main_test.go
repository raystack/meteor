//go:build integration
// +build integration

package metabase_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/odpf/meteor/plugins/extractors/metabase"
	"github.com/odpf/meteor/test/utils"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
)

var session_id string
var (
	port                = "4002"
	host                = "http://localhost:" + port
	email               = "user@example.com"
	pass                = "meteor_pass_1234"
	populatedDashboards = []metabase.Dashboard{}
	dashboardCards      = map[int][]metabase.Card{}
)

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

func setup() (err error) {
	type responseToken struct {
		Token string `json:"setup-token"`
	}
	var data responseToken
	err = makeRequest("GET", host+"/api/session/properties", nil, &data)
	if err != nil {
		return
	}
	err = setSessionID(data.Token)
	if err != nil {
		return
	}
	err = addMockData()
	if err != nil {
		return
	}
	return
}

func addMockData() (err error) {
	collection_id, err := addCollection()
	if err != nil {
		return
	}
	err = addDashboard(collection_id)
	if err != nil {
		return
	}
	return
}

func addCollection() (collection_id int, err error) {
	payload := map[string]interface{}{
		"name":        "temp_collection_meteor",
		"color":       "#ffffb3",
		"description": "Temp Collection for Meteor Metabase Extractor",
	}

	type response struct {
		ID int `json:"id"`
	}
	var resp response
	err = makeRequest("POST", host+"/api/collection", payload, &resp)
	if err != nil {
		return
	}

	collection_id = resp.ID
	return
}

func addDashboard(collection_id int) (err error) {
	payload := map[string]interface{}{
		"name":          "random_dashboard",
		"description":   "some description",
		"collection_id": collection_id,
	}

	var dashboard metabase.Dashboard
	err = makeRequest("POST", host+"/api/dashboard", payload, &dashboard)
	if err != nil {
		return
	}
	err = addCards(dashboard)
	if err != nil {
		return
	}

	populatedDashboards = append(populatedDashboards, dashboard)

	return
}

func addCards(dashboard metabase.Dashboard) (err error) {
	// create card
	cardPayload := map[string]interface{}{
		"name":                   "Orders, Filtered by Quantity",
		"table_id":               1,
		"database_id":            1,
		"collection_id":          dashboard.CollectionID,
		"creator_id":             1,
		"description":            "HELPFUL CHART DESC",
		"query_type":             "query",
		"display":                "table",
		"query_average_duration": 114,
		"archived":               false,
	}
	cardUrl := fmt.Sprintf("%s/api/card", host)
	var card metabase.Card
	err = makeRequest("POST", cardUrl, cardPayload, &card)
	if err != nil {
		return
	}

	// set card to dashboard
	cardToDashboardUrl := fmt.Sprintf("%s/api/dashboard/%d/cards", host, dashboard.ID)
	err = makeRequest("POST", cardToDashboardUrl, map[string]interface{}{
		"card_id": card.ID,
	}, nil)
	if err != nil {
		return
	}

	// save card to memory for asserting
	var cards []metabase.Card
	cards = append(cards, card)
	dashboardCards[dashboard.ID] = cards

	return
}

func setSessionID(setup_token string) (err error) {
	payload := map[string]interface{}{
		"user": map[string]interface{}{
			"first_name": "John",
			"last_name":  "Doe",
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

	type response struct {
		ID string `json:"id"`
	}
	var resp response
	err = makeRequest("POST", host+"/api/setup", payload, &resp)
	if err != nil {
		return
	}
	session_id = resp.ID
	return
}

func makeRequest(method, url string, payload interface{}, data interface{}) (err error) {
	client := &http.Client{
		Timeout: 4 * time.Second,
	}

	jsonBytes, err := json.Marshal(payload)
	if err != nil {
		return
	}
	req, err := http.NewRequest(method, url, bytes.NewBuffer(jsonBytes))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Metabase-Session", session_id)

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
