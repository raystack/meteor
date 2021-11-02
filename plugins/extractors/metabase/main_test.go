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
	"github.com/pkg/errors"
)

var session_id string
var (
	port                = "4002"
	host                = "http://localhost:" + port
	email               = "user@example.com"
	pass                = "meteor_pass_1234"
	populatedDashboards = []metabase.Dashboard{}
	populatedCards      = map[int]metabase.Card{}
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
		return errors.Wrap(err, "error getting setup token")
	}
	err = setSessionID(data.Token)
	if err != nil {
		return errors.Wrap(err, "error setting session_id")
	}
	err = populateData()
	if err != nil {
		return errors.Wrap(err, "error populating data")
	}
	return
}

func populateData() (err error) {
	err = populateCollections()
	if err != nil {
		return errors.Wrap(err, "error populating collections")
	}
	err = populateCards()
	if err != nil {
		return errors.Wrap(err, "error populating cards")
	}
	err = populateDashboards()
	if err != nil {
		return errors.Wrap(err, "error populating dashboards")
	}
	return
}

func populateCollections() (err error) {
	var collections []metabase.Collection
	err = readFromFiles("./testdata/collections.json", &collections)
	if err != nil {
		return errors.Wrap(err, "error reading collections")
	}

	for _, c := range collections {
		var res metabase.Collection
		err = makeRequest("POST", host+"/api/collection", c, &res)
		if err != nil {
			return errors.Wrapf(err, "error creating collection \"%s\"", c.Name)
		}
	}

	return
}

func populateCards() (err error) {
	var cards []metabase.Card
	err = readFromFiles("./testdata/cards.json", &cards)
	if err != nil {
		return errors.Wrap(err, "error reading cards")
	}

	for _, c := range cards {
		var res metabase.Card
		err = makeRequest("POST", host+"/api/card", c, &res)
		if err != nil {
			return errors.Wrapf(err, "error creating card \"%s\"", c.Name)
		}

		populatedCards[res.ID] = res
	}

	return
}

func populateDashboards() (err error) {
	var dashboards []metabase.Dashboard
	err = readFromFiles("./testdata/dashboards.json", &dashboards)
	if err != nil {
		return errors.Wrap(err, "error reading dashboards")
	}

	for i, d := range dashboards {
		var res metabase.Dashboard
		err = makeRequest("POST", host+"/api/dashboard", d, &res)
		if err != nil {
			return errors.Wrapf(err, "error creating dashboard \"%s\"", d.Name)
		}

		for _, oc := range d.OrderedCards {
			err = makeRequest("POST", fmt.Sprintf("%s/api/dashboard/%d/cards", host, res.ID), oc, nil)
			if err != nil {
				return errors.Wrapf(err, "error assigning card_id \"%d\" to dashboard \"%s\"", oc.CardID, d.Name)
			}
		}

		res.OrderedCards = d.OrderedCards
		dashboards[i] = res

		populatedDashboards = append(populatedDashboards, dashboards[i])
	}

	return
}

func readFromFiles(path string, data interface{}) error {
	file, err := os.Open(path)
	if err != nil {
		return errors.Wrapf(err, "error opening \"%s\"", path)
	}
	err = json.NewDecoder(file).Decode(&data)
	if err != nil {
		return errors.Wrapf(err, "error decoding \"%s\"", path)
	}

	return nil
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
