//go:build integration
// +build integration

package tableau_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"testing"

	"github.com/dnaeon/go-vcr/v2/recorder"
	"github.com/odpf/meteor/models"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/plugins/extractors/tableau"
	"github.com/odpf/meteor/test/mocks"
	testutils "github.com/odpf/meteor/test/utils"
	"github.com/stretchr/testify/assert"
)

var (
	host     = "https://server.tableau.com"
	version  = "3.12"
	sitename = "testdev550928"
	username = "meteor_user"
	password = "xxxxxxxxxx"
)

func TestInit(t *testing.T) {
	t.Run("should return error for invalid config", func(t *testing.T) {
		err := tableau.New(testutils.Logger).Init(context.TODO(), map[string]interface{}{
			"host": "invalid_host",
		})

		assert.Equal(t, plugins.InvalidConfigError{}, err)
	})
}

func TestExtract(t *testing.T) {
	t.Run("should return dashboard model", func(t *testing.T) {
		r, err := recorder.New("fixtures/get_workbooks_graphql_e2e")
		if err != nil {
			t.Fatal(err)
		}
		defer r.Stop()

		ctx := context.TODO()
		extr := tableau.New(testutils.Logger,
			tableau.WithHTTPClient(&http.Client{
				Transport: r,
			}))
		err = extr.Init(ctx, map[string]interface{}{
			"host":     host,
			"version":  version,
			"sitename": sitename,
			"username": username,
			"password": password,
		})
		if err != nil {
			t.Fatal(err)
		}

		emitter := mocks.NewEmitter()
		err = extr.Extract(ctx, emitter.Push)
		assert.NoError(t, err)

		records := emitter.Get()
		var actuals []models.Metadata
		for _, r := range records {
			actuals = append(actuals, r.Data())
		}

		expectedJSONStringDashboardProto, err := ioutil.ReadFile("testdata/dashboards_proto.json")
		assert.Nil(t, err)

		assertJSONString(t, string(expectedJSONStringDashboardProto), actuals)
	})
}

func assertJSONString(t *testing.T, expected string, actual interface{}) {
	actualBytes, err := json.Marshal(actual)
	if err != nil {
		t.Fatal(err)
	}
	var prettyPrintActualBytes bytes.Buffer
	err = json.Indent(&prettyPrintActualBytes, []byte(actualBytes), "", "\t")
	assert.Nil(t, err)

	var parsedJSON bytes.Buffer
	err = json.Indent(&parsedJSON, []byte(expected), "", "\t")
	assert.Nil(t, err)
	assert.Equal(t, parsedJSON.String(), prettyPrintActualBytes.String())
}
