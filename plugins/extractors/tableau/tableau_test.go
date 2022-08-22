//go:build plugins
// +build plugins

package tableau_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/dnaeon/go-vcr/v2/recorder"
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
	urnScope = "test-tableau"
)

func TestInit(t *testing.T) {
	t.Run("should return error for invalid config", func(t *testing.T) {
		err := tableau.New(testutils.Logger).Init(context.TODO(), plugins.Config{
			URNScope: urnScope,
			RawConfig: map[string]interface{}{
				"host": "invalid_host",
			}})

		assert.ErrorAs(t, err, &plugins.InvalidConfigError{})
	})
	t.Run("should return error for password missing with username", func(t *testing.T) {
		err := tableau.New(testutils.Logger).Init(context.TODO(), plugins.Config{
			URNScope: urnScope,
			RawConfig: map[string]interface{}{
				"host":       host,
				"version":    version,
				"identifier": "my-tableau",
				"sitename":   sitename,
				"username":   username,
			}})

		assert.ErrorAs(t, err, &plugins.InvalidConfigError{})
	})
	t.Run("should return error for site_id and auth_token missing", func(t *testing.T) {
		err := tableau.New(testutils.Logger).Init(context.TODO(), plugins.Config{
			URNScope: urnScope,
			RawConfig: map[string]interface{}{
				"host":       host,
				"version":    version,
				"identifier": "my-tableau",
				"sitename":   sitename,
			}})

		assert.ErrorAs(t, err, &plugins.InvalidConfigError{})
	})
	t.Run("should return no error for config with site_id and auth_token without username", func(t *testing.T) {
		err := tableau.New(testutils.Logger).Init(context.TODO(), plugins.Config{
			URNScope: urnScope,
			RawConfig: map[string]interface{}{
				"host":       host,
				"version":    version,
				"identifier": "my-tableau",
				"sitename":   sitename,
				"site_id":    "xxxxxxxxx",
				"auth_token": "xxxxxxxxx",
			}})
		assert.NoError(t, err)
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
		err = extr.Init(ctx, plugins.Config{
			URNScope: urnScope,
			RawConfig: map[string]interface{}{
				"host":       host,
				"version":    version,
				"identifier": "my-tableau",
				"sitename":   sitename,
				"username":   username,
				"password":   password,
			}})
		if err != nil {
			t.Fatal(err)
		}

		emitter := mocks.NewEmitter()
		err = extr.Extract(ctx, emitter.Push)
		assert.NoError(t, err)

		actuals := emitter.GetAllData()
		testutils.AssertProtosWithJSONFile(t, "./testdata/dashboards_proto.json", actuals)
	})
}
