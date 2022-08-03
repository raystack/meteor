package redash_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/odpf/meteor/models"
	v1beta2 "github.com/odpf/meteor/models/odpf/assets/v1beta2"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/plugins/extractors/redash"
	"github.com/odpf/meteor/test/mocks"
	"github.com/odpf/meteor/test/utils"
	util "github.com/odpf/meteor/utils"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/anypb"
)

var testServer *httptest.Server
var urnScope = "test-redash"

func TestMain(m *testing.M) {
	testServer = NewTestServer()

	// run tests
	code := m.Run()

	testServer.Close()
	os.Exit(code)
}

// TestInit tests the configs
func TestInit(t *testing.T) {
	t.Run("should return error if for empty base_url in config", func(t *testing.T) {
		err := redash.New(utils.Logger).Init(context.TODO(), plugins.Config{
			URNScope: urnScope,
			RawConfig: map[string]interface{}{
				"base_url": "",
				"api_key":  "checkAPI",
			}})
		assert.ErrorAs(t, err, &plugins.InvalidConfigError{})
	})

	t.Run("should return error if for empty api_key in config", func(t *testing.T) {
		err := redash.New(utils.Logger).Init(context.TODO(), plugins.Config{
			URNScope: urnScope,
			RawConfig: map[string]interface{}{
				"base_url": testServer.URL,
				"api_key":  "",
			}})

		assert.ErrorAs(t, err, &plugins.InvalidConfigError{})
	})
}

// TestExtract tests that the extractor returns the expected result
func TestExtract(t *testing.T) {
	t.Run("should return dashboard model", func(t *testing.T) {
		data, err := anypb.New(&v1beta2.Dashboard{})
		if err != nil {
			err = fmt.Errorf("error creating Any struct: %w", err)
			t.Fatal(err)
		}
		expectedData := []models.Record{
			models.NewRecord(&v1beta2.Asset{
				Urn:     "urn:redash:test-redash:dashboard:421",
				Name:    "firstDashboard",
				Service: "redash",
				Type:    "dashboard",
				Data:    data,
				Attributes: util.TryParseMapToProto(map[string]interface{}{
					"user_id": 1,
					"version": 1,
					"slug":    "new-dashboard-copy",
				}),
			}),
			models.NewRecord(&v1beta2.Asset{
				Urn:     "urn:redash:test-redash:dashboard:634",
				Name:    "secondDashboard",
				Service: "redash",
				Type:    "dashboard",
				Data:    data,
				Attributes: util.TryParseMapToProto(map[string]interface{}{
					"user_id": 1,
					"version": 2,
					"slug":    "test-dashboard-updated",
				}),
			}),
		}

		ctx := context.TODO()
		extractor := redash.New(utils.Logger)
		err = extractor.Init(ctx, plugins.Config{
			URNScope: urnScope,
			RawConfig: map[string]interface{}{
				"base_url": testServer.URL,
				"api_key":  "checkAPI",
			}})
		if err != nil {
			t.Fatal(err)
		}

		emitter := mocks.NewEmitter()
		err = extractor.Extract(ctx, emitter.Push)

		assert.NoError(t, err)
		assert.Equal(t, expectedData, emitter.Get())
	})
}

func NewTestServer() *httptest.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/dashboards", func(res http.ResponseWriter, req *http.Request) {
		_, err := res.Write([]byte(`
			{
				"count": 2,
				"page": 1,
				"page_size": 25,
				"results": [
					{
						"tags": [],
						"is_archived": false,
						"updated_at": "2022-06-29T10:29:26.865Z",
						"is_favorite": false,
						"user": {
							"auth_type": "password",
							"is_disabled": false,
							"updated_at": "2022-06-29T10:46:55.810Z",
							"profile_image_url": "https://www.gravatar.com/avatar/75d23af433e0cea4c0e45a56dba18b30?s=40&d=identicon",
							"is_invitation_pending": false,
							"groups": [
								1,
								2
							],
							"id": 1,
							"name": "admin",
							"created_at": "2022-06-29T10:29:06.784Z",
							"disabled_at": null,
							"is_email_verified": true,
							"active_at": "2022-06-29T10:46:50Z",
							"email": "admin@gmail.com"
						},
						"layout": [],
						"is_draft": true,
						"id": 421,
						"user_id": 1,
						"name": "firstDashboard",
						"created_at": "2022-06-29T10:29:26.865Z",
						"slug": "new-dashboard-copy",
						"version": 1,
						"widgets": null,
						"dashboard_filters_enabled": false
					},
					{
						"tags": [],
						"is_archived": false,
						"updated_at": "2022-06-29T10:29:26.865Z",
						"is_favorite": false,
						"user": {
							"auth_type": "password",
							"is_disabled": false,
							"updated_at": "2022-06-29T10:46:55.810Z",
							"profile_image_url": "https://www.gravatar.com/avatar/75d23af433e0cea4c0e45a56dba18b30?s=40&d=identicon",
							"is_invitation_pending": false,
							"groups": [
								1,
								2
							],
							"id": 1,
							"name": "admin",
							"created_at": "2022-06-29T10:29:06.784Z",
							"disabled_at": null,
							"is_email_verified": true,
							"active_at": "2022-06-29T10:46:50Z",
							"email": "admin@gmail.com"
						},
						"layout": [],
						"is_draft": true,
						"id": 634,
						"user_id": 1,
						"name": "secondDashboard",
						"created_at": "2022-06-29T10:29:26.865Z",
						"slug": "test-dashboard-updated",
						"version": 2,
						"widgets": null,
						"dashboard_filters_enabled": false
					}
				]
			}
		`,
		),
		)
		if err != nil {
			return
		}
	})
	return httptest.NewServer(mux)
}
