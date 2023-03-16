//go:build plugins
// +build plugins

package merlin

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	testutils "github.com/goto/meteor/test/utils"
	"github.com/stretchr/testify/assert"
	"golang.org/x/oauth2"
)

var (
	ctx       = context.Background()
	credsJSON = []byte(`{"type":"service_account","project_id":"company-data-platform","private_key_id":"698vxv308w3i68p938040bz817r95b1e0k4kmvqs","private_key":"-----BEGIN PRIVATE KEY-----\nMIIEoQIBAAKCAQBVzQ0WPuaqdwMNapCGKdKUR/MOgWNByruT60SJwd5lY/2Sjx1w\nQ4sJ6xk/+Tz7bT3CgNBAPQ+rZfLD2fdQJIBeYElRcHw6a2PA/6TaX2e4qq0+5xk3\ngnItlqZm0hQElZd76LNlMcItHNmneLICowTOdzl0hUd2IgrqLB545v3KOGfwoEAp\nz3mPm/iF1+zTPWy041w7ajvWK2N3mRygKoP79ne2gDuN2+QHmW8wPFzQ3pdQZU65\n1npgP9N4wRHQT8vowTUSYdSRZG1p1MPKCXsrqhMUv7yLdrOcacAvcZqYOcMhJQ1p\nbYpsinDR65ARDduMeKoEUkFb3hf2zPUcdYNhAgMBAAECggEAQldOxCGUlr94o7n+\nz02tHavYGiIfDfLkQIYLs3wsKjc7DEQOHgyLh/q4xkc/SKR5uVeCLflIkV09bQOu\nftAKVW6bohWYaE86jTLdU1+rQhTt6ZIkZFA/WlJ+jUfn5HeJ7mvJsffcTKde/2eK\nNBG6GK4Exbx7ubKuv8unMBJiryUycioPykWZEVYl72+0IBsKCQOX39Fd/pgJF9jL\nFPelgCsrvPA/3lodgQu3m8VENlu4G6z3kPQghAvI37xC9NlUNVvx1yxCukQhf0zQ\nQ55kUTwgZ9sIGGcI/2K6H1YHv+m3vnM5D5iL9eTHn1HnlGtplQJhmhKjCxXIpbHx\nQToOwQKBgQCcEZP6H3nq3eH7d5ro1fvA6YEoERfzIzaU4Kk3Sb9e1tXjYSz8ccNv\nK3gZsHV2YZy3q9mCYnc0oPwwx5dSwhzpOrBrwvyopPbkKpD9WCXtZtRkwRTN7CXR\nE+2eSSpu2y14SKysPQoDZmyJo8bs7rseLQTiZeUPlYdlP6adOGSX+QKBgQCMvVqE\n6nbX41DcLJuUxT026T9zncnpRu3gkfyY0O5QF8/Vcq6y5LxdQtyMNbcbkDY8isAM\nwTP4KaXPul38TOCjfG3MODDbzmeQ27qKL/9Ueyi812BN4XIrpguoPKgFtlyi1JNH\nZiUtimedOoNG4LuuDEqeNyW1Qm/WlQu5fqKwqQKBgGscuVW6Ep+6RuWisePJMO62\nk9ke2jQZ39UP17NFXx1FDyjuQcTEg2AiElx3OjbUSY3ZWP/eenfZYRxNb7Lx3IvJ\nptleyq8oAPaZrEbkH6uunmjEB3ZI869qIPQ4vPG2ZZ+fKTtQ7TVmL2nLyLRGKJBO\nT4LecfZfJry7katnz8ppAoGAI0FXyI33YVNHMTBXdOgH0paRV4QCTVaARk4rqZhE\n6nlcjcqhqpyT9wTFvLXD/bqda4MSYt+PBi5go+26l3Ymm62Sz6KP0rAcz3PLgcxO\nOLp1VQDa1geQkxCQQP+Y032ALSX1EuCqlYLjO8aplfq76PiZRJLp9kMDQwypGDl5\nxakCgYAm7pO0LA/hTvdrZ7zGUIfTTZxf1qD+W0iUh2MtyaZM9uQhDoaahf7f2TT/\nt2+wlyIlHMdUxfDYf8U5owl9IysqaPMZsQmYNgYmXpW8/AhNcKFnslyrtd57Of3C\nlFHpNwfjNlxDTsql2kWbcwJbY0EblPRItplE7gDlUvfgSNTj+g==\n-----END PRIVATE KEY-----\n","client_email":"systems-meteor@company-data-platform.iam.gserviceaccount.com","client_id":"043161688880430795893","auth_uri":"https://accounts.google.com/o/oauth2/auth","token_uri":"https://oauth2.googleapis.com/token","auth_provider_x509_cert_url":"https://www.googleapis.com/oauth2/v1/certs","client_x509_cert_url":"https://www.googleapis.com/robot/v1/metadata/x509/systems-meteor%40company-data-platform.iam.gserviceaccount.com"}`)
)

func TestNewClient(t *testing.T) {
	cases := []struct {
		name   string
		params ClientParams
		errStr string
	}{
		{
			name: "Valid",
			params: ClientParams{
				BaseURL:            "http://company.com/api/merlin/",
				ServiceAccountJSON: credsJSON,
			},
		},
		{
			name:   "WithoutCredentials",
			params: ClientParams{BaseURL: "http://company.com/api/merlin/"},
			errStr: "google: could not find default credentials",
		},
		{
			name: "WithInvalidCredentials",
			params: ClientParams{
				BaseURL:            "http://company.com/api/merlin/",
				ServiceAccountJSON: []byte(`{"chuck": "norris"}`),
			},
			errStr: "google credentials from JSON: missing 'type' field in credentials",
		},
		{
			name: "WithInvalidURL",
			params: ClientParams{
				BaseURL:            "Gintama - Yorozuya Gin-chan",
				ServiceAccountJSON: credsJSON,
			},
			errStr: `invalid input: parse "http://Gintama - Yorozuya Gin-chan": invalid character " " in host name`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewClient(ctx, tc.params)
			if tc.errStr != "" {
				assert.ErrorContains(t, err, tc.errStr)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestProjects(t *testing.T) {
	cases := []struct {
		name        string
		status      int
		response    interface{}
		expected    []Project
		expectedErr error
	}{
		{
			name:     "Valid",
			status:   http.StatusOK,
			response: `[{"id":1,"name":"one-piece","mlflow_tracking_url":"http://mlflow.company.com","administrators":["s-ds-gitlab-runner@company-staging.iam.gserviceaccount.com","gol.d.roger@onepiece.com"],"readers":null,"team":"pirates","stream":"roger","labels":null,"created_at":"2019-11-19T11:05:11.41501Z","updated_at":"2022-06-23T09:31:25.834714Z"},{"id":100,"name":"strongest-man","mlflow_tracking_url":"http://mlflow.company.com","administrators":["edward.newgate@onepiece.com"],"readers":["ace.d.portgas@onepiece.com"],"team":"pirates","stream":"whitebeard","labels":null,"created_at":"2021-09-20T05:20:53.540571Z","updated_at":"2021-09-20T05:20:53.540571Z"},{"id":200,"name":"kurohige","mlflow_tracking_url":"http://mlflow.company.com","administrators":["teach.d.marshall@onepiece.com","s-ds-gitlab-runner@company-staging.iam.gserviceaccount.com"],"readers":["jesus.burgess@gojek.com"],"team":"pirates","stream":"blackbeard","labels":null,"created_at":"2021-10-29T02:12:43.142433Z","updated_at":"2022-05-08T17:56:32.924721Z"}]`,
			expected: []Project{
				{
					ID:                1,
					Name:              "one-piece",
					MlflowTrackingURL: "http://mlflow.company.com",
					Administrators:    []string{"s-ds-gitlab-runner@company-staging.iam.gserviceaccount.com", "gol.d.roger@onepiece.com"},
					Team:              "pirates",
					Stream:            "roger",
					CreatedAt:         "2019-11-19T11:05:11.41501Z",
					UpdatedAt:         "2022-06-23T09:31:25.834714Z",
				},
				{
					ID:                100,
					Name:              "strongest-man",
					MlflowTrackingURL: "http://mlflow.company.com",
					Administrators:    []string{"edward.newgate@onepiece.com"},
					Readers:           []string{"ace.d.portgas@onepiece.com"},
					Team:              "pirates",
					Stream:            "whitebeard",
					CreatedAt:         "2021-09-20T05:20:53.540571Z",
					UpdatedAt:         "2021-09-20T05:20:53.540571Z",
				},
				{
					ID:                200,
					Name:              "kurohige",
					MlflowTrackingURL: "http://mlflow.company.com",
					Administrators:    []string{"teach.d.marshall@onepiece.com", "s-ds-gitlab-runner@company-staging.iam.gserviceaccount.com"},
					Readers:           []string{"jesus.burgess@gojek.com"},
					Team:              "pirates",
					Stream:            "blackbeard",
					CreatedAt:         "2021-10-29T02:12:43.142433Z",
					UpdatedAt:         "2022-05-08T17:56:32.924721Z",
				},
			},
		},
		{
			name:     "ValidWithNoProjects",
			status:   http.StatusOK,
			response: `[]`,
			expected: []Project{},
		},
		{
			name:     "Unavailable",
			status:   http.StatusServiceUnavailable,
			response: `Service unavailable, go away`,
			expectedErr: &APIError{
				Method:   http.MethodGet,
				Endpoint: "/api/merlin/v1/projects",
				Status:   http.StatusServiceUnavailable,
				Msg:      "Service unavailable, go away",
			},
		},
		{
			name:     "UnavailableWithJSONResponse",
			status:   http.StatusServiceUnavailable,
			response: `{"error": "Service unavailable, go away"}`,
			expectedErr: &APIError{
				Method:   http.MethodGet,
				Endpoint: "/api/merlin/v1/projects",
				Status:   http.StatusServiceUnavailable,
				Msg:      "Service unavailable, go away",
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			token := "MyIncrediblyPowerfulAccessToken"
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, r.Method, http.MethodGet)
				assert.Equal(t, r.URL.Path, "/api/merlin/v1/projects")
				assert.Equal(t, r.Header.Get("Authorization"), "Bearer "+token)

				testutils.Respond(t, w, tc.status, tc.response)
			}))
			defer srv.Close()

			c, err := NewClient(ctxWithClient(t, token), ClientParams{
				BaseURL:            srv.URL + "/api/merlin",
				ServiceAccountJSON: credsJSON,
				Timeout:            1 * time.Second,
			})
			assert.NoError(t, err)

			projects, err := c.Projects(ctx)
			if tc.expectedErr != nil {
				assert.ErrorIs(t, err, tc.expectedErr)
				assert.Nil(t, projects)
				return
			}

			assert.NoError(t, err)
			assert.Equal(t, tc.expected, projects)
		})
	}

	t.Run("Timeout", func(t *testing.T) {
		timeout := 100 * time.Millisecond
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			time.Sleep(timeout * 2)
		}))
		defer srv.Close()

		c, err := NewClient(ctxWithClient(t, "MyIncrediblyPowerfulAccessToken"), ClientParams{
			BaseURL:            srv.URL + "/api/merlin",
			ServiceAccountJSON: credsJSON,
			Timeout:            timeout,
		})
		assert.NoError(t, err)

		_, err = c.Projects(ctx)
		assert.ErrorIs(t, err, context.DeadlineExceeded)
	})
}

func TestModels(t *testing.T) {
	cases := []struct {
		name        string
		projectID   int64
		status      int
		response    interface{}
		expected    []Model
		expectedErr error
		assertErr   func(*testing.T, error)
	}{
		{
			name:      "Valid",
			projectID: 9999,
			status:    http.StatusOK,
			response:  `[{"id":12,"project_id":1,"mlflow_experiment_id":12,"name":"model.bst","type":"xgboost","mlflow_url":"http://mlflow.company.com/#/experiments/12","endpoints":[],"created_at":"2020-01-10T08:18:46.887143Z","updated_at":"2020-01-10T08:18:46.887143Z"},{"id":80,"project_id":1,"mlflow_experiment_id":83,"name":"resource-request","type":"sklearn","mlflow_url":"http://mlflow.company.com/#/experiments/83","endpoints":[{"id":450,"status":"terminated","url":"resource-request.sample.models.company.com","rule":{"destinations":[{"version_endpoint_id":"ad247eca-6e37-4d7f-90ba-f482890b5896","version_endpoint":{"id":"ad247eca-6e37-4d7f-90ba-f482890b5896","version_id":2,"status":"running","url":"http://resource-request-2.sample.models.company.com/v1/models/resource-request-2","service_name":"resource-request-2-predictor-default.sample.models.company.com","environment_name":"staging","monitoring_url":"","message":"","env_vars":null,"transformer":{"enabled":false,"transformer_type":"custom","image":"","command":"","args":"","env_vars":[],"created_at":"2022-05-30T07:52:32.790496Z","updated_at":"2022-05-30T07:52:51.047734Z"},"deployment_mode":"","created_at":"2022-05-30T07:43:21.252389Z","updated_at":"2022-05-30T07:52:51.046666Z"},"weight":100}]},"environment_name":"staging","created_at":"2022-05-30T07:54:04.137016Z","updated_at":"2022-06-14T07:12:09.538048Z"}],"created_at":"2020-04-23T10:07:40.105711Z","updated_at":"2022-06-14T07:12:09.536419Z"},{"id":689,"project_id":1,"mlflow_experiment_id":692,"name":"pyfunc-standard-transfo","type":"pyfunc","mlflow_url":"http://mlflow.company.com/#/experiments/692","endpoints":[{"id":174,"status":"terminated","url":"pyfunc-standard-transfo.sample.models.company.com","rule":{"destinations":[{"version_endpoint_id":"b53752c9-a4cb-48ac-b955-de468b75b2eb","version_endpoint":{"id":"b53752c9-a4cb-48ac-b955-de468b75b2eb","version_id":7,"status":"running","url":"http://pyfunc-standard-transfo-7.sample.models.company.com/v1/models/pyfunc-standard-transfo-7","service_name":"pyfunc-standard-transfo-7-predictor-default.sample.models.company.com","environment_name":"staging","monitoring_url":"","message":"","env_vars":[{"name":"MODEL_NAME","value":"pyfunc-standard-transfo-7"},{"name":"MODEL_DIR","value":"gs://mlp/mlflow/692/061ew38v3b7kp088s9b49kzr68v5ixvm3/artifacts/model"},{"name":"WORKERS","value":"1"}],"transformer":{"enabled":true,"transformer_type":"standard","image":"asia.gcr.io/company-staging/merlin-transformer:v0.10.0-rc2","command":"","args":"","env_vars":[{"name":"LOG_LEVEL","value":"DEBUG"},{"name":"STANDARD_TRANSFORMER_CONFIG","value":"{\"transformerConfig\":{\"feast\":[{\"project\":\"default\",\"entities\":[{\"name\":\"merchant_id\",\"valueType\":\"STRING\",\"jsonPath\":\"$.merchants[*].id\"}],\"features\":[{\"name\":\"merchant_t1_discovery:t1_estimate\",\"valueType\":\"DOUBLE\",\"defaultValue\":\"0\"}]}]}}"},{"name":"FEAST_FEATURE_STATUS_MONITORING_ENABLED","value":"true"},{"name":"FEAST_FEATURE_VALUE_MONITORING_ENABLED","value":"true"}],"created_at":"2021-02-05T05:26:42.759879Z","updated_at":"2021-02-05T05:40:20.092802Z"},"deployment_mode":"","created_at":"2021-02-05T05:26:42.768235Z","updated_at":"2021-02-05T05:40:20.091784Z"},"weight":100}]},"environment_name":"staging","created_at":"2021-02-05T08:33:26.204561Z","updated_at":"2021-07-22T08:13:34.64483Z"}],"created_at":"2021-02-04T13:05:31.593956Z","updated_at":"2021-07-22T08:13:34.642949Z"}]`,
			expected: []Model{
				{
					ID:                 12,
					ProjectID:          1,
					MlflowExperimentID: 12,
					Name:               "model.bst",
					Type:               "xgboost",
					MlflowURL:          "http://mlflow.company.com/#/experiments/12",
					Endpoints:          []ModelEndpoint{},
					CreatedAt:          time.Date(2020, time.January, 10, 8, 18, 46, 887143000, time.UTC),
					UpdatedAt:          time.Date(2020, time.January, 10, 8, 18, 46, 887143000, time.UTC),
				},
				{
					ID:                 80,
					ProjectID:          1,
					MlflowExperimentID: 83,
					Name:               "resource-request",
					Type:               "sklearn",
					MlflowURL:          "http://mlflow.company.com/#/experiments/83",
					Endpoints: []ModelEndpoint{{
						ID:     450,
						Status: "terminated",
						URL:    "resource-request.sample.models.company.com",
						Rule: ModelEndpointRule{Destinations: []ModelEndpointRuleDestination{{
							VersionEndpointID: "ad247eca-6e37-4d7f-90ba-f482890b5896",
							VersionEndpoint: &VersionEndpoint{
								ID:              "ad247eca-6e37-4d7f-90ba-f482890b5896",
								VersionID:       2,
								Status:          "running",
								URL:             "http://resource-request-2.sample.models.company.com/v1/models/resource-request-2",
								ServiceName:     "resource-request-2-predictor-default.sample.models.company.com",
								EnvironmentName: "staging",
								Transformer: Transformer{
									TransformerType: "custom",
									EnvVars:         []EnvVar{},
									CreatedAt:       time.Date(2022, time.May, 30, 7, 52, 32, 790496000, time.UTC),
									UpdatedAt:       time.Date(2022, time.May, 30, 7, 52, 51, 47734000, time.UTC),
								},
								CreatedAt: time.Date(2022, time.May, 30, 7, 43, 21, 252389000, time.UTC),
								UpdatedAt: time.Date(2022, time.May, 30, 7, 52, 51, 46666000, time.UTC),
							},
							Weight: 100,
						}}},
						EnvironmentName: "staging",
						CreatedAt:       time.Date(2022, time.May, 30, 7, 54, 4, 137016000, time.UTC),
						UpdatedAt:       time.Date(2022, time.June, 14, 7, 12, 9, 538048000, time.UTC),
					}},
					CreatedAt: time.Date(2020, time.April, 23, 10, 7, 40, 105711000, time.UTC),
					UpdatedAt: time.Date(2022, time.June, 14, 7, 12, 9, 536419000, time.UTC),
				},
				{
					ID:                 689,
					ProjectID:          1,
					MlflowExperimentID: 692,
					Name:               "pyfunc-standard-transfo",
					Type:               "pyfunc",
					MlflowURL:          "http://mlflow.company.com/#/experiments/692",
					Endpoints: []ModelEndpoint{{
						ID:     174,
						Status: "terminated",
						URL:    "pyfunc-standard-transfo.sample.models.company.com",
						Rule: ModelEndpointRule{Destinations: []ModelEndpointRuleDestination{{
							VersionEndpointID: "b53752c9-a4cb-48ac-b955-de468b75b2eb",
							VersionEndpoint: &VersionEndpoint{
								ID:              "b53752c9-a4cb-48ac-b955-de468b75b2eb",
								VersionID:       7,
								Status:          "running",
								URL:             "http://pyfunc-standard-transfo-7.sample.models.company.com/v1/models/pyfunc-standard-transfo-7",
								ServiceName:     "pyfunc-standard-transfo-7-predictor-default.sample.models.company.com",
								EnvironmentName: "staging",
								EnvVars: []EnvVar{
									{Name: "MODEL_NAME", Value: "pyfunc-standard-transfo-7"},
									{Name: "MODEL_DIR", Value: "gs://mlp/mlflow/692/061ew38v3b7kp088s9b49kzr68v5ixvm3/artifacts/model"},
									{Name: "WORKERS", Value: "1"},
								},
								Transformer: Transformer{
									Enabled:         true,
									TransformerType: "standard",
									Image:           "asia.gcr.io/company-staging/merlin-transformer:v0.10.0-rc2",
									EnvVars: []EnvVar{
										{Name: "LOG_LEVEL", Value: "DEBUG"},
										{Name: "STANDARD_TRANSFORMER_CONFIG", Value: `{"transformerConfig":{"feast":[{"project":"default","entities":[{"name":"merchant_id","valueType":"STRING","jsonPath":"$.merchants[*].id"}],"features":[{"name":"merchant_t1_discovery:t1_estimate","valueType":"DOUBLE","defaultValue":"0"}]}]}}`},
										{Name: "FEAST_FEATURE_STATUS_MONITORING_ENABLED", Value: "true"},
										{Name: "FEAST_FEATURE_VALUE_MONITORING_ENABLED", Value: "true"},
									},
									CreatedAt: time.Date(2021, time.February, 5, 5, 26, 42, 759879000, time.UTC),
									UpdatedAt: time.Date(2021, time.February, 5, 5, 40, 20, 92802000, time.UTC),
								},
								CreatedAt: time.Date(2021, time.February, 5, 5, 26, 42, 768235000, time.UTC),
								UpdatedAt: time.Date(2021, time.February, 5, 5, 40, 20, 91784000, time.UTC),
							},
							Weight: 100,
						}}},
						EnvironmentName: "staging",
						CreatedAt:       time.Date(2021, time.February, 5, 8, 33, 26, 204561000, time.UTC),
						UpdatedAt:       time.Date(2021, time.July, 22, 8, 13, 34, 644830000, time.UTC),
					}},
					CreatedAt: time.Date(2021, time.February, 4, 13, 5, 31, 593956000, time.UTC),
					UpdatedAt: time.Date(2021, time.July, 22, 8, 13, 34, 642949000, time.UTC),
				},
			},
		},
		{
			name:      "ValidWithNoModels",
			projectID: 876,
			status:    http.StatusOK,
			response:  `[]`,
			expected:  []Model{},
		},
		{
			name:      "NotFound",
			projectID: 808,
			status:    http.StatusNotFound,
			response:  `Project with project_id 808 not found`,
			expectedErr: &APIError{
				Method:   http.MethodGet,
				Endpoint: "/api/merlin/v1/projects/808/models",
				Status:   http.StatusNotFound,
				Msg:      "Project with project_id 808 not found",
			},
		},
		{
			name:      "Unavailable",
			projectID: 503,
			status:    http.StatusServiceUnavailable,
			response:  `{"error": "Service unavailable, go away"}`,
			expectedErr: &APIError{
				Method:   http.MethodGet,
				Endpoint: "/api/merlin/v1/projects/503/models",
				Status:   http.StatusServiceUnavailable,
				Msg:      "Service unavailable, go away",
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			token := "MyIncrediblyPowerfulAccessToken"
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, r.Method, http.MethodGet)
				assert.Equal(t, r.URL.Path, fmt.Sprintf("/api/merlin/v1/projects/%d/models", tc.projectID))
				assert.Equal(t, r.Header.Get("Authorization"), "Bearer "+token)

				testutils.Respond(t, w, tc.status, tc.response)
			}))
			defer srv.Close()

			c, err := NewClient(ctxWithClient(t, token), ClientParams{
				BaseURL:            srv.URL + "/api/merlin",
				ServiceAccountJSON: credsJSON,
				Timeout:            1 * time.Second,
			})
			assert.NoError(t, err)

			models, err := c.Models(ctx, tc.projectID)
			if tc.expectedErr != nil {
				assert.ErrorIs(t, err, tc.expectedErr)
				assert.Nil(t, models)
				return
			}

			assert.NoError(t, err)
			assert.Equal(t, tc.expected, models)
		})
	}

	t.Run("Timeout", func(t *testing.T) {
		timeout := 100 * time.Millisecond
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			time.Sleep(timeout * 2)
		}))
		defer srv.Close()

		c, err := NewClient(ctxWithClient(t, "MyIncrediblyPowerfulAccessToken"), ClientParams{
			BaseURL:            srv.URL + "/api/merlin",
			ServiceAccountJSON: credsJSON,
			Timeout:            timeout,
		})
		assert.NoError(t, err)

		_, err = c.Models(ctx, 10)
		assert.ErrorIs(t, err, context.DeadlineExceeded)
	})
}

func TestModelVersion(t *testing.T) {
	cases := []struct {
		name        string
		modelID     int64
		versionID   int64
		status      int
		response    interface{}
		expected    ModelVersion
		expectedErr error
		assertErr   func(*testing.T, error)
	}{
		{
			name:     "Valid",
			modelID:  9999,
			status:   http.StatusOK,
			response: `{"id":2,"model_id":80,"mlflow_run_id":"040ewv8v3b9kpb1rs9i388z86kv7m53x","mlflow_url":"http://mlflow.company.com/#/experiments/83/runs/040ewv8v3b9kpb1rs9i388z86kv7m53x","endpoints":[{"id":"ad247eca-6e37-4d7f-90ba-f482890b5896","version_id":2,"status":"terminated","url":"http://resource-request-2.sample.models.company.com/v1/models/resource-request-2","service_name":"resource-request-2-predictor-default.sample.models.company.com","environment_name":"id-staging","message":"","env_vars":null,"transformer":{"enabled":false,"transformer_type":"custom","image":"","command":"","args":"","env_vars":[],"created_at":"2022-05-30T07:52:32.790496Z","updated_at":"2022-06-14T07:12:13.513532Z"},"deployment_mode":"serverless","created_at":"2022-05-30T07:43:21.252389Z","updated_at":"2022-06-14T07:12:13.512746Z"}],"labels":null,"created_at":"2022-05-30T07:43:18.120248Z","updated_at":"2022-05-30T07:43:18.120248Z"}`,
			expected: ModelVersion{
				ID:          2,
				ModelD:      80,
				MlflowRunID: "040ewv8v3b9kpb1rs9i388z86kv7m53x",
				MlflowURL:   "http://mlflow.company.com/#/experiments/83/runs/040ewv8v3b9kpb1rs9i388z86kv7m53x",
				Endpoints: []VersionEndpoint{{
					ID:              "ad247eca-6e37-4d7f-90ba-f482890b5896",
					VersionID:       2,
					Status:          "terminated",
					URL:             "http://resource-request-2.sample.models.company.com/v1/models/resource-request-2",
					ServiceName:     "resource-request-2-predictor-default.sample.models.company.com",
					EnvironmentName: "id-staging",
					EnvVars:         []EnvVar(nil),
					Transformer: Transformer{
						TransformerType: "custom",
						EnvVars:         []EnvVar{},
						CreatedAt:       time.Date(2022, time.May, 30, 7, 52, 32, 790496000, time.UTC),
						UpdatedAt:       time.Date(2022, time.June, 14, 7, 12, 13, 513532000, time.UTC),
					},
					DeploymentMode: "serverless",
					CreatedAt:      time.Date(2022, time.May, 30, 7, 43, 21, 252389000, time.UTC),
					UpdatedAt:      time.Date(2022, time.June, 14, 7, 12, 13, 512746000, time.UTC),
				}},
				Labels:    map[string]string(nil),
				CreatedAt: time.Date(2022, time.May, 30, 7, 43, 18, 120248000, time.UTC),
				UpdatedAt: time.Date(2022, time.May, 30, 7, 43, 18, 120248000, time.UTC),
			},
		},
		{
			name:      "NotFound",
			modelID:   808,
			versionID: 202,
			status:    http.StatusNotFound,
			response:  `{"error":"Model version 808 for version 202"}`,
			expectedErr: &APIError{
				Method:   http.MethodGet,
				Endpoint: "/api/merlin/v1/models/808/versions/202",
				Status:   http.StatusNotFound,
				Msg:      "Model version 808 for version 202",
			},
		},
		{
			name:      "Unavailable",
			modelID:   503,
			versionID: 3,
			status:    http.StatusServiceUnavailable,
			response:  `Service unavailable, go away`,
			expectedErr: &APIError{
				Method:   http.MethodGet,
				Endpoint: "/api/merlin/v1/models/503/versions/3",
				Status:   http.StatusServiceUnavailable,
				Msg:      "Service unavailable, go away",
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			token := "MyIncrediblyPowerfulAccessToken"
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, r.Method, http.MethodGet)
				assert.Equal(t, r.URL.Path, fmt.Sprintf(
					"/api/merlin/v1/models/%d/versions/%d", tc.modelID, tc.versionID,
				))
				assert.Equal(t, r.Header.Get("Authorization"), "Bearer "+token)

				testutils.Respond(t, w, tc.status, tc.response)
			}))
			defer srv.Close()

			c, err := NewClient(ctxWithClient(t, token), ClientParams{
				BaseURL:            srv.URL + "/api/merlin",
				ServiceAccountJSON: credsJSON,
				Timeout:            1 * time.Second,
			})
			assert.NoError(t, err)

			mdlv, err := c.ModelVersion(ctx, tc.modelID, tc.versionID)
			if tc.expectedErr != nil {
				assert.ErrorIs(t, err, tc.expectedErr)
				assert.Zero(t, mdlv)
				return
			}

			assert.NoError(t, err)
			assert.Equal(t, tc.expected, mdlv)
		})
	}

	t.Run("Timeout", func(t *testing.T) {
		timeout := 100 * time.Millisecond
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			time.Sleep(timeout * 2)
		}))
		defer srv.Close()

		c, err := NewClient(ctxWithClient(t, "MyIncrediblyPowerfulAccessToken"), ClientParams{
			BaseURL:            srv.URL + "/api/merlin",
			ServiceAccountJSON: credsJSON,
			Timeout:            timeout,
		})
		assert.NoError(t, err)

		_, err = c.ModelVersion(ctx, 1, 2)
		assert.ErrorIs(t, err, context.DeadlineExceeded)
	})
}

func ctxWithClient(t *testing.T, token string) context.Context {
	return context.WithValue(ctx, oauth2.HTTPClient, &http.Client{
		Transport: mockOauthRoundTripper{
			T:           t,
			AccessToken: token,
			Base:        http.DefaultTransport,
		},
	})
}

type mockOauthRoundTripper struct {
	T           *testing.T
	AccessToken string
	Base        http.RoundTripper
}

func (m mockOauthRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if m.match(req) {
		return &http.Response{
			Status:     http.StatusText(http.StatusOK),
			StatusCode: http.StatusOK,
			Proto:      "HTTP/1.1",
			ProtoMajor: 1,
			ProtoMinor: 1,
			Header:     make(http.Header),
			Body: testutils.ValueAsJSONReader(m.T, map[string]interface{}{
				"access_token": m.AccessToken,
				"expires_in":   3599,
				"token_type":   "Bearer",
			}),
			Uncompressed: true,
		}, nil
	}

	return m.Base.RoundTrip(req)
}

func (m mockOauthRoundTripper) match(r *http.Request) bool {
	return r.Method == http.MethodPost &&
		r.URL.Host == "oauth2.googleapis.com" &&
		r.URL.Path == "/token"
}

func (e *APIError) Is(err error) bool {
	// Override comparison just in tests to simplify matching without host in
	// the endpoint.
	other, ok := err.(*APIError)
	if !ok {
		return false
	}

	return e.Method == other.Method &&
		strings.Contains(e.Endpoint, other.Endpoint) &&
		e.Status == other.Status &&
		e.Msg == other.Msg
}
