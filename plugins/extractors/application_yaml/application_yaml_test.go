//go:build plugins
// +build plugins

package applicationyaml

import (
	"context"
	"os"
	"testing"

	"github.com/raystack/meteor/models"
	meteorv1beta1 "github.com/raystack/meteor/models/raystack/meteor/v1beta1"
	"github.com/raystack/meteor/plugins"
	"github.com/raystack/meteor/test/mocks"
	testutils "github.com/raystack/meteor/test/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	urnScope = "test-application"
)

var ctx = context.Background()

func TestInit(t *testing.T) {
	extr := New(testutils.Logger)

	for _, c := range []plugins.Config{
		{
			URNScope:  urnScope,
			RawConfig: map[string]any{},
		},
		{
			URNScope: urnScope,
			RawConfig: map[string]any{
				"file": "not-exist",
			},
		},
		{
			URNScope: urnScope,
			RawConfig: map[string]any{
				"file":       "testdata/application.onlyrequired.yaml",
				"env_prefix": "",
			},
		},
	} {
		err := extr.Init(ctx, c)
		assert.ErrorAs(t, err, &plugins.InvalidConfigError{}, "should return error if config is invalid")
	}
}

func TestExtract(t *testing.T) {
	extr := New(testutils.Logger)

	os.Clearenv()
	cases := []struct {
		name          string
		env           map[string]string
		cfg           plugins.Config
		expectedEnts  []*meteorv1beta1.Entity
		expectedEdges []*meteorv1beta1.Edge
		errStr        string
	}{
		{
			name: "InvalidTemplate",
			cfg: plugins.Config{
				URNScope: urnScope,
				RawConfig: map[string]any{
					"file": "testdata/application.invalidtmpl.yaml",
				},
			},
			errStr: "application_yaml extract: parse file: template: application.invalidtmpl.yaml:2: unclosed action started at application.invalidtmpl.yaml:1",
		},
		{
			name: "TemplateExecFailure",
			cfg: plugins.Config{
				URNScope: urnScope,
				RawConfig: map[string]any{
					"file": "testdata/application.execfail.yaml",
				},
			},
			errStr: `application_yaml extract: inject env vars: template: application.execfail.yaml:1:10: executing "application.execfail.yaml" at <.project_name>: map has no entry for key "project_name"`,
		},
		{
			name: "UnknownField",
			cfg: plugins.Config{
				URNScope: urnScope,
				RawConfig: map[string]any{
					"file": "testdata/application.unknownfld.yaml",
				},
			},
			errStr: "application_yaml extract: load application: yaml: unmarshal errors:\n  line 1: field unknown_field not found in type applicationyaml.Application",
		},
		{
			name: "ValidationFailure/Name",
			cfg: plugins.Config{
				URNScope: urnScope,
				RawConfig: map[string]any{
					"file": "testdata/application.validationfail.yaml",
				},
			},
			errStr: "application_yaml extract: validate: Key: 'Application.name' Error:Field validation for 'name' failed on the 'required' tag",
		},
		{
			name: "ValidationFailure/ID",
			cfg: plugins.Config{
				URNScope: urnScope,
				RawConfig: map[string]any{
					"file": "testdata/application.validationfail.yaml",
				},
			},
			errStr: "Key: 'Application.id' Error:Field validation for 'id' failed on the 'required' tag",
		},
		{
			name: "ValidationFailure/URL",
			cfg: plugins.Config{
				URNScope: urnScope,
				RawConfig: map[string]any{
					"file": "testdata/application.validationfail.yaml",
				},
			},
			errStr: "Key: 'Application.url' Error:Field validation for 'url' failed on the 'url' tag",
		},
		{
			name: "OnlyRequiredFields",
			cfg: plugins.Config{
				URNScope: urnScope,
				RawConfig: map[string]any{
					"file": "testdata/application.onlyrequired.yaml",
				},
			},
			expectedEnts: []*meteorv1beta1.Entity{
				models.NewEntity(
					"urn:application_yaml:test-application:application:test",
					"application",
					"test",
					"application_yaml",
					map[string]any{
						"id": "test-id",
					},
				),
			},
		},
		{
			name: "Detailed",
			cfg: plugins.Config{
				URNScope: urnScope,
				RawConfig: map[string]any{
					"file": "testdata/application.detailed.yaml",
				},
			},
			expectedEnts: func() []*meteorv1beta1.Entity {
				e := models.NewEntity(
					"urn:application_yaml:test-application:application:test",
					"application",
					"test",
					"application_yaml",
					map[string]any{
						"id":          "test-id",
						"version":     "c23sdf6",
						"url":         "http://company.com/myteam/test",
						"create_time": "2006-01-02T15:04:05Z",
						"update_time": "2006-01-02T15:04:05Z",
						"labels":      map[string]any{"x": "y"},
					},
				)
				e.Description = "My incredible project"
				return []*meteorv1beta1.Entity{e}
			}(),
			expectedEdges: []*meteorv1beta1.Edge{
				models.OwnerEdge(
					"urn:application_yaml:test-application:application:test",
					"urn:user:myteam@company.com",
					"application_yaml",
				),
				models.DerivedFromEdge(
					"urn:application_yaml:test-application:application:test",
					"urn:bigquery:bq-raw-internal:table:bq-raw-internal:dagstream.production_feast09_s2id13_30min_demand",
					"application_yaml",
				),
				models.DerivedFromEdge(
					"urn:application_yaml:test-application:application:test",
					"urn:kafka:int-dagstream-kafka.yonkou.io:topic:staging_feast09_s2id13_30min_demand",
					"application_yaml",
				),
				models.GeneratesEdge(
					"urn:application_yaml:test-application:application:test",
					"urn:kafka:1-my-kafka.company.com,2-my-kafka.company.com:topic:staging_feast09_mixed_granularity_demand_forecast_3es",
					"application_yaml",
				),
			},
		},
		{
			name: "WithEnvVars",
			env: map[string]string{
				"CI_PROJECT_NAME":     "test",
				"CIPROJECT_URL":       "http://company.com/myteam/test",
				"CI_COMMIT_SHORT_SHA": "c23sdf6",
				"BI_COMMIT_SHORT_SHA": "randomsha",
			},
			cfg: plugins.Config{
				URNScope: urnScope,
				RawConfig: map[string]any{
					"file": "testdata/application.envvars.yaml",
				},
			},
			expectedEnts: func() []*meteorv1beta1.Entity {
				e := models.NewEntity(
					"urn:application_yaml:test-application:application:test",
					"application",
					"test",
					"application_yaml",
					map[string]any{
						"id":      "test-id",
						"version": "c23sdf6",
						"url":     "http://company.com/myteam/test",
					},
				)
				e.Description = "My incredible project"
				return []*meteorv1beta1.Entity{e}
			}(),
			expectedEdges: []*meteorv1beta1.Edge{
				models.OwnerEdge(
					"urn:application_yaml:test-application:application:test",
					"urn:user:myteam@company.com",
					"application_yaml",
				),
			},
		},
		{
			name: "WithEnvVarsPrefix",
			env: map[string]string{
				"GCI_PROJECT_NAME":     "test",
				"GCIPROJECT_URL":       "http://company.com/myteam/test",
				"GCI_COMMIT_SHORT_SHA": "c23sdf6",
				"GBI_COMMIT_SHORT_SHA": "randomsha",
			},
			cfg: plugins.Config{
				URNScope: urnScope,
				RawConfig: map[string]any{
					"file":       "testdata/application.envvars.yaml",
					"env_prefix": "GCI",
				},
			},
			expectedEnts: func() []*meteorv1beta1.Entity {
				e := models.NewEntity(
					"urn:application_yaml:test-application:application:test",
					"application",
					"test",
					"application_yaml",
					map[string]any{
						"id":      "test-id",
						"version": "c23sdf6",
						"url":     "http://company.com/myteam/test",
					},
				)
				e.Description = "My incredible project"
				return []*meteorv1beta1.Entity{e}
			}(),
			expectedEdges: []*meteorv1beta1.Edge{
				models.OwnerEdge(
					"urn:application_yaml:test-application:application:test",
					"urn:user:myteam@company.com",
					"application_yaml",
				),
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for k, v := range tc.env {
				require.NoError(t, os.Setenv(k, v))
			}
			t.Cleanup(os.Clearenv)

			require.NoError(t, extr.Init(ctx, tc.cfg))

			emitter := mocks.NewEmitter()
			err := extr.Extract(ctx, emitter.Push)
			if tc.errStr != "" {
				assert.ErrorContains(t, err, tc.errStr)
			} else {
				assert.NoError(t, err)
			}

			testutils.AssertEqualProtos(t, tc.expectedEnts, emitter.GetAllEntities())
			if tc.expectedEdges != nil {
				testutils.AssertEqualProtos(t, tc.expectedEdges, emitter.GetAllEdges())
			}
		})
	}
}
