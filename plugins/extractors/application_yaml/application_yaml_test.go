//go:build plugins
// +build plugins

package applicationyaml

import (
	"context"
	"os"
	"testing"
	"time"

	v1beta2 "github.com/goto/meteor/models/gotocompany/assets/v1beta2"
	"github.com/goto/meteor/plugins"
	"github.com/goto/meteor/test/mocks"
	testutils "github.com/goto/meteor/test/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
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
			RawConfig: map[string]interface{}{},
		},
		{
			URNScope: urnScope,
			RawConfig: map[string]interface{}{
				"file": "not-exist",
			},
		},
		{
			URNScope: urnScope,
			RawConfig: map[string]interface{}{
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

	ts := func(s string) *timestamppb.Timestamp {
		ts, err := time.Parse(time.RFC3339, s)
		require.NoError(t, err)
		return timestamppb.New(ts)
	}

	os.Clearenv()
	cases := []struct {
		name     string
		env      map[string]string
		cfg      plugins.Config
		expected []*v1beta2.Asset
		errStr   string
	}{
		{
			name: "InvalidTemplate",
			cfg: plugins.Config{
				URNScope: urnScope,
				RawConfig: map[string]interface{}{
					"file": "testdata/application.invalidtmpl.yaml",
				},
			},
			errStr: "application_yaml extract: parse file: template: application.invalidtmpl.yaml:2: unclosed action started at application.invalidtmpl.yaml:1",
		},
		{
			name: "TemplateExecFailure",
			cfg: plugins.Config{
				URNScope: urnScope,
				RawConfig: map[string]interface{}{
					"file": "testdata/application.execfail.yaml",
				},
			},
			errStr: `application_yaml extract: inject env vars: template: application.execfail.yaml:1:10: executing "application.execfail.yaml" at <.project_name>: map has no entry for key "project_name"`,
		},
		{
			name: "UnknownField",
			cfg: plugins.Config{
				URNScope: urnScope,
				RawConfig: map[string]interface{}{
					"file": "testdata/application.unknownfld.yaml",
				},
			},
			errStr: "application_yaml extract: load application: yaml: unmarshal errors:\n  line 1: field unknown_field not found in type applicationyaml.Application",
		},
		{
			name: "ValidationFailure/Name",
			cfg: plugins.Config{
				URNScope: urnScope,
				RawConfig: map[string]interface{}{
					"file": "testdata/application.validationfail.yaml",
				},
			},
			errStr: "application_yaml extract: validate: Key: 'Application.name' Error:Field validation for 'name' failed on the 'required' tag",
		},
		{
			name: "ValidationFailure/ID",
			cfg: plugins.Config{
				URNScope: urnScope,
				RawConfig: map[string]interface{}{
					"file": "testdata/application.validationfail.yaml",
				},
			},
			errStr: "Key: 'Application.id' Error:Field validation for 'id' failed on the 'required' tag",
		},
		{
			name: "ValidationFailure/URL",
			cfg: plugins.Config{
				URNScope: urnScope,
				RawConfig: map[string]interface{}{
					"file": "testdata/application.validationfail.yaml",
				},
			},
			errStr: "Key: 'Application.url' Error:Field validation for 'url' failed on the 'url' tag",
		},
		{
			name: "OnlyRequiredFields",
			cfg: plugins.Config{
				URNScope: urnScope,
				RawConfig: map[string]interface{}{
					"file": "testdata/application.onlyrequired.yaml",
				},
			},
			expected: []*v1beta2.Asset{{
				Urn:     "urn:application_yaml:test-application:application:test",
				Name:    "test",
				Type:    "application",
				Service: "application_yaml",
				Data:    testutils.BuildAny(t, &v1beta2.Application{Id: "test-id", Attributes: &structpb.Struct{}}),
				Lineage: &v1beta2.Lineage{},
			}},
		},
		{
			name: "Detailed",
			cfg: plugins.Config{
				URNScope: urnScope,
				RawConfig: map[string]interface{}{
					"file": "testdata/application.detailed.yaml",
				},
			},
			expected: []*v1beta2.Asset{{
				Urn:         "urn:application_yaml:test-application:application:test",
				Name:        "test",
				Service:     "application_yaml",
				Type:        "application",
				Url:         "http://company.com/myteam/test",
				Description: "My incredible project",
				Data: testutils.BuildAny(t, &v1beta2.Application{
					Id:         "test-id",
					Version:    "c23sdf6",
					Attributes: &structpb.Struct{},
					CreateTime: ts("2006-01-02T15:04:05Z"),
					UpdateTime: ts("2006-01-02T15:04:05Z"),
				}),
				Owners: []*v1beta2.Owner{{
					Urn:   "123",
					Name:  "myteam",
					Email: "myteam@company.com",
				}},
				Lineage: &v1beta2.Lineage{
					Upstreams: []*v1beta2.Resource{
						{Urn: "urn:bigquery:bq-raw-internal:table:bq-raw-internal:dagstream.production_feast09_s2id13_30min_demand"},
						{Urn: "urn:kafka:int-dagstream-kafka.yonkou.io:topic:staging_feast09_s2id13_30min_demand"},
					},
					Downstreams: []*v1beta2.Resource{
						{Urn: "urn:kafka:1-my-kafka.company.com,2-my-kafka.company.com:topic:staging_feast09_mixed_granularity_demand_forecast_3es"},
					},
				},
				Labels: map[string]string{"x": "y"},
			}},
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
				RawConfig: map[string]interface{}{
					"file": "testdata/application.envvars.yaml",
				},
			},
			expected: []*v1beta2.Asset{{
				Urn:         "urn:application_yaml:test-application:application:test",
				Name:        "test",
				Service:     "application_yaml",
				Type:        "application",
				Url:         "http://company.com/myteam/test",
				Description: "My incredible project",
				Data: testutils.BuildAny(t, &v1beta2.Application{
					Id:         "test-id",
					Attributes: &structpb.Struct{},
					Version:    "c23sdf6",
				}),
				Owners: []*v1beta2.Owner{{
					Urn:   "123",
					Name:  "myteam",
					Email: "myteam@company.com",
				}},
				Lineage: &v1beta2.Lineage{},
			}},
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
				RawConfig: map[string]interface{}{
					"file":       "testdata/application.envvars.yaml",
					"env_prefix": "GCI",
				},
			},
			expected: []*v1beta2.Asset{{
				Urn:         "urn:application_yaml:test-application:application:test",
				Name:        "test",
				Service:     "application_yaml",
				Type:        "application",
				Url:         "http://company.com/myteam/test",
				Description: "My incredible project",
				Data: testutils.BuildAny(t, &v1beta2.Application{
					Id:         "test-id",
					Attributes: &structpb.Struct{},
					Version:    "c23sdf6",
				}),
				Owners: []*v1beta2.Owner{{
					Urn:   "123",
					Name:  "myteam",
					Email: "myteam@company.com",
				}},
				Lineage: &v1beta2.Lineage{},
			}},
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

			testutils.AssertEqualProtos(t, tc.expected, emitter.GetAllData())
		})
	}
}
