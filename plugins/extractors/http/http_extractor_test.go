//go:build plugins
// +build plugins

package http

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/MakeNowJust/heredoc"
	v1beta2 "github.com/goto/meteor/models/gotocompany/assets/v1beta2"
	"github.com/goto/meteor/plugins"
	"github.com/goto/meteor/test/mocks"
	testutils "github.com/goto/meteor/test/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const urnScope = "test-http"

var ctx = context.Background()

func TestInit(t *testing.T) {
	cases := []struct {
		name        string
		rawCfg      map[string]interface{}
		expectedErr string
	}{
		{
			name: "ValidMinimal",
			rawCfg: map[string]interface{}{
				"request": map[string]interface{}{
					"url":          "http://example.com/api/v1/endpoint",
					"content_type": "application/json",
					"accept":       "application/json",
				},
				"script": map[string]interface{}{
					"engine": "tengo",
					"source": "// do nothing",
				},
			},
		},
		{
			name:        "ReqURLRequired",
			rawCfg:      map[string]interface{}{},
			expectedErr: "validation for field 'request.url' failed on the 'required' tag",
		},
		{
			name: "ReqURLInvalid",
			rawCfg: map[string]interface{}{
				"request": map[string]interface{}{"url": "invalid_url"},
			},
			expectedErr: "validation for field 'request.url' failed on the 'url' tag",
		},
		{
			name: "ReqQueryParamInvalidKey",
			rawCfg: map[string]interface{}{
				"request": map[string]interface{}{
					"query_params": []QueryParam{{Value: "v"}},
				},
			},
			expectedErr: "validation for field 'request.query_params[0].key' failed on the 'required' tag",
		},
		{
			name: "ReqQueryParamInvalidValue",
			rawCfg: map[string]interface{}{
				"request": map[string]interface{}{
					"query_params": []QueryParam{{Key: "k"}},
				},
			},
			expectedErr: "validation for field 'request.query_params[0].value' failed on the 'required' tag",
		},
		{
			name:        "ReqContentTypeRequired",
			rawCfg:      map[string]interface{}{},
			expectedErr: "validation for field 'request.content_type' failed on the 'required' tag",
		},
		{
			name: "ReqContentTypeUnsupported",
			rawCfg: map[string]interface{}{
				"request": map[string]interface{}{
					"content_type": "application/text",
				},
			},
			expectedErr: "validation for field 'request.content_type' failed on the 'oneof' tag",
		},
		{
			name:        "ReqAcceptRequired",
			rawCfg:      map[string]interface{}{},
			expectedErr: "validation for field 'request.accept' failed on the 'required' tag",
		},
		{
			name: "ReqAcceptUnsupported",
			rawCfg: map[string]interface{}{
				"request": map[string]interface{}{
					"accept": "application/text",
				},
			},
			expectedErr: "validation for field 'request.accept' failed on the 'oneof' tag",
		},
		{
			name: "ReqMethodUnsupported",
			rawCfg: map[string]interface{}{
				"request": map[string]interface{}{
					"method": "PUT",
				},
			},
			expectedErr: "validation for field 'request.method' failed on the 'oneof' tag",
		},
		{
			name: "ReqTimeoutInvalid",
			rawCfg: map[string]interface{}{
				"request": map[string]interface{}{
					"timeout": 1,
				},
			},
			expectedErr: "validation for field 'request.timeout' failed on the 'min' tag",
		},
		{
			name: "SuccessCodesInvalid",
			rawCfg: map[string]interface{}{
				"success_codes": []int{10000},
			},
			expectedErr: "validation for field 'success_codes[0]' failed on the 'lt' tag",
		},
		{
			name:        "ScriptEngineRequired",
			rawCfg:      map[string]interface{}{},
			expectedErr: "validation for field 'script.engine' failed on the 'required' tag",
		},
		{
			name: "ScriptEngineUnsupported",
			rawCfg: map[string]interface{}{
				"script": map[string]string{
					"engine": "mango",
				},
			},
			expectedErr: "validation for field 'script.engine' failed on the 'oneof' tag",
		},
		{
			name:        "ScriptSourceRequired",
			rawCfg:      map[string]interface{}{},
			expectedErr: "validation for field 'script.source' failed on the 'required' tag",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := New(testutils.Logger).Init(ctx, plugins.Config{
				URNScope:  urnScope,
				RawConfig: tc.rawCfg,
			})
			if tc.expectedErr != "" {
				assert.ErrorAs(t, err, &plugins.InvalidConfigError{}, "should return error if config is invalid")
				assert.ErrorContains(t, err, tc.expectedErr)
				return
			}

			assert.NoError(t, err)
		})
	}
}

func TestExtract(t *testing.T) {
	cases := []struct {
		name        string
		rawCfg      map[string]interface{}
		handler     func(t *testing.T, w http.ResponseWriter, r *http.Request)
		expected    []*v1beta2.Asset
		expectedErr string
	}{
		{
			name: "MatchRequestBasic",
			rawCfg: map[string]interface{}{
				"request": map[string]interface{}{
					"url":          "{{serverURL}}/api/v1/endpoint",
					"content_type": "application/json",
					"accept":       "application/json",
				},
				"script": map[string]interface{}{
					"engine": "tengo",
					"source": "// do nothing",
				},
			},
			handler: func(t *testing.T, w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, r.Method, http.MethodGet)
				assert.Equal(t, r.URL.Path, "/api/v1/endpoint")
				assert.Equal(t, r.URL.RawQuery, "")
				h := r.Header
				assert.Equal(t, "", h.Get("Content-Type"))
				assert.Equal(t, "application/json", h.Get("Accept"))
				data, err := io.ReadAll(r.Body)
				assert.NoError(t, err)
				assert.Empty(t, data)

				testutils.Respond(t, w, http.StatusOK, `[]`)
			},
		},
		{
			name: "MatchRequestAdvanced",
			rawCfg: map[string]interface{}{
				"request": map[string]interface{}{
					"url": "{{serverURL}}/api/v1/endpoint?a=1",
					"query_params": []QueryParam{
						{Key: "a", Value: "2"},
						{Key: "a", Value: "3"},
						{Key: "formula", Value: "a=b"},
					},
					"method":       http.MethodPost,
					"headers":      map[string]string{"User-Id": "1a4336bc-bc6a-4972-83c1-d6426b4d79c3"},
					"content_type": "application/json",
					"accept":       "application/json",
					"body": map[string]interface{}{
						"id": "urn:merlin:merlin-stg:model:46.218",
					},
				},
				"success_codes": []int{http.StatusOK, http.StatusNonAuthoritativeInfo},
				"script": map[string]interface{}{
					"engine": "tengo",
					"source": "// do nothing",
				},
			},
			handler: func(t *testing.T, w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, r.Method, http.MethodPost)
				assert.Equal(t, r.URL.Path, "/api/v1/endpoint")
				assert.Equal(t, r.URL.RawQuery, "a=2&a=3&formula=a%3Db")
				h := r.Header
				assert.Equal(t, "application/json", h.Get("Content-Type"))
				assert.Equal(t, "application/json", h.Get("Accept"))
				assert.Equal(t, "1a4336bc-bc6a-4972-83c1-d6426b4d79c3", h.Get("User-Id"))
				data, err := io.ReadAll(r.Body)
				assert.NoError(t, err)
				testutils.AssertJSONEq(t, `{"id": "urn:merlin:merlin-stg:model:46.218"}`, data)

				testutils.Respond(t, w, http.StatusNonAuthoritativeInfo, `[]`)
			},
		},
		{
			name: "5xxResponse",
			rawCfg: map[string]interface{}{
				"request": map[string]interface{}{
					"url":          "{{serverURL}}/api/v1/endpoint",
					"content_type": "application/json",
					"accept":       "application/json",
				},
				"script": map[string]interface{}{
					"engine": "tengo",
					"source": "// do nothing",
				},
			},
			handler: func(t *testing.T, w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusInternalServerError)
			},
			expectedErr: "unsuccessful request: response status code: 500",
		},
		{
			name: "RequestTimeout",
			rawCfg: map[string]interface{}{
				"request": map[string]interface{}{
					"url":          "{{serverURL}}/api/v1/endpoint",
					"content_type": "application/json",
					"accept":       "application/json",
					"timeout":      "50ms",
				},
				"script": map[string]interface{}{
					"engine": "tengo",
					"source": "// do nothing",
				},
			},
			handler: func(t *testing.T, w http.ResponseWriter, r *http.Request) {
				time.Sleep(100 * time.Millisecond)
				testutils.Respond(t, w, http.StatusOK, `[]`)
			},
			expectedErr: "context deadline exceeded",
		},
		{
			name: "AssetFromResponse",
			rawCfg: map[string]interface{}{
				"request": map[string]interface{}{
					"url":          "{{serverURL}}/api/v1/endpoint",
					"content_type": "application/json",
					"accept":       "application/json",
				},
				"script": map[string]interface{}{
					"engine": "tengo",
					"source": heredoc.Doc(`
						body := response.body
						asset := new_asset("user")
						// URN format: "urn:{service}:{scope}:{type}:{id}"
						asset.urn = format("urn:%s:%s:user:%s", "my_usr_svc", recipe_scope, body.employee_id)
						asset.name = body.fullname
						asset.service = "my_usr_svc"
						// asset.type = "user" // not required, new_asset("user") sets the field.
						asset.data.email = body.work_email
						asset.data.username = body.employee_id
						asset.data.first_name = body.legal_first_name
						asset.data.last_name = body.legal_last_name
						asset.data.full_name = body.fullname
						asset.data.display_name = body.fullname
						asset.data.title = body.business_title
						asset.data.status = body.terminated == "true" ? "suspended" : "active"
						asset.data.manager_email = body.manager_email
						asset.data.attributes = {
							manager_id:           body.manager_id,
							cost_center_id:       body.cost_center_id,
							supervisory_org_name: body.supervisory_org_name,
							location_id:          body.location_id
						}
						emit(asset)
					`),
				},
			},
			handler: func(t *testing.T, w http.ResponseWriter, r *http.Request) {
				testutils.Respond(t, w, http.StatusOK,
					`{"manager_name":"Gabbi Champain","terminated":"false","fullname":"Van Stump","location_name":"Morocco","work_email":"vstump0@pcworld.com","supervisory_org_id":"1863d11c-fb86-4dbd-aa17-a53cbbaf3e9c","supervisory_org_name":"Research and Development","business_title":"General Manager","company_name":"Erdman Group","cost_center_id":"d6b470d8-e1ed-43ee-ab43-a645e607cf81","cost_center_name":"Sales","employee_id":"395f8292-d48b-431b-9e2d-63b3dcd4b986","manager_id":"496a320c-3c0a-4c0d-9658-a4f1dbbae20d","location_id":"MA","termination_date":null,"company_id":"8560f69c-11ef-42d4-b57e-8b8eacf32f9f","legal_first_name":"Van","manager_email":"vgchampain1@dot.gov","legal_last_name":"Stump"}`,
				)
			},
			expected: []*v1beta2.Asset{{
				Urn:     "urn:my_usr_svc:test-http:user:395f8292-d48b-431b-9e2d-63b3dcd4b986",
				Name:    "Van Stump",
				Service: "my_usr_svc",
				Type:    "user",
				Data: testutils.BuildAny(t, &v1beta2.User{
					FirstName:    "Van",
					LastName:     "Stump",
					FullName:     "Van Stump",
					DisplayName:  "Van Stump",
					Email:        "vstump0@pcworld.com",
					Title:        "General Manager",
					ManagerEmail: "vgchampain1@dot.gov",
					Status:       "active",
					Username:     "395f8292-d48b-431b-9e2d-63b3dcd4b986",
					Attributes: &structpb.Struct{Fields: map[string]*structpb.Value{
						"cost_center_id":       structpb.NewStringValue("d6b470d8-e1ed-43ee-ab43-a645e607cf81"),
						"location_id":          structpb.NewStringValue("MA"),
						"manager_id":           structpb.NewStringValue("496a320c-3c0a-4c0d-9658-a4f1dbbae20d"),
						"supervisory_org_name": structpb.NewStringValue("Research and Development"),
					}},
				}),
			}},
		},
		{
			name: "MultipleAssetsFromResponse",
			rawCfg: map[string]interface{}{
				"request": map[string]interface{}{
					"url":          "{{serverURL}}/api/v1/endpoint",
					"content_type": "application/json",
					"accept":       "application/json",
				},
				"script": map[string]interface{}{
					"engine": "tengo",
					"source": heredoc.Doc(`
						text := import("text")
						enum := import("enum")
	
						merge := func(m1, m2) {
							for k, v in m2 {
								m1[k] = v
							}
							return m1
						}
	
						kafkaServerToScope := func(server) {
							ss := text.split(server, ":")
							return ss[0]
						}
						
						buildUpstreams := func(ft) {
							upstreams := undefined
							if src := ft.spec.batch_source; src != undefined && src.type == "BATCH_BIGQUERY" {
								ss := text.split(src.bigquery_options.table_ref, ":")
								upstreams = append(upstreams || [], {
									urn: format("urn:bigquery:%s:table:%s", ss[0], src.bigquery_options.table_ref),
									service: "bigquery",
									type: "table"
								})
							}
							if src := ft.spec.stream_source; src != undefined {
								upstreams = append(upstreams || [], {
									urn: format(
										"urn:kafka:%s:topic:%s", 
										kafkaServerToScope(src.kafka_options.bootstrap_servers), src.kafka_options.topic
									),
									service: "kafka",
									type: "topic"
								})
							}
							return upstreams
						}
						
						body := response.body
						for ft in body.tables {
							ast := new_asset("feature_table")
							ast.urn = format(
								"urn:caramlstore:staging:feature_table:%s.%s",
								body.project, ft.spec.name
							)
							ast.name = ft.spec.name
							ast.service = "caramlstore"
							ast.type = "feature_table"
							ast.data = merge(ast.data, {
								namespace: body.project,
								entities: enum.map(ft.spec.entities, func(i, e){
									entity := enum.find(body.entities, func(i, entity) {
										return e == entity.spec.name
									})
									return {
										name: entity.spec.name,
										labels: {
											"value_type": entity.spec.value_type,
											"description": entity.spec.description
										}
									}
								}),
								features: enum.map(ft.spec.features, func(i, f){
									return {
										name: f.name,
										data_type: f.value_type
									}
								}),
								create_time: ft.meta.created_timestamp,
								update_time: ft.meta.last_updated_timestamp
							})
							ast.lineage = {
								upstreams: buildUpstreams(ft)
							}
							ast.labels = ft.spec.labels
							emit(ast)
						}
					`),
				},
			},
			handler: func(t *testing.T, w http.ResponseWriter, r *http.Request) {
				testutils.Respond(t, w, http.StatusOK,
					`{"project":"sauron","entities":[{"spec":{"name":"merchant_uuid","value_type":"STRING","description":"merchant uuid"},"meta":{"created_timestamp":"2022-08-08T03:17:51Z","last_updated_timestamp":"2022-08-08T03:17:51Z"}},{"spec":{"name":"booking_hour","value_type":"STRING","description":"Booking creation hour"},"meta":{"created_timestamp":"2022-08-08T03:17:51Z","last_updated_timestamp":"2022-08-08T03:17:51Z"}},{"spec":{"name":"day_of_week","value_type":"STRING","description":"Booking Day of Week"},"meta":{"created_timestamp":"2022-08-08T03:17:51Z","last_updated_timestamp":"2022-08-08T03:17:51Z"}},{"spec":{"name":"meal_id","value_type":"STRING","description":"Mealtime Identifier"},"meta":{"created_timestamp":"2022-08-08T03:17:51Z","last_updated_timestamp":"2022-08-08T03:17:51Z"}},{"spec":{"name":"t3_distance_bucket","value_type":"STRING","description":"T3 Distance Bucket"},"meta":{"created_timestamp":"2022-08-08T03:17:51Z","last_updated_timestamp":"2022-08-08T03:17:51Z"}},{"spec":{"name":"destination_s2id_12","value_type":"STRING","description":"Destination s2id_12"},"meta":{"created_timestamp":"2022-09-02T08:28:33Z","last_updated_timestamp":"2022-09-02T08:28:33Z"}},{"spec":{"name":"service_area_id","value_type":"STRING","description":"the id of gofood service area"},"meta":{"created_timestamp":"2022-09-14T03:10:45Z","last_updated_timestamp":"2022-09-14T03:10:45Z"}},{"spec":{"name":"item_uuid","value_type":"STRING","description":"item uuid"},"meta":{"created_timestamp":"2022-09-17T15:14:13Z","last_updated_timestamp":"2022-09-17T15:14:13Z"}}],"tables":[{"spec":{"name":"merchant_uuid_t2_discovery","entities":["merchant_uuid"],"features":[{"name":"avg_t2_merchant_3d","value_type":"DOUBLE"},{"name":"avg_t2_merchant_1d","value_type":"DOUBLE"},{"name":"avg_merchant_price","value_type":"DOUBLE"},{"name":"avg_t2_same_hour_merchant_1m","value_type":"DOUBLE"},{"name":"avg_t2_merchant_1w","value_type":"DOUBLE"},{"name":"avg_gmv_merchant_1w","value_type":"DOUBLE"},{"name":"avg_gmv_merchant_1d","value_type":"DOUBLE"},{"name":"merch_demand_same_hour_1m","value_type":"DOUBLE"},{"name":"avg_t2_merchant_3h","value_type":"DOUBLE"},{"name":"t2_discovery","value_type":"DOUBLE"},{"name":"avg_gmv_merchant_3h","value_type":"DOUBLE"},{"name":"avg_gmv_merchant_1m","value_type":"DOUBLE"},{"name":"avg_gmv_same_hour_merchant_1m","value_type":"DOUBLE"},{"name":"avg_t2_merchant_1m","value_type":"DOUBLE"}],"max_age":"7200s","batch_source":{"type":"BATCH_BIGQUERY","event_timestamp_column":"event_timestamp","bigquery_options":{"table_ref":"celestial-dragons-staging:feast.merchant_uuid_t2_discovery"}},"online_store":{"name":"bigtable","type":"BIGTABLE"}},"meta":{"created_timestamp":"2022-08-08T03:17:54Z","last_updated_timestamp":"2022-08-08T03:17:54Z","hash":"1227ba57"}},{"spec":{"name":"avg_dispatch_arrival_time_10_mins","entities":["merchant_uuid"],"features":[{"name":"ongoing_placed_and_waiting_acceptance_orders","value_type":"INT64"},{"name":"ongoing_orders","value_type":"INT64"},{"name":"merchant_avg_dispatch_arrival_time_10m","value_type":"FLOAT"},{"name":"ongoing_accepted_orders","value_type":"INT64"}],"max_age":"0s","batch_source":{"type":"BATCH_FILE","event_timestamp_column":"null","file_options":{"file_format":{"parquet_format":{}},"file_url":"/dev/null"}},"stream_source":{"type":"STREAM_KAFKA","field_mapping":{"merchant_uuid":"restaurant_uuid"},"event_timestamp_column":"event_timestamp","kafka_options":{"bootstrap_servers":"int-dagstream-kafka.yonkou.io:6668","topic":"GO_FOOD-delay-allocation-merchant-feature-10m-log","message_format":{"proto_format":{"class_path":"com.bubble.DelayAllocationMerchantFeature10mLogMessage"}}}},"online_store":{"name":"bigtable","type":"BIGTABLE"}},"meta":{"created_timestamp":"2022-09-19T22:42:04Z","last_updated_timestamp":"2022-09-21T13:23:02Z","revision":"2","hash":"730855ef"}}]}`,
				)
			},
			expected: []*v1beta2.Asset{
				{
					Urn:     "urn:caramlstore:staging:feature_table:sauron.merchant_uuid_t2_discovery",
					Name:    "merchant_uuid_t2_discovery",
					Service: "caramlstore",
					Type:    "feature_table",
					Data: testutils.BuildAny(t, &v1beta2.FeatureTable{
						Namespace: "sauron",
						Entities: []*v1beta2.FeatureTable_Entity{{
							Name: "merchant_uuid",
							Labels: map[string]string{
								"description": "merchant uuid",
								"value_type":  "STRING",
							},
						}},
						Features: []*v1beta2.Feature{
							{Name: "avg_t2_merchant_3d", DataType: "DOUBLE"},
							{Name: "avg_t2_merchant_1d", DataType: "DOUBLE"},
							{Name: "avg_merchant_price", DataType: "DOUBLE"},
							{Name: "avg_t2_same_hour_merchant_1m", DataType: "DOUBLE"},
							{Name: "avg_t2_merchant_1w", DataType: "DOUBLE"},
							{Name: "avg_gmv_merchant_1w", DataType: "DOUBLE"},
							{Name: "avg_gmv_merchant_1d", DataType: "DOUBLE"},
							{Name: "merch_demand_same_hour_1m", DataType: "DOUBLE"},
							{Name: "avg_t2_merchant_3h", DataType: "DOUBLE"},
							{Name: "t2_discovery", DataType: "DOUBLE"},
							{Name: "avg_gmv_merchant_3h", DataType: "DOUBLE"},
							{Name: "avg_gmv_merchant_1m", DataType: "DOUBLE"},
							{Name: "avg_gmv_same_hour_merchant_1m", DataType: "DOUBLE"},
							{Name: "avg_t2_merchant_1m", DataType: "DOUBLE"},
						},
						CreateTime: timestamppb.New(time.Date(2022, time.August, 8, 3, 17, 54, 0, time.UTC)),
						UpdateTime: timestamppb.New(time.Date(2022, time.August, 8, 3, 17, 54, 0, time.UTC)),
					}),
					Lineage: &v1beta2.Lineage{
						Upstreams: []*v1beta2.Resource{{
							Urn:     "urn:bigquery:celestial-dragons-staging:table:celestial-dragons-staging:feast.merchant_uuid_t2_discovery",
							Service: "bigquery",
							Type:    "table",
						}},
					},
				},
				{
					Urn:     "urn:caramlstore:staging:feature_table:sauron.avg_dispatch_arrival_time_10_mins",
					Name:    "avg_dispatch_arrival_time_10_mins",
					Service: "caramlstore",
					Type:    "feature_table",
					Data: testutils.BuildAny(t, &v1beta2.FeatureTable{
						Namespace: "sauron",
						Entities: []*v1beta2.FeatureTable_Entity{{
							Name: "merchant_uuid",
							Labels: map[string]string{
								"description": "merchant uuid",
								"value_type":  "STRING",
							},
						}},
						Features: []*v1beta2.Feature{
							{Name: "ongoing_placed_and_waiting_acceptance_orders", DataType: "INT64"},
							{Name: "ongoing_orders", DataType: "INT64"},
							{Name: "merchant_avg_dispatch_arrival_time_10m", DataType: "FLOAT"},
							{Name: "ongoing_accepted_orders", DataType: "INT64"},
						},
						CreateTime: timestamppb.New(time.Date(2022, time.September, 19, 22, 42, 04, 0, time.UTC)),
						UpdateTime: timestamppb.New(time.Date(2022, time.September, 21, 13, 23, 02, 0, time.UTC)),
					}),
					Lineage: &v1beta2.Lineage{
						Upstreams: []*v1beta2.Resource{{
							Urn:     "urn:kafka:int-dagstream-kafka.yonkou.io:topic:GO_FOOD-delay-allocation-merchant-feature-10m-log",
							Service: "kafka",
							Type:    "topic",
						}},
					},
				},
			},
		},
		{
			name: "AdditionalRequestsFromScript",
			rawCfg: map[string]interface{}{
				"request": map[string]interface{}{
					"url":          "{{serverURL}}/jobs",
					"content_type": "application/json",
					"accept":       "application/json",
				},
				"script": map[string]interface{}{
					"engine": "tengo",
					"source": heredoc.Doc(`
						reqs := []
						for j in response.body.jobs {
						  reqs = append(reqs, {
							url: format("{{serverURL}}/jobs/%s/config", j.id),
							method: "GET",
							content_type: "application/json",
							accept: "application/json",
							timeout: "5s"
						  })
						}
						  
						responses := execute_request(reqs...)
						for r in responses {
						  if is_error(r) {
							continue
						  }
						  
						  asset := new_asset("job")
						  asset.name = r.body.name
						  exec_cfg := r.body["execution-config"]
						  asset.data.attributes = {
							"job_id": r.body.jid,
							"job_parallelism": exec_cfg["job-parallelism"],
							"config": exec_cfg["user-config"]
						  }
						  emit(asset)
						}
					`),
				},
			},
			handler: func(t *testing.T, w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/jobs":
					testutils.Respond(t, w, http.StatusOK,
						`{"jobs":[{"id":"72b6753ab1984be6a65055b95ea9dd32","status":"RUNNING"},{"id":"3473947d1115c155513014cc6ecbd2fa","status":"RUNNING"},{"id":"9b12cb10b119b957b085c08e49bde3f2","status":"RESTARTING"},{"id":"fc308f1ac8c23b5f5a7942742b253917","status":"RESTARTING"}]}`,
					)
				case "/jobs/72b6753ab1984be6a65055b95ea9dd32/config":
					testutils.Respond(t, w, http.StatusOK,
						`{"jid":"72b6753ab1984be6a65055b95ea9dd32","name":"data-test-external-voucher-dagger","execution-config":{"execution-mode":"PIPELINED","restart-strategy":"Cluster level default restart strategy","job-parallelism":1,"object-reuse-mode":false,"user-config":{"SINK_KAFKA_TOPIC":"test_external_voucher","FLINK_ROWTIME_ATTRIBUTE_NAME":"rowtime","ENABLE_STENCIL_URL":"true","FLINK_SQL_QUERY":"SELECT member_ids.member_id as customer_id, '96962e7a-cd9e-4fb2-87fe-96091c124de6' as voucher_batch_id, rowtime as event_timestampfrom table1, UNNEST(table1.members) AS member_ids (member_id)where segment_name = 'testdagger' and action = 'ADD_MEMBERS'","SINK_TYPE":"kafka","PROCESSOR_PREPROCESSOR_CONFIG":"","FLINK_WATERMARK_INTERVAL_MS":"60000","FLINK_PARALLELISM":"1","SINK_INFLUX_BATCH_SIZE":"100","PROCESSOR_POSTPROCESSOR_ENABLE":"true","SINK_KAFKA_PROTO_MESSAGE":"com.company.esb.growth.AllocatePromoRequestMessage","PROCESSOR_PREPROCESSOR_ENABLE":"","SINK_INFLUX_MEASUREMENT_NAME":"data-test-external-voucher-dagger","SCHEMA_REGISTRY_STENCIL_ENABLE":"true","SINK_INFLUX_DB_NAME":"DAGGERS_COLLECTIVE","SCHEMA_REGISTRY_STENCIL_URLS":"http://data-systems-stencil.company.io/v1beta1/namespaces/company/schemas/esb/versions/68","SINK_KAFKA_STREAM":"data-dagstream","PROCESSOR_LONGBOW_GCP_INSTANCE_ID":"","PROCESSOR_POSTPROCESSOR_CONFIG":"{\"external_source\":{\"http\":[{\"endpoint\":\"ase1.company.io/internal/v2/voucher/allocate\",\"verb\":\"post\",\"request_pattern\":\"{\\\"voucher_batch_id\\\": \\\"%s\\\",\\\"customer_id\\\": \\\"%s-60\\\"}\",\"request_variables\":\"customer_id,customer_id\",\"stream_timeout\":\"5000\",\"connect_timeout\":\"5000\",\"fail_on_errors\":\"false\",\"capacity\":\"30\",\"headers\":{\"Content-Type\":\"application/json\",\"Accept-Language\":\"en\"},\"type\":\"com.company.esb.growth.AllocatePromoRequestMessage\",\"output_mapping\":{\"voucher_batch_id\":{\"path\":\"$.data.id\"}}}]}}","STREAMS":"[{\"SOURCE_KAFKA_TOPIC_NAMES\":\"segmentation-message\",\"INPUT_SCHEMA_PROTO_CLASS\":\"com.company.esb.segmentation.UpdateLogMessage\",\"INPUT_SCHEMA_TABLE\":\"table1\",\"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_COMMIT_ENABLE\":\"false\",\"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_OFFSET_RESET\":\"latest\",\"SOURCE_KAFKA_CONSUMER_CONFIG_GROUP_ID\":\"data-test-external-voucher-dagger-0001\",\"SOURCE_KAFKA_CONSUMER_CONFIG_BOOTSTRAP_SERVERS\":\"<REDACTED>\",\"INPUT_SCHEMA_EVENT_TIMESTAMP_FIELD_INDEX\":\"3\",\"SOURCE_KAFKA_NAME\":\"data-dagstream\"}]","SINK_INFLUX_FLUSH_DURATION_MS":"1000","SINK_INFLUX_URL":"http://data-dagger-shared-influx.company.io:6798","SINK_KAFKA_BROKERS":"<REDACTED>","FLINK_JOB_ID":"data-test-external-voucher-dagger","FLINK_WATERMARK_DELAY_MS":"1000","SINK_KAFKA_PROTO_KEY":"com.company.esb.growth.AllocatePromoRequestKey"}}}`,
					)
				case "/jobs/3473947d1115c155513014cc6ecbd2fa/config":
					testutils.Respond(t, w, http.StatusOK,
						`{"jid":"3473947d1115c155513014cc6ecbd2fa","name":"data-booking-map-matching-dagger","execution-config":{"execution-mode":"PIPELINED","restart-strategy":"Cluster level default restart strategy","job-parallelism":1,"object-reuse-mode":false,"user-config":{"SINK_KAFKA_TOPIC":"booking-map-matching-log","FLINK_ROWTIME_ATTRIBUTE_NAME":"rowtime","SINK_BIGQUERY_TABLE_CLUSTERING_ENABLE":"false","SINK_TYPE":"kafka","FLINK_SQL_QUERY":"SELECT driver_id, booking_id, country_code, event_timestamp, booking_status, vehicle_type, driver_locations, ping_processing_driver_locations(driver_locations) as filtered_driver_locations, gh_map_matching_response, polylineFROM data_streams_0","SINK_BIGQUERY_TABLE_PARTITION_EXPIRY_MS":"-1","SINK_CONNECTOR_SCHEMA_PROTO_MESSAGE_CLASS":"","PROCESSOR_PREPROCESSOR_CONFIG":"","FLINK_WATERMARK_INTERVAL_MS":"60000","FLINK_PARALLELISM":"1","SINK_METRICS_APPLICATION_PREFIX":"dagger_","SINK_INFLUX_BATCH_SIZE":"100","SINK_KAFKA_PROTO_MESSAGE":"company.esb.cartography.erp.ERPMapMatchingLogV2Message","SINK_BIGQUERY_CLIENT_READ_TIMEOUT_MS":"-1","SINK_BIGQUERY_DATASET_LABELS":"","SCHEMA_REGISTRY_STENCIL_ENABLE":"true","SINK_INFLUX_DB_NAME":"DAGGERS_COLLECTIVE","SINK_BIGQUERY_TABLE_LABELS":"","SINK_CONNECTOR_SCHEMA_DATA_TYPE":"PROTOBUF","SINK_CONNECTOR_SCHEMA_PROTO_ALLOW_UNKNOWN_FIELDS_ENABLE":"false","SINK_INFLUX_URL":"http://data-dagger-shared-influx.company.io:6798","FLINK_JOB_ID":"data-booking-map-matching-dagger","SINK_KAFKA_BROKERS":"<REDACTED>","PYTHON_UDF_ENABLE":"true","FLINK_WATERMARK_DELAY_MS":"1000","SINK_KAFKA_PROTO_KEY":"company.esb.cartography.erp.ERPMapMatchingLogV2Key","SINK_CONNECTOR_SCHEMA_PROTO_KEY_CLASS":"","SINK_BIGQUERY_BATCH_SIZE":"","SINK_BIGQUERY_METADATA_COLUMNS_TYPES":"","ENABLE_STENCIL_URL":"true","PYTHON_UDF_CONFIG":"{\"PYTHON_FILES\":\"gs://data-dagger-magic/python/master/88.41.13/python_udfs.zip\",\"PYTHON_ARCHIVES\":\"gs://data-dagger-magic/python/master/88.41.13/data.zip\",\"PYTHON_REQUIREMENTS\":\"gs://data-dagger-magic/python/master/88.41.13/requirements.txt\"}","SINK_CONNECTOR_SCHEMA_MESSAGE_MODE":"LOG_MESSAGE","PROCESSOR_POSTPROCESSOR_ENABLE":"true","SINK_BIGQUERY_CLIENT_CONNECT_TIMEOUT_MS":"-1","SINK_INFLUX_MEASUREMENT_NAME":"map-matching-dagger","PROCESSOR_PREPROCESSOR_ENABLE":"false","SINK_BIGQUERY_TABLE_NAME":"","SINK_KAFKA_STREAM":"data-dagstream","SCHEMA_REGISTRY_STENCIL_URLS":"http://data-systems-stencil.company.io/v1beta1/namespaces/company/schemas/esb/versions/590","SINK_BIGQUERY_GOOGLE_CLOUD_PROJECT_ID":"","SINK_BIGQUERY_DATASET_NAME":"","PROCESSOR_LONGBOW_GCP_INSTANCE_ID":"","PROCESSOR_POSTPROCESSOR_CONFIG":"{\"external_source\":{\"http\":[{\"endpoint\":\"http://11.126.1.18:6798/match\",\"verb\":\"post\",\"request_pattern\":\"{\"locations\":%s,\"hints\":{\"vehicle\":\"car\",\"instructions\":false,\"points_encoded\":true,\"gps_accuracy\":50}}\",\"request_variables\":\"filtered_driver_locations\",\"stream_timeout\":\"100000\",\"connect_timeout\":\"10000\",\"fail_on_errors\":\"false\",\"capacity\":\"30\",\"headers\":{\"content-type\":\"application/json\"},\"output_mapping\":{\"gh_map_matching_response\":{\"path\":\"$\"},\"polyline\":{\"path\":\"$.map_matching.edge_geometry_polyline\"}}}]},\"internal_source\":[{\"output_field\":\"driver_id\",\"value\":\"driver_id\",\"type\":\"sql\"},{\"output_field\":\"booking_id\",\"value\":\"booking_id\",\"type\":\"sql\"},{\"output_field\":\"country_code\",\"value\":\"country_code\",\"type\":\"sql\"},{\"output_field\":\"event_timestamp\",\"value\":\"event_timestamp\",\"type\":\"sql\"},{\"output_field\":\"booking_status\",\"value\":\"booking_status\",\"type\":\"sql\"},{\"output_field\":\"vehicle_type\",\"value\":\"vehicle_type\",\"type\":\"sql\"},{\"output_field\":\"driver_locations\",\"value\":\"driver_locations\",\"type\":\"sql\"},{\"output_field\":\"filtered_driver_locations\",\"value\":\"filtered_driver_locations\",\"type\":\"sql\"}]}","SINK_BIGQUERY_TABLE_CLUSTERING_KEYS":"","STREAMS":"[{\"INPUT_SCHEMA_EVENT_TIMESTAMP_FIELD_INDEX\":\"4\",\"INPUT_SCHEMA_PROTO_CLASS\":\"company.esb.cartography.erp.ERPMapMatchingLogV2Message\",\"INPUT_SCHEMA_TABLE\":\"data_streams_0\",\"SOURCE_DETAILS\":[{\"SOURCE_NAME\":\"KAFKA_CONSUMER\",\"SOURCE_TYPE\":\"UNBOUNDED\"}],\"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_COMMIT_ENABLE\":\"false\",\"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_OFFSET_RESET\":\"latest\",\"SOURCE_KAFKA_CONSUMER_CONFIG_BOOTSTRAP_SERVERS\":\"company-mainstream.company.io:6668\",\"SOURCE_KAFKA_CONSUMER_CONFIG_GROUP_ID\":\"data-booking-map-matching-dagger-0002\",\"SOURCE_KAFKA_NAME\":\"company-mainstream\",\"SOURCE_KAFKA_TOPIC_NAMES\":\"aggregated-busy-driver-location-ping\",\"SOURCE_PARQUET_FILE_DATE_RANGE\":null,\"SOURCE_PARQUET_FILE_PATHS\":null}]","SINK_INFLUX_FLUSH_DURATION_MS":"1000","SINK_BIGQUERY_METADATA_NAMESPACE":""}}}`,
					)
				case "/jobs/9b12cb10b119b957b085c08e49bde3f2/config":
					testutils.Respond(t, w, http.StatusOK,
						`{"jid":"9b12cb10b119b957b085c08e49bde3f2","name":"data-eim-driver-nearby-staging-dagger","execution-config":{"execution-mode":"PIPELINED","restart-strategy":"Cluster level default restart strategy","job-parallelism":1,"object-reuse-mode":false,"user-config":{"SINK_KAFKA_TOPIC":"eim-driver-nearby-log","FLINK_ROWTIME_ATTRIBUTE_NAME":"rowtime","ENABLE_STENCIL_URL":"true","SINK_TYPE":"kafka","FLINK_SQL_QUERY":"SELECT gofood_booking.event_timestamp as event_timestamp, gofood_booking.order_number AS order_number, gofood_booking.service_type AS gofood_booking_message.service_type, gofood_booking.order_number AS gofood_booking_message.order_number, gofood_booking.order_url AS gofood_booking_message.order_url, gofood_booking.status AS gofood_booking_message.status, gofood_booking.event_timestamp AS gofood_booking_message.event_timestamp, gofood_booking.customer_id AS gofood_booking_message.customer_id, gofood_booking.customer_url AS gofood_booking_message.customer_url, gofood_booking.driver_id AS gofood_booking_message.driver_id, gofood_booking.driver_url AS gofood_booking_message.driver_url, gofood_booking.is_reblast AS gofood_booking_message.is_reblast, gofood_booking.activity_source AS gofood_booking_message.activity_source, gofood_booking.service_area_id AS gofood_booking_message.service_area_id, gofood_booking.payment_type AS gofood_booking_message.payment_type, gofood_booking.total_unsubsidised_price AS gofood_booking_message.total_unsubsidised_price, gofood_booking.customer_price AS gofood_booking_message.customer_price, gofood_booking.amount_paid_by_cash AS gofood_booking_message.amount_paid_by_cash, gofood_booking.amount_paid_by_credits AS gofood_booking_message.amount_paid_by_credits, gofood_booking.surcharge_amount AS gofood_booking_message.surcharge_amount, gofood_booking.tip_amount AS gofood_booking_message.tip_amount, gofood_booking.driver_cut_amount AS gofood_booking_message.driver_cut_amount, gofood_booking.requested_payment_type AS gofood_booking_message.requested_payment_type, gofood_booking.total_distance_in_kms AS gofood_booking_message.total_distance_in_kms, gofood_booking.route_polyline AS gofood_booking_message.route_polyline, gofood_booking.routes AS gofood_booking_message.routes, gofood_booking.driver_eta_pickup AS gofood_booking_message.driver_eta_pickup, gofood_booking.driver_eta_dropoff AS gofood_booking_message.driver_eta_dropoff, gofood_booking.driver_pickup_location AS gofood_booking_message.driver_pickup_location, gofood_booking.driver_dropoff_location AS gofood_booking_message.driver_dropoff_location, gofood_booking.customer_email AS gofood_booking_message.customer_email, gofood_booking.customer_name AS gofood_booking_message.customer_name, gofood_booking.customer_phone AS gofood_booking_message.customer_phone, gofood_booking.driver_email AS gofood_booking_message.driver_email, gofood_booking.driver_name AS gofood_booking_message.driver_name, gofood_booking.driver_phone AS gofood_booking_message.driver_phone, gofood_booking.driver_phone2 AS gofood_booking_message.driver_phone2, gofood_booking.driver_phone3 AS gofood_booking_message.driver_phone3, gofood_booking.cancel_reason_id AS gofood_booking_message.cancel_reason_id, gofood_booking.cancel_reason_description AS gofood_booking_message.cancel_reason_description, gofood_booking.cancel_source AS gofood_booking_message.cancel_source, gofood_booking.customer_type AS gofood_booking_message.customer_type, gofood_booking.cancel_owner AS gofood_booking_message.cancel_owner, gofood_booking.booking_creation_time AS gofood_booking_message.booking_creation_time, gofood_booking.total_customer_discount AS gofood_booking_message.total_customer_discount, gofood_booking.gopay_customer_discount AS gofood_booking_message.gopay_customer_discount, gofood_booking.voucher_customer_discount AS gofood_booking_message.voucher_customer_discount, gofood_booking.pickup_time AS gofood_booking_message.pickup_time, gofood_booking.driver_paid_in_cash AS gofood_booking_message.driver_paid_in_cash, gofood_booking.driver_paid_in_credit AS gofood_booking_message.driver_paid_in_credit, gofood_booking.receiver_name AS gofood_booking_message.receiver_name, gofood_booking.driver_photo_url AS gofood_booking_message.driver_photo_url, gofood_booking.previous_booking_status AS gofood_booking_message.previous_booking_status, gofood_booking.vehicle_type AS gofood_booking_message.vehicle_type, gofood_booking.customer_total_fare_without_surge AS gofood_booking_message.customer_total_fare_without_surge, gofood_booking.customer_surge_factor AS gofood_booking_message.customer_surge_factor, gofood_booking.customer_dynamic_surge AS gofood_booking_message.customer_dynamic_surge, gofood_booking.customer_dynamic_surge_enabled AS gofood_booking_message.customer_dynamic_surge_enabled, gofood_booking.driver_total_fare_without_surge AS gofood_booking_message.driver_total_fare_without_surge, gofood_booking.driver_surge_factor AS gofood_booking_message.driver_surge_factor, gofood_booking.driver_dynamic_surge AS gofood_booking_message.driver_dynamic_surge, gofood_booking.driver_dynamic_surge_enabled AS gofood_booking_message.driver_dynamic_surge_enabled, gofood_booking.driver_ata_pickup AS gofood_booking_message.driver_ata_pickup, gofood_booking.driver_ata_dropoff AS gofood_booking_message.driver_ata_dropoff, gofood_booking.gcm_key AS gofood_booking_message.gcm_key, gofood_booking.device_token AS gofood_booking_message.device_token, gofood_booking.pricing_service_id AS gofood_booking_message.pricing_service_id, gofood_booking.payment_invoice_number AS gofood_booking_message.payment_invoice_number, gofood_booking.pricing_currency AS gofood_booking_message.pricing_currency, gofood_booking.country_code AS gofood_booking_message.country_code, gofood_booking.service_area_tzname AS gofood_booking_message.service_area_tzname, gofood_booking.payment_option_type AS gofood_booking_message.payment_option_type, gofood_booking.payment_option_metadata AS gofood_booking_message.payment_option_metadata, gofood_booking.payment_option_name AS gofood_booking_message.payment_option_name, gofood_booking.booking_info AS gofood_booking_message.booking_info, gofood_booking.price_edit_reason AS gofood_booking_message.price_edit_reason, gofood_booking.driver_arrived_location AS gofood_booking_message.driver_arrived_location, gofood_booking.driver_arrived_time AS gofood_booking_message.driver_arrived_time, gofood_booking.driver_order_placed_location AS gofood_booking_message.driver_order_placed_location, gofood_booking.driver_order_placed_time AS gofood_booking_message.driver_order_placed_time, gofood_booking.merchant_accepted_time AS gofood_booking_message.merchant_accepted_time, gofood_booking.merchant_acknowledged_time AS gofood_booking_message.merchant_acknowledged_time, gofood_booking.merchant_received_time AS gofood_booking_message.merchant_received_time, gofood_booking.merchant_cancel_reason AS gofood_booking_message.merchant_cancel_reason, gofood_booking.merchant_cancel_time AS gofood_booking_message.merchant_cancel_time, gofood_booking.merchant_cancel_description AS gofood_booking_message.merchant_cancel_description, gofood_booking.takeaway_charges AS gofood_booking_message.takeaway_charges, gofood_booking.food_prepared_time AS gofood_booking_message.food_prepared_time, gofood_booking.cancel_reason_code AS gofood_booking_message.cancel_reason_code, gofood_booking.verification_requested_time AS gofood_booking_message.verification_requested_time, gofood_booking.customer_pickup_time AS gofood_booking_message.customer_pickup_time, gofood_booking.customer_pickup_location AS gofood_booking_message.customer_pickup_location, gofood_booking.verification_failed_time AS gofood_booking_message.verification_failed_time, gofood_booking.verification_requested_location AS gofood_booking_message.verification_requested_location, gofood_booking.convenience_fee AS gofood_booking_message.convenience_fee, gofood_booking.payment_actions AS gofood_booking_message.payment_actions, gofood_booking.marketplace_serviceability_log_id AS gofood_booking_message.marketplace_serviceability_log_id, gofood_booking.order_completion_time AS gofood_booking_message.order_completion_time, gofood_booking.restaurant_id AS gofood_booking_message.restaurant_id, gofood_booking.sub_status AS gofood_booking_message.sub_status, gofood_booking.shopping_price AS gofood_booking_message.shopping_price, gofood_booking.commission_price AS gofood_booking_message.commission_price, gofood_booking.withholding_income_tax AS gofood_booking_message.withholding_income_tax, gofood_booking.voucher_redeemed_value AS gofood_booking_message.voucher_redeemed_value, gofood_booking.voucher_commission AS gofood_booking_message.voucher_commission, gofood_booking.voucher_id AS gofood_booking_message.voucher_id, gofood_booking.voucher_title AS gofood_booking_message.voucher_title, gofood_booking.otp AS gofood_booking_message.otp, gofood_booking.driver_entered_price AS gofood_booking_message.driver_entered_price, gofood_booking.driver_wallet_id AS gofood_booking_message.driver_wallet_id, gofood_booking.saudagar_id AS gofood_booking_message.saudagar_id, gofood_booking.validated_at AS gofood_booking_message.validated_at, gofood_booking.merchant_wallet_id AS gofood_booking_message.merchant_wallet_id, gofood_booking.restaurant_uuid AS gofood_booking_message.restaurant_uuid, gofood_booking.search_id AS gofood_booking_message.search_id, gofood_booking.gopay_driver_reservation_id AS gofood_booking_message.gopay_driver_reservation_id, gofood_booking.voucher_batch_id AS gofood_booking_message.voucher_batch_id, gofood_booking.merchant_config AS gofood_booking_message.merchant_config, gofood_booking.brand_id AS gofood_booking_message.brand_id, gofood_booking.customer_wallet_id AS gofood_booking_message.customer_wallet_id, gofood_booking.customer_payment_details AS gofood_booking_message.customer_payment_details, gofood_booking.merchant_phone AS gofood_booking_message.merchant_phone, gofood_booking.previous_sub_status AS gofood_booking_message.previous_sub_status, gofood_booking.merchant_acceptance_deadline AS gofood_booking_message.merchant_acceptance_deadline, gofood_booking.inapplicable_voucher_id AS gofood_booking_message.inapplicable_voucher_id, gofood_booking.driver_fee_adjustments AS gofood_booking_message.driver_fee_adjustments, gofood_booking.receipt_url AS gofood_booking_message.receipt_url, gofood_booking.is_goresto AS gofood_booking_message.is_goresto, gofood_booking.shopping_items AS gofood_booking_message.shopping_items, gofood_booking.has_promo AS gofood_booking_message.has_promo, gofood_booking.analytics AS gofood_booking_message.analytics, gofood_booking.fraud_reason AS gofood_booking_message.fraud_reason, gofood_booking.driver_completion_time AS gofood_booking_message.driver_completion_time, gofood_booking.use_service_wallet_fund_flow AS gofood_booking_message.use_service_wallet_fund_flow, gofood_booking.campaign_discounts AS gofood_booking_message.campaign_discounts, gofood_booking.tracer_bullet AS gofood_booking_message.tracer_bullet, gofood_booking.bid_delay_in_seconds AS gofood_booking_message.bid_delay_in_seconds, gofood_booking.driver_gopay_account_id AS gofood_booking_message.driver_gopay_account_id, gofood_booking.use_gopay_v3 AS gofood_booking_message.use_gopay_v3, gofood_booking.experiments AS gofood_booking_message.experiments, gofood_booking.eta_in_minutes AS gofood_booking_message.eta_in_minutes, gofood_booking.eta_source AS gofood_booking_message.eta_source, gofood_booking.eta_performance_bucket AS gofood_booking_message.eta_performance_bucket, gofood_booking.payment_method AS gofood_booking_message.payment_method, gofood_booking.vehicle_info AS gofood_booking_message.vehicle_info, gofood_booking.actor AS gofood_booking_message.actor, gofood_booking.vehicle_tags AS gofood_booking_message.vehicle_tags, gofood_booking.goclub AS gofood_booking_message.goclub, gofood_nearby.event_timestamp AS gofood_nearby_message.event_timestamp, gofood_nearby.order_number AS gofood_nearby_message.order_number, gofood_nearby.driver_id AS gofood_nearby_message.driver_id, gofood_nearby.customer_id AS gofood_nearby_message.customer_id, gofood_nearby.restaurant_uuid AS gofood_nearby_message.restaurant_uuid, gofood_nearby.saudagar_id AS gofood_nearby_message.saudagar_id, gofood_nearby.type AS gofood_nearby_message.type, gofood_nearby.radius AS gofood_nearby_message.radius, gofood_nearby.actor AS gofood_nearby_message.actorFROM gofood_nearby JOIN gofood_booking ON gofood_nearby.order_number = gofood_booking.order_number AND gofood_nearby.driver_id = gofood_booking.driver_id AND gofood_nearby.customer_id = gofood_booking.customer_id AND gofood_nearby.restaurant_uuid = gofood_booking.restaurant_uuid AND gofood_nearby.rowtime BETWEEN (gofood_booking.rowtime - INTERVAL '10' MINUTE) AND (gofood_booking.rowtime + INTERVAL '40' MINUTE) AND gofood_booking.sub_status = 'OTW_PICKUP' AND gofood_nearby.type = 'PICKUP' AND gofood_nearby.actor = 'DRIVER'","PYTHON_UDF_CONFIG":"{\"PYTHON_FILES\": \"gs://data-dagger/python/master/latest/python_udfs.zip\",\"PYTHON_REQUIREMENTS\": \"gs://data-dagger/python/master/latest/requirements.txt\",\"PYTHON_ARCHIVES\": \"gs://data-dagger/python/master/latest/data.zip#data\",\"PYTHON_FN_EXECUTION_ARROW_BATCH_SIZE\": \"10000\",\"PYTHON_FN_EXECUTION_BUNDLE_SIZE\": \"100000\",\"PYTHON_FN_EXECUTION_BUNDLE_TIME\": \"1000\"}","PROCESSOR_PREPROCESSOR_CONFIG":"","FLINK_WATERMARK_INTERVAL_MS":"30000","FLINK_PARALLELISM":"1","SINK_INFLUX_BATCH_SIZE":"100","PROCESSOR_POSTPROCESSOR_ENABLE":"false","SINK_KAFKA_PROTO_MESSAGE":"company.esb.gomerchants.eim.EimDriverNearbyEventMessage","SINK_INFLUX_MEASUREMENT_NAME":"data-eim-driver-nearby-staging-dagger","PROCESSOR_PREPROCESSOR_ENABLE":"","SCHEMA_REGISTRY_STENCIL_ENABLE":"true","SINK_INFLUX_DB_NAME":"DAGGERS_COLLECTIVE","SINK_KAFKA_STREAM":"data-dagstream","SCHEMA_REGISTRY_STENCIL_URLS":"http://data-systems-stencil.company.io/v1beta1/namespaces/company/schemas/esb/versions/72","PROCESSOR_LONGBOW_GCP_INSTANCE_ID":"","PROCESSOR_POSTPROCESSOR_CONFIG":"{}","STREAMS":"[{\"INPUT_SCHEMA_EVENT_TIMESTAMP_FIELD_INDEX\":\"5\",\"INPUT_SCHEMA_PROTO_CLASS\":\"company.esb.booking.GoFoodBookingLogMessage\",\"INPUT_SCHEMA_TABLE\":\"gofood_booking\",\"SOURCE_DETAILS\":[{\"SOURCE_NAME\":\"KAFKA_CONSUMER\",\"SOURCE_TYPE\":\"UNBOUNDED\"}],\"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_COMMIT_ENABLE\":\"false\",\"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_OFFSET_RESET\":\"latest\",\"SOURCE_KAFKA_CONSUMER_CONFIG_BOOTSTRAP_SERVERS\":\"<REDACTED>\",\"SOURCE_KAFKA_CONSUMER_CONFIG_GROUP_ID\":\"data-eim-driver-nearby-staging-dagger-0039\",\"SOURCE_KAFKA_NAME\":\"company-mainstream\",\"SOURCE_KAFKA_TOPIC_NAMES\":\"gofood-booking-log\"},{\"INPUT_SCHEMA_EVENT_TIMESTAMP_FIELD_INDEX\":\"1\",\"INPUT_SCHEMA_PROTO_CLASS\":\"company.esb.gofood.NearbyEventMessage\",\"INPUT_SCHEMA_TABLE\":\"gofood_nearby\",\"SOURCE_DETAILS\":[{\"SOURCE_NAME\":\"KAFKA_CONSUMER\",\"SOURCE_TYPE\":\"UNBOUNDED\"}],\"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_COMMIT_ENABLE\":\"false\",\"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_OFFSET_RESET\":\"latest\",\"SOURCE_KAFKA_CONSUMER_CONFIG_BOOTSTRAP_SERVERS\":\"<REDACTED>\",\"SOURCE_KAFKA_CONSUMER_CONFIG_GROUP_ID\":\"data-eim-driver-nearby-staging-dagger-0040\",\"SOURCE_KAFKA_NAME\":\"company-mainstream\",\"SOURCE_KAFKA_TOPIC_NAMES\":\"gofood-nearby-log\"}]","SINK_INFLUX_FLUSH_DURATION_MS":"1000","SINK_INFLUX_URL":"http://data-dagger-shared-influx.company.io:6798","FLINK_JOB_ID":"data-eim-driver-nearby-staging-dagger","SINK_KAFKA_BROKERS":"<REDACTED>","PYTHON_UDF_ENABLE":"false","FLINK_WATERMARK_DELAY_MS":"1000","SINK_KAFKA_PROTO_KEY":"company.esb.gomerchants.eim.EimDriverNearbyEventKey"}}}`,
					)
				case "/jobs/fc308f1ac8c23b5f5a7942742b253917/config":
					testutils.Respond(t, w, http.StatusInternalServerError, http.StatusText(http.StatusInternalServerError))
				default:
					t.Error("Unexpected HTTP call on", r.URL.Path)
				}
			},
			expected: []*v1beta2.Asset{
				{
					Name: "data-test-external-voucher-dagger",
					Type: "job",
					Data: testutils.BuildAny(t, &v1beta2.Job{
						Attributes: &structpb.Struct{Fields: map[string]*structpb.Value{
							"job_id":          structpb.NewStringValue("72b6753ab1984be6a65055b95ea9dd32"),
							"job_parallelism": structpb.NewNumberValue(1),
							"config": structpb.NewStructValue(&structpb.Struct{Fields: map[string]*structpb.Value{
								"ENABLE_STENCIL_URL":                structpb.NewStringValue("true"),
								"FLINK_JOB_ID":                      structpb.NewStringValue("data-test-external-voucher-dagger"),
								"FLINK_PARALLELISM":                 structpb.NewStringValue("1"),
								"FLINK_ROWTIME_ATTRIBUTE_NAME":      structpb.NewStringValue("rowtime"),
								"FLINK_SQL_QUERY":                   structpb.NewStringValue("SELECT member_ids.member_id as customer_id, '96962e7a-cd9e-4fb2-87fe-96091c124de6' as voucher_batch_id, rowtime as event_timestampfrom table1, UNNEST(table1.members) AS member_ids (member_id)where segment_name = 'testdagger' and action = 'ADD_MEMBERS'"),
								"FLINK_WATERMARK_DELAY_MS":          structpb.NewStringValue("1000"),
								"FLINK_WATERMARK_INTERVAL_MS":       structpb.NewStringValue("60000"),
								"PROCESSOR_LONGBOW_GCP_INSTANCE_ID": structpb.NewStringValue(""),
								"PROCESSOR_POSTPROCESSOR_CONFIG":    structpb.NewStringValue(`{"external_source":{"http":[{"endpoint":"ase1.company.io/internal/v2/voucher/allocate","verb":"post","request_pattern":"{\"voucher_batch_id\": \"%s\",\"customer_id\": \"%s-60\"}","request_variables":"customer_id,customer_id","stream_timeout":"5000","connect_timeout":"5000","fail_on_errors":"false","capacity":"30","headers":{"Content-Type":"application/json","Accept-Language":"en"},"type":"com.company.esb.growth.AllocatePromoRequestMessage","output_mapping":{"voucher_batch_id":{"path":"$.data.id"}}}]}}`),
								"PROCESSOR_POSTPROCESSOR_ENABLE":    structpb.NewStringValue("true"),
								"PROCESSOR_PREPROCESSOR_CONFIG":     structpb.NewStringValue(""),
								"PROCESSOR_PREPROCESSOR_ENABLE":     structpb.NewStringValue(""),
								"SCHEMA_REGISTRY_STENCIL_ENABLE":    structpb.NewStringValue("true"),
								"SCHEMA_REGISTRY_STENCIL_URLS":      structpb.NewStringValue("http://data-systems-stencil.company.io/v1beta1/namespaces/company/schemas/esb/versions/68"),
								"SINK_INFLUX_BATCH_SIZE":            structpb.NewStringValue("100"),
								"SINK_INFLUX_DB_NAME":               structpb.NewStringValue("DAGGERS_COLLECTIVE"),
								"SINK_INFLUX_FLUSH_DURATION_MS":     structpb.NewStringValue("1000"),
								"SINK_INFLUX_MEASUREMENT_NAME":      structpb.NewStringValue("data-test-external-voucher-dagger"),
								"SINK_INFLUX_URL":                   structpb.NewStringValue("http://data-dagger-shared-influx.company.io:6798"),
								"SINK_KAFKA_BROKERS":                structpb.NewStringValue("<REDACTED>"),
								"SINK_KAFKA_PROTO_KEY":              structpb.NewStringValue("com.company.esb.growth.AllocatePromoRequestKey"),
								"SINK_KAFKA_PROTO_MESSAGE":          structpb.NewStringValue("com.company.esb.growth.AllocatePromoRequestMessage"),
								"SINK_KAFKA_STREAM":                 structpb.NewStringValue("data-dagstream"),
								"SINK_KAFKA_TOPIC":                  structpb.NewStringValue("test_external_voucher"),
								"SINK_TYPE":                         structpb.NewStringValue("kafka"),
								"STREAMS":                           structpb.NewStringValue(`[{"SOURCE_KAFKA_TOPIC_NAMES":"segmentation-message","INPUT_SCHEMA_PROTO_CLASS":"com.company.esb.segmentation.UpdateLogMessage","INPUT_SCHEMA_TABLE":"table1","SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_COMMIT_ENABLE":"false","SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_OFFSET_RESET":"latest","SOURCE_KAFKA_CONSUMER_CONFIG_GROUP_ID":"data-test-external-voucher-dagger-0001","SOURCE_KAFKA_CONSUMER_CONFIG_BOOTSTRAP_SERVERS":"<REDACTED>","INPUT_SCHEMA_EVENT_TIMESTAMP_FIELD_INDEX":"3","SOURCE_KAFKA_NAME":"data-dagstream"}]`),
							}}),
						}},
					}),
				},
				{
					Name: "data-booking-map-matching-dagger",
					Type: "job",
					Data: testutils.BuildAny(t, &v1beta2.Job{
						Attributes: &structpb.Struct{Fields: map[string]*structpb.Value{
							"job_id":          structpb.NewStringValue("3473947d1115c155513014cc6ecbd2fa"),
							"job_parallelism": structpb.NewNumberValue(1),
							"config": structpb.NewStructValue(&structpb.Struct{Fields: map[string]*structpb.Value{
								"ENABLE_STENCIL_URL":                                      structpb.NewStringValue("true"),
								"FLINK_JOB_ID":                                            structpb.NewStringValue("data-booking-map-matching-dagger"),
								"FLINK_PARALLELISM":                                       structpb.NewStringValue("1"),
								"FLINK_ROWTIME_ATTRIBUTE_NAME":                            structpb.NewStringValue("rowtime"),
								"FLINK_SQL_QUERY":                                         structpb.NewStringValue("SELECT driver_id, booking_id, country_code, event_timestamp, booking_status, vehicle_type, driver_locations, ping_processing_driver_locations(driver_locations) as filtered_driver_locations, gh_map_matching_response, polylineFROM data_streams_0"),
								"FLINK_WATERMARK_DELAY_MS":                                structpb.NewStringValue("1000"),
								"FLINK_WATERMARK_INTERVAL_MS":                             structpb.NewStringValue("60000"),
								"PROCESSOR_LONGBOW_GCP_INSTANCE_ID":                       structpb.NewStringValue(""),
								"PROCESSOR_POSTPROCESSOR_CONFIG":                          structpb.NewStringValue(`{"external_source":{"http":[{"endpoint":"http://11.126.1.18:6798/match","verb":"post","request_pattern":"{"locations":%s,"hints":{"vehicle":"car","instructions":false,"points_encoded":true,"gps_accuracy":50}}","request_variables":"filtered_driver_locations","stream_timeout":"100000","connect_timeout":"10000","fail_on_errors":"false","capacity":"30","headers":{"content-type":"application/json"},"output_mapping":{"gh_map_matching_response":{"path":"$"},"polyline":{"path":"$.map_matching.edge_geometry_polyline"}}}]},"internal_source":[{"output_field":"driver_id","value":"driver_id","type":"sql"},{"output_field":"booking_id","value":"booking_id","type":"sql"},{"output_field":"country_code","value":"country_code","type":"sql"},{"output_field":"event_timestamp","value":"event_timestamp","type":"sql"},{"output_field":"booking_status","value":"booking_status","type":"sql"},{"output_field":"vehicle_type","value":"vehicle_type","type":"sql"},{"output_field":"driver_locations","value":"driver_locations","type":"sql"},{"output_field":"filtered_driver_locations","value":"filtered_driver_locations","type":"sql"}]}`),
								"PROCESSOR_POSTPROCESSOR_ENABLE":                          structpb.NewStringValue("true"),
								"PROCESSOR_PREPROCESSOR_CONFIG":                           structpb.NewStringValue(""),
								"PROCESSOR_PREPROCESSOR_ENABLE":                           structpb.NewStringValue("false"),
								"PYTHON_UDF_CONFIG":                                       structpb.NewStringValue(`{"PYTHON_FILES":"gs://data-dagger-magic/python/master/88.41.13/python_udfs.zip","PYTHON_ARCHIVES":"gs://data-dagger-magic/python/master/88.41.13/data.zip","PYTHON_REQUIREMENTS":"gs://data-dagger-magic/python/master/88.41.13/requirements.txt"}`),
								"PYTHON_UDF_ENABLE":                                       structpb.NewStringValue("true"),
								"SCHEMA_REGISTRY_STENCIL_ENABLE":                          structpb.NewStringValue("true"),
								"SCHEMA_REGISTRY_STENCIL_URLS":                            structpb.NewStringValue("http://data-systems-stencil.company.io/v1beta1/namespaces/company/schemas/esb/versions/590"),
								"SINK_BIGQUERY_BATCH_SIZE":                                structpb.NewStringValue(""),
								"SINK_BIGQUERY_CLIENT_CONNECT_TIMEOUT_MS":                 structpb.NewStringValue("-1"),
								"SINK_BIGQUERY_CLIENT_READ_TIMEOUT_MS":                    structpb.NewStringValue("-1"),
								"SINK_BIGQUERY_DATASET_LABELS":                            structpb.NewStringValue(""),
								"SINK_BIGQUERY_DATASET_NAME":                              structpb.NewStringValue(""),
								"SINK_BIGQUERY_GOOGLE_CLOUD_PROJECT_ID":                   structpb.NewStringValue(""),
								"SINK_BIGQUERY_METADATA_COLUMNS_TYPES":                    structpb.NewStringValue(""),
								"SINK_BIGQUERY_METADATA_NAMESPACE":                        structpb.NewStringValue(""),
								"SINK_BIGQUERY_TABLE_CLUSTERING_ENABLE":                   structpb.NewStringValue("false"),
								"SINK_BIGQUERY_TABLE_CLUSTERING_KEYS":                     structpb.NewStringValue(""),
								"SINK_BIGQUERY_TABLE_LABELS":                              structpb.NewStringValue(""),
								"SINK_BIGQUERY_TABLE_NAME":                                structpb.NewStringValue(""),
								"SINK_BIGQUERY_TABLE_PARTITION_EXPIRY_MS":                 structpb.NewStringValue("-1"),
								"SINK_CONNECTOR_SCHEMA_DATA_TYPE":                         structpb.NewStringValue("PROTOBUF"),
								"SINK_CONNECTOR_SCHEMA_MESSAGE_MODE":                      structpb.NewStringValue("LOG_MESSAGE"),
								"SINK_CONNECTOR_SCHEMA_PROTO_ALLOW_UNKNOWN_FIELDS_ENABLE": structpb.NewStringValue("false"),
								"SINK_CONNECTOR_SCHEMA_PROTO_KEY_CLASS":                   structpb.NewStringValue(""),
								"SINK_CONNECTOR_SCHEMA_PROTO_MESSAGE_CLASS":               structpb.NewStringValue(""),
								"SINK_INFLUX_BATCH_SIZE":                                  structpb.NewStringValue("100"),
								"SINK_INFLUX_DB_NAME":                                     structpb.NewStringValue("DAGGERS_COLLECTIVE"),
								"SINK_INFLUX_FLUSH_DURATION_MS":                           structpb.NewStringValue("1000"),
								"SINK_INFLUX_MEASUREMENT_NAME":                            structpb.NewStringValue("map-matching-dagger"),
								"SINK_INFLUX_URL":                                         structpb.NewStringValue("http://data-dagger-shared-influx.company.io:6798"),
								"SINK_KAFKA_BROKERS":                                      structpb.NewStringValue("<REDACTED>"),
								"SINK_KAFKA_PROTO_KEY":                                    structpb.NewStringValue("company.esb.cartography.erp.ERPMapMatchingLogV2Key"),
								"SINK_KAFKA_PROTO_MESSAGE":                                structpb.NewStringValue("company.esb.cartography.erp.ERPMapMatchingLogV2Message"),
								"SINK_KAFKA_STREAM":                                       structpb.NewStringValue("data-dagstream"),
								"SINK_KAFKA_TOPIC":                                        structpb.NewStringValue("booking-map-matching-log"),
								"SINK_METRICS_APPLICATION_PREFIX":                         structpb.NewStringValue("dagger_"),
								"SINK_TYPE":                                               structpb.NewStringValue("kafka"),
								"STREAMS":                                                 structpb.NewStringValue(`[{"INPUT_SCHEMA_EVENT_TIMESTAMP_FIELD_INDEX":"4","INPUT_SCHEMA_PROTO_CLASS":"company.esb.cartography.erp.ERPMapMatchingLogV2Message","INPUT_SCHEMA_TABLE":"data_streams_0","SOURCE_DETAILS":[{"SOURCE_NAME":"KAFKA_CONSUMER","SOURCE_TYPE":"UNBOUNDED"}],"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_COMMIT_ENABLE":"false","SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_OFFSET_RESET":"latest","SOURCE_KAFKA_CONSUMER_CONFIG_BOOTSTRAP_SERVERS":"company-mainstream.company.io:6668","SOURCE_KAFKA_CONSUMER_CONFIG_GROUP_ID":"data-booking-map-matching-dagger-0002","SOURCE_KAFKA_NAME":"company-mainstream","SOURCE_KAFKA_TOPIC_NAMES":"aggregated-busy-driver-location-ping","SOURCE_PARQUET_FILE_DATE_RANGE":null,"SOURCE_PARQUET_FILE_PATHS":null}]`),
							}}),
						}},
					}),
				},
				{
					Name: "data-eim-driver-nearby-staging-dagger",
					Type: "job",
					Data: testutils.BuildAny(t, &v1beta2.Job{
						Attributes: &structpb.Struct{Fields: map[string]*structpb.Value{
							"job_id":          structpb.NewStringValue("9b12cb10b119b957b085c08e49bde3f2"),
							"job_parallelism": structpb.NewNumberValue(1),
							"config": structpb.NewStructValue(&structpb.Struct{Fields: map[string]*structpb.Value{
								"ENABLE_STENCIL_URL":                structpb.NewStringValue("true"),
								"FLINK_JOB_ID":                      structpb.NewStringValue("data-eim-driver-nearby-staging-dagger"),
								"FLINK_PARALLELISM":                 structpb.NewStringValue("1"),
								"FLINK_ROWTIME_ATTRIBUTE_NAME":      structpb.NewStringValue("rowtime"),
								"FLINK_SQL_QUERY":                   structpb.NewStringValue("SELECT gofood_booking.event_timestamp as event_timestamp, gofood_booking.order_number AS order_number, gofood_booking.service_type AS gofood_booking_message.service_type, gofood_booking.order_number AS gofood_booking_message.order_number, gofood_booking.order_url AS gofood_booking_message.order_url, gofood_booking.status AS gofood_booking_message.status, gofood_booking.event_timestamp AS gofood_booking_message.event_timestamp, gofood_booking.customer_id AS gofood_booking_message.customer_id, gofood_booking.customer_url AS gofood_booking_message.customer_url, gofood_booking.driver_id AS gofood_booking_message.driver_id, gofood_booking.driver_url AS gofood_booking_message.driver_url, gofood_booking.is_reblast AS gofood_booking_message.is_reblast, gofood_booking.activity_source AS gofood_booking_message.activity_source, gofood_booking.service_area_id AS gofood_booking_message.service_area_id, gofood_booking.payment_type AS gofood_booking_message.payment_type, gofood_booking.total_unsubsidised_price AS gofood_booking_message.total_unsubsidised_price, gofood_booking.customer_price AS gofood_booking_message.customer_price, gofood_booking.amount_paid_by_cash AS gofood_booking_message.amount_paid_by_cash, gofood_booking.amount_paid_by_credits AS gofood_booking_message.amount_paid_by_credits, gofood_booking.surcharge_amount AS gofood_booking_message.surcharge_amount, gofood_booking.tip_amount AS gofood_booking_message.tip_amount, gofood_booking.driver_cut_amount AS gofood_booking_message.driver_cut_amount, gofood_booking.requested_payment_type AS gofood_booking_message.requested_payment_type, gofood_booking.total_distance_in_kms AS gofood_booking_message.total_distance_in_kms, gofood_booking.route_polyline AS gofood_booking_message.route_polyline, gofood_booking.routes AS gofood_booking_message.routes, gofood_booking.driver_eta_pickup AS gofood_booking_message.driver_eta_pickup, gofood_booking.driver_eta_dropoff AS gofood_booking_message.driver_eta_dropoff, gofood_booking.driver_pickup_location AS gofood_booking_message.driver_pickup_location, gofood_booking.driver_dropoff_location AS gofood_booking_message.driver_dropoff_location, gofood_booking.customer_email AS gofood_booking_message.customer_email, gofood_booking.customer_name AS gofood_booking_message.customer_name, gofood_booking.customer_phone AS gofood_booking_message.customer_phone, gofood_booking.driver_email AS gofood_booking_message.driver_email, gofood_booking.driver_name AS gofood_booking_message.driver_name, gofood_booking.driver_phone AS gofood_booking_message.driver_phone, gofood_booking.driver_phone2 AS gofood_booking_message.driver_phone2, gofood_booking.driver_phone3 AS gofood_booking_message.driver_phone3, gofood_booking.cancel_reason_id AS gofood_booking_message.cancel_reason_id, gofood_booking.cancel_reason_description AS gofood_booking_message.cancel_reason_description, gofood_booking.cancel_source AS gofood_booking_message.cancel_source, gofood_booking.customer_type AS gofood_booking_message.customer_type, gofood_booking.cancel_owner AS gofood_booking_message.cancel_owner, gofood_booking.booking_creation_time AS gofood_booking_message.booking_creation_time, gofood_booking.total_customer_discount AS gofood_booking_message.total_customer_discount, gofood_booking.gopay_customer_discount AS gofood_booking_message.gopay_customer_discount, gofood_booking.voucher_customer_discount AS gofood_booking_message.voucher_customer_discount, gofood_booking.pickup_time AS gofood_booking_message.pickup_time, gofood_booking.driver_paid_in_cash AS gofood_booking_message.driver_paid_in_cash, gofood_booking.driver_paid_in_credit AS gofood_booking_message.driver_paid_in_credit, gofood_booking.receiver_name AS gofood_booking_message.receiver_name, gofood_booking.driver_photo_url AS gofood_booking_message.driver_photo_url, gofood_booking.previous_booking_status AS gofood_booking_message.previous_booking_status, gofood_booking.vehicle_type AS gofood_booking_message.vehicle_type, gofood_booking.customer_total_fare_without_surge AS gofood_booking_message.customer_total_fare_without_surge, gofood_booking.customer_surge_factor AS gofood_booking_message.customer_surge_factor, gofood_booking.customer_dynamic_surge AS gofood_booking_message.customer_dynamic_surge, gofood_booking.customer_dynamic_surge_enabled AS gofood_booking_message.customer_dynamic_surge_enabled, gofood_booking.driver_total_fare_without_surge AS gofood_booking_message.driver_total_fare_without_surge, gofood_booking.driver_surge_factor AS gofood_booking_message.driver_surge_factor, gofood_booking.driver_dynamic_surge AS gofood_booking_message.driver_dynamic_surge, gofood_booking.driver_dynamic_surge_enabled AS gofood_booking_message.driver_dynamic_surge_enabled, gofood_booking.driver_ata_pickup AS gofood_booking_message.driver_ata_pickup, gofood_booking.driver_ata_dropoff AS gofood_booking_message.driver_ata_dropoff, gofood_booking.gcm_key AS gofood_booking_message.gcm_key, gofood_booking.device_token AS gofood_booking_message.device_token, gofood_booking.pricing_service_id AS gofood_booking_message.pricing_service_id, gofood_booking.payment_invoice_number AS gofood_booking_message.payment_invoice_number, gofood_booking.pricing_currency AS gofood_booking_message.pricing_currency, gofood_booking.country_code AS gofood_booking_message.country_code, gofood_booking.service_area_tzname AS gofood_booking_message.service_area_tzname, gofood_booking.payment_option_type AS gofood_booking_message.payment_option_type, gofood_booking.payment_option_metadata AS gofood_booking_message.payment_option_metadata, gofood_booking.payment_option_name AS gofood_booking_message.payment_option_name, gofood_booking.booking_info AS gofood_booking_message.booking_info, gofood_booking.price_edit_reason AS gofood_booking_message.price_edit_reason, gofood_booking.driver_arrived_location AS gofood_booking_message.driver_arrived_location, gofood_booking.driver_arrived_time AS gofood_booking_message.driver_arrived_time, gofood_booking.driver_order_placed_location AS gofood_booking_message.driver_order_placed_location, gofood_booking.driver_order_placed_time AS gofood_booking_message.driver_order_placed_time, gofood_booking.merchant_accepted_time AS gofood_booking_message.merchant_accepted_time, gofood_booking.merchant_acknowledged_time AS gofood_booking_message.merchant_acknowledged_time, gofood_booking.merchant_received_time AS gofood_booking_message.merchant_received_time, gofood_booking.merchant_cancel_reason AS gofood_booking_message.merchant_cancel_reason, gofood_booking.merchant_cancel_time AS gofood_booking_message.merchant_cancel_time, gofood_booking.merchant_cancel_description AS gofood_booking_message.merchant_cancel_description, gofood_booking.takeaway_charges AS gofood_booking_message.takeaway_charges, gofood_booking.food_prepared_time AS gofood_booking_message.food_prepared_time, gofood_booking.cancel_reason_code AS gofood_booking_message.cancel_reason_code, gofood_booking.verification_requested_time AS gofood_booking_message.verification_requested_time, gofood_booking.customer_pickup_time AS gofood_booking_message.customer_pickup_time, gofood_booking.customer_pickup_location AS gofood_booking_message.customer_pickup_location, gofood_booking.verification_failed_time AS gofood_booking_message.verification_failed_time, gofood_booking.verification_requested_location AS gofood_booking_message.verification_requested_location, gofood_booking.convenience_fee AS gofood_booking_message.convenience_fee, gofood_booking.payment_actions AS gofood_booking_message.payment_actions, gofood_booking.marketplace_serviceability_log_id AS gofood_booking_message.marketplace_serviceability_log_id, gofood_booking.order_completion_time AS gofood_booking_message.order_completion_time, gofood_booking.restaurant_id AS gofood_booking_message.restaurant_id, gofood_booking.sub_status AS gofood_booking_message.sub_status, gofood_booking.shopping_price AS gofood_booking_message.shopping_price, gofood_booking.commission_price AS gofood_booking_message.commission_price, gofood_booking.withholding_income_tax AS gofood_booking_message.withholding_income_tax, gofood_booking.voucher_redeemed_value AS gofood_booking_message.voucher_redeemed_value, gofood_booking.voucher_commission AS gofood_booking_message.voucher_commission, gofood_booking.voucher_id AS gofood_booking_message.voucher_id, gofood_booking.voucher_title AS gofood_booking_message.voucher_title, gofood_booking.otp AS gofood_booking_message.otp, gofood_booking.driver_entered_price AS gofood_booking_message.driver_entered_price, gofood_booking.driver_wallet_id AS gofood_booking_message.driver_wallet_id, gofood_booking.saudagar_id AS gofood_booking_message.saudagar_id, gofood_booking.validated_at AS gofood_booking_message.validated_at, gofood_booking.merchant_wallet_id AS gofood_booking_message.merchant_wallet_id, gofood_booking.restaurant_uuid AS gofood_booking_message.restaurant_uuid, gofood_booking.search_id AS gofood_booking_message.search_id, gofood_booking.gopay_driver_reservation_id AS gofood_booking_message.gopay_driver_reservation_id, gofood_booking.voucher_batch_id AS gofood_booking_message.voucher_batch_id, gofood_booking.merchant_config AS gofood_booking_message.merchant_config, gofood_booking.brand_id AS gofood_booking_message.brand_id, gofood_booking.customer_wallet_id AS gofood_booking_message.customer_wallet_id, gofood_booking.customer_payment_details AS gofood_booking_message.customer_payment_details, gofood_booking.merchant_phone AS gofood_booking_message.merchant_phone, gofood_booking.previous_sub_status AS gofood_booking_message.previous_sub_status, gofood_booking.merchant_acceptance_deadline AS gofood_booking_message.merchant_acceptance_deadline, gofood_booking.inapplicable_voucher_id AS gofood_booking_message.inapplicable_voucher_id, gofood_booking.driver_fee_adjustments AS gofood_booking_message.driver_fee_adjustments, gofood_booking.receipt_url AS gofood_booking_message.receipt_url, gofood_booking.is_goresto AS gofood_booking_message.is_goresto, gofood_booking.shopping_items AS gofood_booking_message.shopping_items, gofood_booking.has_promo AS gofood_booking_message.has_promo, gofood_booking.analytics AS gofood_booking_message.analytics, gofood_booking.fraud_reason AS gofood_booking_message.fraud_reason, gofood_booking.driver_completion_time AS gofood_booking_message.driver_completion_time, gofood_booking.use_service_wallet_fund_flow AS gofood_booking_message.use_service_wallet_fund_flow, gofood_booking.campaign_discounts AS gofood_booking_message.campaign_discounts, gofood_booking.tracer_bullet AS gofood_booking_message.tracer_bullet, gofood_booking.bid_delay_in_seconds AS gofood_booking_message.bid_delay_in_seconds, gofood_booking.driver_gopay_account_id AS gofood_booking_message.driver_gopay_account_id, gofood_booking.use_gopay_v3 AS gofood_booking_message.use_gopay_v3, gofood_booking.experiments AS gofood_booking_message.experiments, gofood_booking.eta_in_minutes AS gofood_booking_message.eta_in_minutes, gofood_booking.eta_source AS gofood_booking_message.eta_source, gofood_booking.eta_performance_bucket AS gofood_booking_message.eta_performance_bucket, gofood_booking.payment_method AS gofood_booking_message.payment_method, gofood_booking.vehicle_info AS gofood_booking_message.vehicle_info, gofood_booking.actor AS gofood_booking_message.actor, gofood_booking.vehicle_tags AS gofood_booking_message.vehicle_tags, gofood_booking.goclub AS gofood_booking_message.goclub, gofood_nearby.event_timestamp AS gofood_nearby_message.event_timestamp, gofood_nearby.order_number AS gofood_nearby_message.order_number, gofood_nearby.driver_id AS gofood_nearby_message.driver_id, gofood_nearby.customer_id AS gofood_nearby_message.customer_id, gofood_nearby.restaurant_uuid AS gofood_nearby_message.restaurant_uuid, gofood_nearby.saudagar_id AS gofood_nearby_message.saudagar_id, gofood_nearby.type AS gofood_nearby_message.type, gofood_nearby.radius AS gofood_nearby_message.radius, gofood_nearby.actor AS gofood_nearby_message.actorFROM gofood_nearby JOIN gofood_booking ON gofood_nearby.order_number = gofood_booking.order_number AND gofood_nearby.driver_id = gofood_booking.driver_id AND gofood_nearby.customer_id = gofood_booking.customer_id AND gofood_nearby.restaurant_uuid = gofood_booking.restaurant_uuid AND gofood_nearby.rowtime BETWEEN (gofood_booking.rowtime - INTERVAL '10' MINUTE) AND (gofood_booking.rowtime + INTERVAL '40' MINUTE) AND gofood_booking.sub_status = 'OTW_PICKUP' AND gofood_nearby.type = 'PICKUP' AND gofood_nearby.actor = 'DRIVER'"),
								"FLINK_WATERMARK_DELAY_MS":          structpb.NewStringValue("1000"),
								"FLINK_WATERMARK_INTERVAL_MS":       structpb.NewStringValue("30000"),
								"PROCESSOR_LONGBOW_GCP_INSTANCE_ID": structpb.NewStringValue(""),
								"PROCESSOR_POSTPROCESSOR_CONFIG":    structpb.NewStringValue("{}"),
								"PROCESSOR_POSTPROCESSOR_ENABLE":    structpb.NewStringValue("false"),
								"PROCESSOR_PREPROCESSOR_CONFIG":     structpb.NewStringValue(""),
								"PROCESSOR_PREPROCESSOR_ENABLE":     structpb.NewStringValue(""),
								"PYTHON_UDF_CONFIG":                 structpb.NewStringValue(`{"PYTHON_FILES": "gs://data-dagger/python/master/latest/python_udfs.zip","PYTHON_REQUIREMENTS": "gs://data-dagger/python/master/latest/requirements.txt","PYTHON_ARCHIVES": "gs://data-dagger/python/master/latest/data.zip#data","PYTHON_FN_EXECUTION_ARROW_BATCH_SIZE": "10000","PYTHON_FN_EXECUTION_BUNDLE_SIZE": "100000","PYTHON_FN_EXECUTION_BUNDLE_TIME": "1000"}`),
								"PYTHON_UDF_ENABLE":                 structpb.NewStringValue("false"),
								"SCHEMA_REGISTRY_STENCIL_ENABLE":    structpb.NewStringValue("true"),
								"SCHEMA_REGISTRY_STENCIL_URLS":      structpb.NewStringValue("http://data-systems-stencil.company.io/v1beta1/namespaces/company/schemas/esb/versions/72"),
								"SINK_INFLUX_BATCH_SIZE":            structpb.NewStringValue("100"),
								"SINK_INFLUX_DB_NAME":               structpb.NewStringValue("DAGGERS_COLLECTIVE"),
								"SINK_INFLUX_FLUSH_DURATION_MS":     structpb.NewStringValue("1000"),
								"SINK_INFLUX_MEASUREMENT_NAME":      structpb.NewStringValue("data-eim-driver-nearby-staging-dagger"),
								"SINK_INFLUX_URL":                   structpb.NewStringValue("http://data-dagger-shared-influx.company.io:6798"),
								"SINK_KAFKA_BROKERS":                structpb.NewStringValue("<REDACTED>"),
								"SINK_KAFKA_PROTO_KEY":              structpb.NewStringValue("company.esb.gomerchants.eim.EimDriverNearbyEventKey"),
								"SINK_KAFKA_PROTO_MESSAGE":          structpb.NewStringValue("company.esb.gomerchants.eim.EimDriverNearbyEventMessage"),
								"SINK_KAFKA_STREAM":                 structpb.NewStringValue("data-dagstream"),
								"SINK_KAFKA_TOPIC":                  structpb.NewStringValue("eim-driver-nearby-log"),
								"SINK_TYPE":                         structpb.NewStringValue("kafka"),
								"STREAMS":                           structpb.NewStringValue(`[{"INPUT_SCHEMA_EVENT_TIMESTAMP_FIELD_INDEX":"5","INPUT_SCHEMA_PROTO_CLASS":"company.esb.booking.GoFoodBookingLogMessage","INPUT_SCHEMA_TABLE":"gofood_booking","SOURCE_DETAILS":[{"SOURCE_NAME":"KAFKA_CONSUMER","SOURCE_TYPE":"UNBOUNDED"}],"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_COMMIT_ENABLE":"false","SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_OFFSET_RESET":"latest","SOURCE_KAFKA_CONSUMER_CONFIG_BOOTSTRAP_SERVERS":"<REDACTED>","SOURCE_KAFKA_CONSUMER_CONFIG_GROUP_ID":"data-eim-driver-nearby-staging-dagger-0039","SOURCE_KAFKA_NAME":"company-mainstream","SOURCE_KAFKA_TOPIC_NAMES":"gofood-booking-log"},{"INPUT_SCHEMA_EVENT_TIMESTAMP_FIELD_INDEX":"1","INPUT_SCHEMA_PROTO_CLASS":"company.esb.gofood.NearbyEventMessage","INPUT_SCHEMA_TABLE":"gofood_nearby","SOURCE_DETAILS":[{"SOURCE_NAME":"KAFKA_CONSUMER","SOURCE_TYPE":"UNBOUNDED"}],"SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_COMMIT_ENABLE":"false","SOURCE_KAFKA_CONSUMER_CONFIG_AUTO_OFFSET_RESET":"latest","SOURCE_KAFKA_CONSUMER_CONFIG_BOOTSTRAP_SERVERS":"<REDACTED>","SOURCE_KAFKA_CONSUMER_CONFIG_GROUP_ID":"data-eim-driver-nearby-staging-dagger-0040","SOURCE_KAFKA_NAME":"company-mainstream","SOURCE_KAFKA_TOPIC_NAMES":"gofood-nearby-log"}]`),
							}}),
						}},
					}),
				},
			},
		},
		{
			name: "InvalidRequestConfigFromScript",
			rawCfg: map[string]interface{}{
				"request": map[string]interface{}{
					"url":          "{{serverURL}}/api/v1/endpoint",
					"content_type": "application/json",
					"accept":       "application/json",
				},
				"script": map[string]interface{}{
					"engine": "tengo",
					"source": heredoc.Doc(`
						responses := execute_request(
						  {
						    "url":          "{{serverURL}}/api/v1/endpoint",
						    "content_type": "application/json",
						    "accept":       "application/json"
						  },
						  {
						    "content_type": "application/json",
						    "accept":       "application/json"
						  }
						)
					`),
				},
			},
			handler: func(t *testing.T, w http.ResponseWriter, r *http.Request) {
				testutils.Respond(t, w, http.StatusOK, `{}`)
			},
			expectedErr: `Error:Field validation for 'url' failed on the 'required' tag`,
		},
		{
			name: "ConditionalExit",
			rawCfg: map[string]interface{}{
				"request": map[string]interface{}{
					"url":          "{{serverURL}}/api/v1/endpoint",
					"content_type": "application/json",
					"accept":       "application/json",
				},
				"script": map[string]interface{}{
					"engine": "tengo",
					"source": heredoc.Doc(`
						if !response.body.success {
							exit()
						}
						a := new_asset("invalid")
					`),
				},
			},
			handler: func(t *testing.T, w http.ResponseWriter, r *http.Request) {
				testutils.Respond(t, w, http.StatusOK, `{"success": false, "data": []}`)
			},
		},
		{
			name: "NewAssetWithoutType",
			rawCfg: map[string]interface{}{
				"request": map[string]interface{}{
					"url":          "{{serverURL}}/api/v1/endpoint",
					"content_type": "application/json",
					"accept":       "application/json",
				},
				"script": map[string]interface{}{
					"engine": "tengo",
					"source": heredoc.Doc(`
						a := new_asset()
						emit(a)
					`),
				},
			},
			handler: func(t *testing.T, w http.ResponseWriter, r *http.Request) {
				testutils.Respond(t, w, http.StatusOK, `{}`)
			},
			expectedErr: "Runtime Error: wrong number of arguments in call to 'user-function:new_asset'",
		},
		{
			name: "InvalidAssetType",
			rawCfg: map[string]interface{}{
				"request": map[string]interface{}{
					"url":          "{{serverURL}}/api/v1/endpoint",
					"content_type": "application/json",
					"accept":       "application/json",
				},
				"script": map[string]interface{}{
					"engine": "tengo",
					"source": heredoc.Doc(`
						a := new_asset("invalid")
						emit(a)
					`),
				},
			},
			handler: func(t *testing.T, w http.ResponseWriter, r *http.Request) {
				testutils.Respond(t, w, http.StatusOK, `{}`)
			},
			expectedErr: "Runtime Error: new asset: unexpected type: invalid",
		},
		{
			name: "EmitInvalidValue",
			rawCfg: map[string]interface{}{
				"request": map[string]interface{}{
					"url":          "{{serverURL}}/api/v1/endpoint",
					"content_type": "application/json",
					"accept":       "application/json",
				},
				"script": map[string]interface{}{
					"engine": "tengo",
					"source": heredoc.Doc(`
						emit("invalid")
					`),
				},
			},
			handler: func(t *testing.T, w http.ResponseWriter, r *http.Request) {
				testutils.Respond(t, w, http.StatusOK, `{}`)
			},
			expectedErr: "Runtime Error: invalid type for argument 'asset' in call to 'user-function:emit': expected Map, found string",
		},
		{
			name: "EmitMultiple",
			rawCfg: map[string]interface{}{
				"request": map[string]interface{}{
					"url":          "{{serverURL}}/api/v1/endpoint",
					"content_type": "application/json",
					"accept":       "application/json",
				},
				"script": map[string]interface{}{
					"engine": "tengo",
					"source": heredoc.Doc(`
						emit(new_asset("user"), new_asset("user"))
					`),
				},
			},
			handler: func(t *testing.T, w http.ResponseWriter, r *http.Request) {
				testutils.Respond(t, w, http.StatusOK, `{}`)
			},
			expectedErr: "Runtime Error: wrong number of arguments in call to 'user-function:emit'",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				tc.handler(t, w, r)
			}))
			defer srv.Close()

			replaceServerURL(tc.rawCfg, srv.URL)

			extr := New(testutils.Logger)
			err := extr.Init(ctx, plugins.Config{
				URNScope:  urnScope,
				RawConfig: tc.rawCfg,
			})
			require.NoError(t, err)

			emitter := mocks.NewEmitter()
			err = extr.Extract(ctx, emitter.Push)
			if tc.expectedErr != "" {
				assert.ErrorContains(t, err, tc.expectedErr)
				return
			}

			assert.NoError(t, err)
			testutils.AssertEqualProtos(t, tc.expected, emitter.GetAllData())
		})
	}
}

func replaceServerURL(cfg map[string]interface{}, serverURL string) {
	reqCfg := cfg["request"].(map[string]interface{})
	reqCfg["url"] = strings.Replace(reqCfg["url"].(string), "{{serverURL}}", serverURL, 1)
	scriptCfg := cfg["script"].(map[string]interface{})
	scriptCfg["source"] = strings.Replace(scriptCfg["source"].(string), "{{serverURL}}", serverURL, -1)
}
