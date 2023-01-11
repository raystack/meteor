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
	v1beta2 "github.com/odpf/meteor/models/odpf/assets/v1beta2"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/test/mocks"
	testutils "github.com/odpf/meteor/test/utils"
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
				assert.JSONEq(t, `{"id": "urn:merlin:merlin-stg:model:46.218"}`, (string)(data))

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
						asset := new_asset("user")
						// URN format: "urn:{service}:{scope}:{type}:{id}"
						asset.urn = format("urn:%s:staging:user:%s", "my_usr_svc", response.employee_id)
						asset.name = response.fullname
						asset.service = "my_usr_svc"
						// asset.type = "user" // not required, new_asset("user") sets the field.
						asset.data.email = response.work_email
						asset.data.username = response.employee_id
						asset.data.first_name = response.legal_first_name
						asset.data.last_name = response.legal_last_name
						asset.data.full_name = response.fullname
						asset.data.display_name = response.fullname
						asset.data.title = response.business_title
						asset.data.status = response.terminated == "true" ? "suspended" : "active"
						asset.data.manager_email = response.manager_email
						asset.data.attributes = {
							manager_id:           response.manager_id,
							cost_center_id:       response.cost_center_id,
							supervisory_org_name: response.supervisory_org_name,
							location_id:          response.location_id
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
				Urn:     "urn:my_usr_svc:staging:user:395f8292-d48b-431b-9e2d-63b3dcd4b986",
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
						
						for ft in response.tables {
							ast := new_asset("feature_table")
							ast.urn = format(
								"urn:caramlstore:staging:feature_table:%s.%s",
								response.project, ft.spec.name
							)
							ast.name = ft.spec.name
							ast.service = "caramlstore"
							ast.type = "feature_table"
							ast.data = merge(ast.data, {
								namespace: response.project,
								entities: enum.map(ft.spec.entities, func(i, e){
									entity := enum.find(response.entities, func(i, entity) {
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
						if !response.success {
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
}
