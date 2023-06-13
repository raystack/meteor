//go:build plugins
// +build plugins

package script

import (
	"context"
	"testing"
	"time"

	"github.com/MakeNowJust/heredoc"
	"github.com/goto/meteor/models"
	v1beta2 "github.com/goto/meteor/models/gotocompany/assets/v1beta2"
	"github.com/goto/meteor/plugins"
	testutils "github.com/goto/meteor/test/utils"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	ctx    = context.Background()
	script = `asset.owners = append(asset.owners || [], { name: "Big Mom", email: "big.mom@wholecakeisland.com" })`
)

func TestInit(t *testing.T) {
	t.Run("InvalidConfig", func(t *testing.T) {
		cases := []struct {
			name string
			cfg  plugins.Config
		}{
			{
				name: "WithoutScript",
				cfg: plugins.Config{
					RawConfig: map[string]interface{}{"engine": "tengo"},
				},
			},
			{
				name: "WithoutEngine",
				cfg: plugins.Config{
					RawConfig: map[string]interface{}{"script": script},
				},
			},
			{
				name: "WithUnsupportedEngine",
				cfg: plugins.Config{
					RawConfig: map[string]interface{}{"script": script, "engine": "goja"},
				},
			},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				err := New(testutils.Logger).Init(ctx, tc.cfg)
				assert.ErrorAs(t, err, &plugins.InvalidConfigError{})
			})
		}
	})

	t.Run("InvalidScript", func(t *testing.T) {
		err := New(testutils.Logger).Init(ctx, plugins.Config{
			RawConfig: map[string]interface{}{
				"script": `ast.owners = []`,
				"engine": "tengo",
			},
		})
		assert.ErrorContains(t, err, "script processor init: compile script: Compile Error: unresolved reference 'ast'")
	})
}

func TestProcess(t *testing.T) {
	cases := []struct {
		name     string
		script   string
		input    *v1beta2.Asset
		expected *v1beta2.Asset
		errStr   string
	}{
		{
			name: "FeatureTableAsset",
			script: heredoc.Doc(`
				text := import("text")
				times := import("times")
				
				merge := func(m1, m2) {
					for k, v in m2 {
						m1[k] = v
					}
					return m1
				}
				
				asset.labels = merge({script_engine: "tengo"}, asset.labels)
				
				for e in asset.data.entities {
					e.labels = merge({catch_phrase: "You talkin' to me?"}, e.labels)
				}
				
				for f in asset.data.features {
					if f.name == "ongoing_placed_and_waiting_acceptance_orders" || f.name == "ongoing_orders" {
						f.entity_name = "customer_orders"
					} else if f.name == "merchant_avg_dispatch_arrival_time_10m" {
						f.entity_name = "merchant_driver"
					} else if f.name == "ongoing_accepted_orders" {
						f.entity_name = "merchant_orders"
					}
				}
				
				asset.owners = append(asset.owners || [], { name: "Big Mom", email: "big.mom@wholecakeisland.com" })

				for u in asset.lineage.upstreams {
					u.urn = u.service != "kafka" ? u.urn : text.replace(u.urn, ".yonkou.io", "", -1)
				}

				update_time := times.parse("2006-01-02T15:04:05Z07:00", asset.data.update_time)
				asset.data.update_time = times.add_date(update_time, 0, 0, 1)
				`),
			input: &v1beta2.Asset{
				Urn:     "urn:caramlstore:test-caramlstore:feature_table:avg_dispatch_arrival_time_10_mins",
				Name:    "avg_dispatch_arrival_time_10_mins",
				Service: "caramlstore",
				Type:    "feature_table",
				Data: testutils.BuildAny(t, &v1beta2.FeatureTable{
					Namespace: "sauron",
					Entities: []*v1beta2.FeatureTable_Entity{
						{Name: "merchant_uuid", Labels: map[string]string{
							"description": "merchant uuid",
							"value_type":  "STRING",
						}},
					},
					Features: []*v1beta2.Feature{
						{Name: "ongoing_placed_and_waiting_acceptance_orders", DataType: "INT64"},
						{Name: "ongoing_orders", DataType: "INT64"},
						{Name: "merchant_avg_dispatch_arrival_time_10m", DataType: "FLOAT"},
						{Name: "ongoing_accepted_orders", DataType: "INT64"},
					},
					CreateTime: timestamppb.New(time.Date(2022, time.September, 19, 22, 42, 0o4, 0, time.UTC)),
					UpdateTime: timestamppb.New(time.Date(2022, time.September, 21, 13, 23, 0o2, 0, time.UTC)),
				}),
				Lineage: &v1beta2.Lineage{
					Upstreams: []*v1beta2.Resource{
						{
							Urn:     "urn:kafka:int-dagstream-kafka.yonkou.io:topic:GO_FOOD-delay-allocation-merchant-feature-10m-log",
							Service: "kafka",
							Type:    "topic",
						},
					},
				},
			},
			expected: &v1beta2.Asset{
				Urn:     "urn:caramlstore:test-caramlstore:feature_table:avg_dispatch_arrival_time_10_mins",
				Name:    "avg_dispatch_arrival_time_10_mins",
				Service: "caramlstore",
				Type:    "feature_table",
				Data: testutils.BuildAny(t, &v1beta2.FeatureTable{
					Namespace: "sauron",
					Entities: []*v1beta2.FeatureTable_Entity{
						{Name: "merchant_uuid", Labels: map[string]string{
							"catch_phrase": "You talkin' to me?",
							"description":  "merchant uuid",
							"value_type":   "STRING",
						}},
					},
					Features: []*v1beta2.Feature{
						{Name: "ongoing_placed_and_waiting_acceptance_orders", DataType: "INT64", EntityName: "customer_orders"},
						{Name: "ongoing_orders", DataType: "INT64", EntityName: "customer_orders"},
						{Name: "merchant_avg_dispatch_arrival_time_10m", DataType: "FLOAT", EntityName: "merchant_driver"},
						{Name: "ongoing_accepted_orders", DataType: "INT64", EntityName: "merchant_orders"},
					},
					CreateTime: timestamppb.New(time.Date(2022, time.September, 19, 22, 42, 0o4, 0, time.UTC)),
					UpdateTime: timestamppb.New(time.Date(2022, time.September, 22, 13, 23, 0o2, 0, time.UTC)),
				}),
				Lineage: &v1beta2.Lineage{
					Upstreams: []*v1beta2.Resource{
						{
							Urn:     "urn:kafka:int-dagstream-kafka:topic:GO_FOOD-delay-allocation-merchant-feature-10m-log",
							Service: "kafka",
							Type:    "topic",
						},
					},
				},
				Labels: map[string]string{"script_engine": "tengo"},
				Owners: []*v1beta2.Owner{{Name: "Big Mom", Email: "big.mom@wholecakeisland.com"}},
			},
		},
		{
			name: "UnknownFields",
			script: heredoc.Doc(`
				asset.does_not_exist = "value"
			`),
			input: &v1beta2.Asset{
				Data: &anypb.Any{TypeUrl: "type.googleapis.com/gotocompany.assets.v1beta2.Table"},
			},
			expected: nil,
			errStr:   "invalid keys: does_not_exist",
		},
		{
			name:   "InvalidInput",
			script: "a := 1",
			input: &v1beta2.Asset{
				Data: &anypb.Any{TypeUrl: "$$$$"},
			},
			expected: nil,
			errStr:   "script processor: structmap",
		},
		{
			name: "ErrRunContext",
			script: heredoc.Doc(`
				a := 5 / 0
			`),
			input: &v1beta2.Asset{
				Data: testutils.BuildAny(t, &v1beta2.FeatureTable{
					Namespace: "sauron",
				}),
			},
			expected: nil,
			errStr:   "script processor: run script",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := New(testutils.Logger)
			err := p.Init(ctx, plugins.Config{
				RawConfig: map[string]interface{}{
					"script": tc.script,
					"engine": "tengo",
				},
			})
			if !assert.NoError(t, err) {
				return
			}

			res, err := p.Process(ctx, models.NewRecord(tc.input))
			if tc.errStr == "" {
				assert.NoError(t, err)
			} else {
				assert.ErrorContains(t, err, tc.errStr)
			}

			testutils.AssertEqualProto(t, tc.expected, res.Data())
		})
	}
}
