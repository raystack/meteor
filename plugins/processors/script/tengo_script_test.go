//go:build plugins
// +build plugins

package script

import (
	"context"
	"testing"

	"github.com/MakeNowJust/heredoc"
	"github.com/raystack/meteor/models"
	meteorv1beta1 "github.com/raystack/meteor/models/raystack/meteor/v1beta1"
	"github.com/raystack/meteor/plugins"
	testutils "github.com/raystack/meteor/test/utils"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/structpb"
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
					RawConfig: map[string]any{"engine": "tengo"},
				},
			},
			{
				name: "WithoutEngine",
				cfg: plugins.Config{
					RawConfig: map[string]any{"script": script},
				},
			},
			{
				name: "WithUnsupportedEngine",
				cfg: plugins.Config{
					RawConfig: map[string]any{"script": script, "engine": "goja"},
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
			RawConfig: map[string]any{
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
		input    *meteorv1beta1.Entity
		expected *meteorv1beta1.Entity
		errStr   string
	}{
		{
			name: "EntityWithProperties",
			script: heredoc.Doc(`
				text := import("text")

				merge := func(m1, m2) {
					for k, v in m2 {
						m1[k] = v
					}
					return m1
				}

				asset.properties = merge(asset.properties || {}, {script_engine: "tengo"})
				`),
			input: &meteorv1beta1.Entity{
				Urn:    "urn:caramlstore:test-caramlstore:feature_table:avg_dispatch_arrival_time_10_mins",
				Name:   "avg_dispatch_arrival_time_10_mins",
				Source: "caramlstore",
				Type:   "feature_table",
				Properties: func() *structpb.Struct {
					s, _ := structpb.NewStruct(map[string]any{
						"namespace": "sauron",
					})
					return s
				}(),
			},
			expected: &meteorv1beta1.Entity{
				Urn:    "urn:caramlstore:test-caramlstore:feature_table:avg_dispatch_arrival_time_10_mins",
				Name:   "avg_dispatch_arrival_time_10_mins",
				Source: "caramlstore",
				Type:   "feature_table",
				Properties: func() *structpb.Struct {
					s, _ := structpb.NewStruct(map[string]any{
						"namespace":     "sauron",
						"script_engine": "tengo",
					})
					return s
				}(),
			},
		},
		{
			name: "UnknownFields",
			script: heredoc.Doc(`
				asset.does_not_exist = "value"
			`),
			input: &meteorv1beta1.Entity{
				Urn:  "urn:test:test:table:test",
				Type: "table",
			},
			expected: nil,
			errStr:   "invalid keys: does_not_exist",
		},
		{
			name:   "ModifyEntityName",
			script: `asset.name = "new-name"`,
			input: &meteorv1beta1.Entity{
				Urn:    "urn:test:test:table:test",
				Name:   "old-name",
				Source: "test",
				Type:   "table",
			},
			expected: &meteorv1beta1.Entity{
				Urn:    "urn:test:test:table:test",
				Name:   "new-name",
				Source: "test",
				Type:   "table",
			},
		},
		{
			name: "EntityWithNilProperties",
			script: heredoc.Doc(`
				asset.properties = {new_key: "new_value"}
			`),
			input: &meteorv1beta1.Entity{
				Urn:    "urn:test:test:table:test",
				Name:   "test",
				Source: "test",
				Type:   "table",
			},
			expected: &meteorv1beta1.Entity{
				Urn:    "urn:test:test:table:test",
				Name:   "test",
				Source: "test",
				Type:   "table",
				Properties: func() *structpb.Struct {
					s, _ := structpb.NewStruct(map[string]any{
						"new_key": "new_value",
					})
					return s
				}(),
			},
		},
		{
			name:   "ErrRunContext",
			script: heredoc.Doc(`a := 5 / 0`),
			input: &meteorv1beta1.Entity{
				Urn:    "urn:test:test:feature_table:test",
				Source: "caramlstore",
				Type:   "feature_table",
				Properties: func() *structpb.Struct {
					s, _ := structpb.NewStruct(map[string]any{
						"namespace": "sauron",
					})
					return s
				}(),
			},
			expected: nil,
			errStr:   "script processor: run script",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := New(testutils.Logger)
			err := p.Init(ctx, plugins.Config{
				RawConfig: map[string]any{
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

			testutils.AssertEqualProto(t, tc.expected, res.Entity())
		})
	}

	t.Run("PreservesEdges", func(t *testing.T) {
		p := New(testutils.Logger)
		err := p.Init(ctx, plugins.Config{
			RawConfig: map[string]any{
				"script": `asset.name = "modified"`,
				"engine": "tengo",
			},
		})
		if !assert.NoError(t, err) {
			return
		}

		edges := []*meteorv1beta1.Edge{
			{
				SourceUrn: "urn:test:test:table:src",
				TargetUrn: "urn:test:test:user:owner1",
				Type:      "owned_by",
				Source:     "test",
			},
			{
				SourceUrn: "urn:test:test:table:src",
				TargetUrn: "urn:test:test:table:upstream",
				Type:      "lineage",
				Source:     "test",
			},
		}

		input := models.NewRecord(
			&meteorv1beta1.Entity{
				Urn:    "urn:test:test:table:src",
				Name:   "original",
				Source: "test",
				Type:   "table",
			},
			edges...,
		)

		res, err := p.Process(ctx, input)
		assert.NoError(t, err)

		testutils.AssertEqualProto(t, &meteorv1beta1.Entity{
			Urn:    "urn:test:test:table:src",
			Name:   "modified",
			Source: "test",
			Type:   "table",
		}, res.Entity())

		gotEdges := res.Edges()
		assert.Len(t, gotEdges, 2)
		for i, e := range edges {
			testutils.AssertEqualProto(t, e, gotEdges[i])
		}
	})
}
