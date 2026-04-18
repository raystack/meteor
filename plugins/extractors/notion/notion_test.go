//go:build plugins
// +build plugins

package notion_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/raystack/meteor/models"
	meteorv1beta1 "github.com/raystack/meteor/models/raystack/meteor/v1beta1"
	"github.com/raystack/meteor/plugins"
	extractor "github.com/raystack/meteor/plugins/extractors/notion"
	"github.com/raystack/meteor/test/mocks"
	testutils "github.com/raystack/meteor/test/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const urnScope = "test-notion"

func TestInit(t *testing.T) {
	t.Run("should return error when token is missing", func(t *testing.T) {
		err := extractor.New(testutils.Logger).Init(context.TODO(), plugins.Config{
			URNScope:  urnScope,
			RawConfig: map[string]any{},
		})
		assert.ErrorAs(t, err, &plugins.InvalidConfigError{})
	})

	t.Run("should succeed with valid config", func(t *testing.T) {
		err := extractor.New(testutils.Logger).Init(context.TODO(), plugins.Config{
			URNScope: urnScope,
			RawConfig: map[string]any{
				"token": "ntn_test_token",
			},
		})
		assert.NoError(t, err)
	})
}

func TestExtract(t *testing.T) {
	t.Run("should extract pages with edges", func(t *testing.T) {
		server := newMockServer(t)
		defer server.Close()

		extr := initExtractor(t, server.URL, map[string]any{
			"token":   "test-token",
			"extract": []any{"pages"},
		})

		emitter := mocks.NewEmitter()
		err := extr.Extract(context.Background(), emitter.Push)
		require.NoError(t, err)

		records := emitter.Get()
		require.Len(t, records, 2)

		// Verify parent page.
		parentPage := findByURNSuffix(records, "page-1")
		require.NotNil(t, parentPage)
		assert.Equal(t, "Architecture Docs", parentPage.Entity().GetName())
		assert.Equal(t, "document", parentPage.Entity().GetType())
		assert.Equal(t, "notion", parentPage.Entity().GetSource())

		parentEdges := parentPage.Edges()
		assert.Len(t, parentEdges, 1) // owned_by only (workspace parent is skipped)
		assert.NotNil(t, findEdge(parentEdges, "owned_by"))

		// Verify child page with child_of edge.
		childPage := findByURNSuffix(records, "page-2")
		require.NotNil(t, childPage)
		assert.Equal(t, "Database Design", childPage.Entity().GetName())

		childEdges := childPage.Edges()
		require.Len(t, childEdges, 2) // child_of + owned_by
		childOfEdge := findEdge(childEdges, "child_of")
		require.NotNil(t, childOfEdge)
		assert.Contains(t, childOfEdge.GetTargetUrn(), "page-1")
	})

	t.Run("should extract databases with columns and description", func(t *testing.T) {
		server := newMockServer(t)
		defer server.Close()

		extr := initExtractor(t, server.URL, map[string]any{
			"token":   "test-token",
			"extract": []any{"databases"},
		})

		emitter := mocks.NewEmitter()
		err := extr.Extract(context.Background(), emitter.Push)
		require.NoError(t, err)

		records := emitter.Get()
		require.Len(t, records, 1)

		dbRecord := records[0]
		assert.Equal(t, "Project Tracker", dbRecord.Entity().GetName())
		assert.Equal(t, "Track engineering projects", dbRecord.Entity().GetDescription())

		props := dbRecord.Entity().GetProperties().AsMap()
		assert.NotNil(t, props["columns"])
		assert.Equal(t, "db-1", props["database_id"])

		// Should have owned_by edge.
		edges := dbRecord.Edges()
		assert.NotNil(t, findEdge(edges, "owned_by"))
	})

	t.Run("should detect URN references in page content", func(t *testing.T) {
		server := newMockServerWithURNs(t)
		defer server.Close()

		extr := initExtractor(t, server.URL, map[string]any{
			"token":   "test-token",
			"extract": []any{"pages"},
		})

		emitter := mocks.NewEmitter()
		err := extr.Extract(context.Background(), emitter.Push)
		require.NoError(t, err)

		records := emitter.Get()
		require.Len(t, records, 1)

		edges := records[0].Edges()
		docEdge := findEdge(edges, "documented_by")
		require.NotNil(t, docEdge)
		assert.Equal(t, "urn:bigquery:prod:table:project.dataset.orders", docEdge.GetTargetUrn())
	})

	t.Run("should skip archived pages", func(t *testing.T) {
		server := newMockServerWithArchived(t)
		defer server.Close()

		extr := initExtractor(t, server.URL, map[string]any{
			"token":   "test-token",
			"extract": []any{"pages"},
		})

		emitter := mocks.NewEmitter()
		err := extr.Extract(context.Background(), emitter.Push)
		require.NoError(t, err)

		records := emitter.Get()
		require.Len(t, records, 1)
		assert.Equal(t, "Active Page", records[0].Entity().GetName())
	})
}

// --- test helpers ---

func initExtractor(t *testing.T, serverURL string, rawConfig map[string]any) *extractor.Extractor {
	t.Helper()
	rawConfig["base_url"] = serverURL
	extr := extractor.New(testutils.Logger)
	err := extr.Init(context.Background(), plugins.Config{
		URNScope:  urnScope,
		RawConfig: rawConfig,
	})
	require.NoError(t, err)
	return extr
}

func findByURNSuffix(records []models.Record, suffix string) *models.Record {
	for i, r := range records {
		urn := r.Entity().GetUrn()
		if len(urn) >= len(suffix) && urn[len(urn)-len(suffix):] == suffix {
			return &records[i]
		}
	}
	return nil
}

func findEdge(edges []*meteorv1beta1.Edge, typ string) *meteorv1beta1.Edge {
	for _, e := range edges {
		if e.GetType() == typ {
			return e
		}
	}
	return nil
}

// --- mock servers ---

func newMockServer(t *testing.T) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()

	mux.HandleFunc("/v1/search", func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		json.NewDecoder(r.Body).Decode(&body)

		filter := body["filter"].(map[string]any)
		value := filter["value"].(string)

		switch value {
		case "page":
			writeJSON(w, map[string]any{
				"results": []map[string]any{
					makePage("page-1", "Architecture Docs", "", "workspace", "user-a", "Alice", false),
					makePage("page-2", "Database Design", "page-1", "page_id", "user-b", "Bob", false),
				},
				"has_more":    false,
				"next_cursor": "",
			})
		case "database":
			writeJSON(w, map[string]any{
				"results": []map[string]any{
					makeDatabase("db-1", "Project Tracker", "Track engineering projects", "user-a", "Alice",
						map[string]any{
							"Name":     map[string]any{"type": "title"},
							"Status":   map[string]any{"type": "select"},
							"Priority": map[string]any{"type": "number"},
						}),
				},
				"has_more":    false,
				"next_cursor": "",
			})
		}
	})

	mux.HandleFunc("/v1/blocks/page-1/children", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, map[string]any{
			"results": []map[string]any{
				makeBlock("paragraph", "Overview of architecture."),
			},
			"has_more":    false,
			"next_cursor": "",
		})
	})

	mux.HandleFunc("/v1/blocks/page-2/children", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, map[string]any{
			"results": []map[string]any{
				makeBlock("paragraph", "Database schema details."),
			},
			"has_more":    false,
			"next_cursor": "",
		})
	})

	return httptest.NewServer(mux)
}

func newMockServerWithURNs(t *testing.T) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()

	mux.HandleFunc("/v1/search", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, map[string]any{
			"results": []map[string]any{
				makePage("page-urn", "Pipeline Docs", "", "workspace", "user-a", "Alice", false),
			},
			"has_more":    false,
			"next_cursor": "",
		})
	})

	mux.HandleFunc("/v1/blocks/page-urn/children", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, map[string]any{
			"results": []map[string]any{
				makeBlock("paragraph", "This reads from urn:bigquery:prod:table:project.dataset.orders daily."),
			},
			"has_more":    false,
			"next_cursor": "",
		})
	})

	return httptest.NewServer(mux)
}

func newMockServerWithArchived(t *testing.T) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()

	mux.HandleFunc("/v1/search", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, map[string]any{
			"results": []map[string]any{
				makePage("page-active", "Active Page", "", "workspace", "user-a", "Alice", false),
				makePage("page-archived", "Archived Page", "", "workspace", "user-b", "Bob", true),
			},
			"has_more":    false,
			"next_cursor": "",
		})
	})

	mux.HandleFunc("/v1/blocks/page-active/children", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, map[string]any{
			"results":     []map[string]any{},
			"has_more":    false,
			"next_cursor": "",
		})
	})

	return httptest.NewServer(mux)
}

// --- mock data builders ---

func makePage(id, title, parentID, parentType, userID, userName string, archived bool) map[string]any {
	parent := map[string]any{"type": parentType}
	if parentType == "page_id" {
		parent["page_id"] = parentID
	}

	return map[string]any{
		"object":           "page",
		"id":               id,
		"created_time":     "2024-01-15T10:30:00.000Z",
		"last_edited_time": "2024-03-20T14:15:00.000Z",
		"created_by":       map[string]any{"id": userID, "name": userName},
		"last_edited_by":   map[string]any{"id": userID, "name": userName},
		"archived":         archived,
		"url":              "https://www.notion.so/" + id,
		"parent":           parent,
		"properties": map[string]any{
			"Name": map[string]any{
				"type": "title",
				"title": []map[string]any{
					{"plain_text": title},
				},
			},
		},
	}
}

func makeDatabase(id, title, description, userID, userName string, props map[string]any) map[string]any {
	return map[string]any{
		"object":           "database",
		"id":               id,
		"created_time":     "2024-01-10T09:00:00.000Z",
		"last_edited_time": "2024-03-18T16:00:00.000Z",
		"created_by":       map[string]any{"id": userID, "name": userName},
		"last_edited_by":   map[string]any{"id": userID, "name": userName},
		"title":            []map[string]any{{"plain_text": title}},
		"description":      []map[string]any{{"plain_text": description}},
		"archived":         false,
		"url":              "https://www.notion.so/" + id,
		"parent":           map[string]any{"type": "workspace"},
		"properties":       props,
	}
}

func makeBlock(blockType, text string) map[string]any {
	return map[string]any{
		"type": blockType,
		blockType: map[string]any{
			"rich_text": []map[string]any{
				{"plain_text": text},
			},
		},
	}
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v) //nolint:errcheck
}
