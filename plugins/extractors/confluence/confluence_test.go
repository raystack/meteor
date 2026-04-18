//go:build plugins
// +build plugins

package confluence_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/raystack/meteor/models"
	meteorv1beta1 "github.com/raystack/meteor/models/raystack/meteor/v1beta1"
	"github.com/raystack/meteor/plugins"
	extractor "github.com/raystack/meteor/plugins/extractors/confluence"
	"github.com/raystack/meteor/test/mocks"
	testutils "github.com/raystack/meteor/test/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const urnScope = "test-confluence"

func TestInit(t *testing.T) {
	t.Run("should return error when base_url is missing", func(t *testing.T) {
		err := extractor.New(testutils.Logger).Init(context.TODO(), plugins.Config{
			URNScope: urnScope,
			RawConfig: map[string]any{
				"username": "user@test.com",
				"token":    "test-token",
			},
		})
		assert.ErrorAs(t, err, &plugins.InvalidConfigError{})
	})

	t.Run("should return error when token is missing", func(t *testing.T) {
		err := extractor.New(testutils.Logger).Init(context.TODO(), plugins.Config{
			URNScope: urnScope,
			RawConfig: map[string]any{
				"base_url": "https://test.atlassian.net/wiki",
				"username": "user@test.com",
			},
		})
		assert.ErrorAs(t, err, &plugins.InvalidConfigError{})
	})

	t.Run("should succeed with valid config", func(t *testing.T) {
		err := extractor.New(testutils.Logger).Init(context.TODO(), plugins.Config{
			URNScope: urnScope,
			RawConfig: map[string]any{
				"base_url": "https://test.atlassian.net/wiki",
				"username": "user@test.com",
				"token":    "test-token",
			},
		})
		assert.NoError(t, err)
	})
}

func TestExtract(t *testing.T) {
	t.Run("should extract spaces and pages with edges", func(t *testing.T) {
		server := newMockServer(t)
		defer server.Close()

		extr := initExtractor(t, map[string]any{
			"base_url": server.URL,
			"username": "user@test.com",
			"token":    "test-token",
		})

		emitter := mocks.NewEmitter()
		err := extr.Extract(context.Background(), emitter.Push)
		require.NoError(t, err)

		records := emitter.Get()
		// 1 space + 2 pages = 3 records.
		require.Len(t, records, 3)

		// Verify space entity.
		spaceRecord := findByType(records, "space")
		require.NotNil(t, spaceRecord)
		spaceEntity := spaceRecord.Entity()
		assert.Equal(t, "Engineering", spaceEntity.GetName())
		assert.Equal(t, "confluence", spaceEntity.GetSource())
		props := spaceEntity.GetProperties().AsMap()
		assert.Equal(t, "ENG", props["space_key"])

		// Verify page entity.
		pageRecords := findAllByType(records, "document")
		require.Len(t, pageRecords, 2)

		// Find the child page.
		childPage := findByURNSuffix(records, "202")
		require.NotNil(t, childPage)
		childEntity := childPage.Entity()
		assert.Equal(t, "Child Page", childEntity.GetName())

		// Verify edges on child page: belongs_to, child_of, owned_by.
		edges := childPage.Edges()
		assert.Len(t, edges, 3)
		assert.NotNil(t, findEdge(edges, "belongs_to"))
		assert.NotNil(t, findEdge(edges, "child_of"))
		assert.NotNil(t, findEdge(edges, "owned_by"))

		childOfEdge := findEdge(edges, "child_of")
		assert.Contains(t, childOfEdge.GetTargetUrn(), "201")
	})

	t.Run("should detect URN references in page content", func(t *testing.T) {
		server := newMockServerWithURNs(t)
		defer server.Close()

		extr := initExtractor(t, map[string]any{
			"base_url": server.URL,
			"username": "user@test.com",
			"token":    "test-token",
		})

		emitter := mocks.NewEmitter()
		err := extr.Extract(context.Background(), emitter.Push)
		require.NoError(t, err)

		records := emitter.Get()
		pageRecords := findAllByType(records, "document")
		require.Len(t, pageRecords, 1)

		edges := pageRecords[0].Edges()
		docEdge := findEdge(edges, "documented_by")
		require.NotNil(t, docEdge)
		assert.Equal(t, "urn:bigquery:prod:table:project.dataset.orders", docEdge.GetTargetUrn())
	})

	t.Run("should exclude spaces in exclude list", func(t *testing.T) {
		server := newMockServerMultiSpace(t)
		defer server.Close()

		extr := initExtractor(t, map[string]any{
			"base_url": server.URL,
			"username": "user@test.com",
			"token":    "test-token",
			"exclude":  []any{"ARCHIVE"},
		})

		emitter := mocks.NewEmitter()
		err := extr.Extract(context.Background(), emitter.Push)
		require.NoError(t, err)

		records := emitter.Get()
		spaceRecords := findAllByType(records, "space")
		require.Len(t, spaceRecords, 1)

		props := spaceRecords[0].Entity().GetProperties().AsMap()
		assert.Equal(t, "ENG", props["space_key"])
	})
}

// --- test helpers ---

func initExtractor(t *testing.T, rawConfig map[string]any) *extractor.Extractor {
	t.Helper()
	extr := extractor.New(testutils.Logger)
	err := extr.Init(context.Background(), plugins.Config{
		URNScope:  urnScope,
		RawConfig: rawConfig,
	})
	require.NoError(t, err)
	return extr
}

func findByType(records []models.Record, typ string) *models.Record {
	for i, r := range records {
		if r.Entity().GetType() == typ {
			return &records[i]
		}
	}
	return nil
}

func findAllByType(records []models.Record, typ string) []models.Record {
	var out []models.Record
	for _, r := range records {
		if r.Entity().GetType() == typ {
			out = append(out, r)
		}
	}
	return out
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

func newMockServer(t *testing.T) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()

	mux.HandleFunc("/api/v2/spaces", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, map[string]any{
			"results": []map[string]any{
				{
					"id":     "100",
					"key":    "ENG",
					"name":   "Engineering",
					"type":   "global",
					"status": "current",
					"_links": map[string]any{"webui": "/spaces/ENG"},
				},
			},
			"_links": map[string]any{},
		})
	})

	mux.HandleFunc("/api/v2/pages", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, map[string]any{
			"results": []map[string]any{
				{
					"id":       "201",
					"title":    "Architecture Overview",
					"status":   "current",
					"spaceId":  "100",
					"parentId": "",
					"authorId": "user-1",
					"version":  map[string]any{"number": 3, "authorId": "user-1", "createdAt": "2024-03-20T14:00:00Z"},
					"body":     map[string]any{"storage": map[string]any{"value": "<p>Overview of architecture</p>"}},
					"_links":   map[string]any{"webui": "/spaces/ENG/pages/201"},
				},
				{
					"id":       "202",
					"title":    "Child Page",
					"status":   "current",
					"spaceId":  "100",
					"parentId": "201",
					"authorId": "user-2",
					"version":  map[string]any{"number": 1, "authorId": "user-2", "createdAt": "2024-03-21T10:00:00Z"},
					"body":     map[string]any{"storage": map[string]any{"value": "<p>Child content</p>"}},
					"_links":   map[string]any{"webui": "/spaces/ENG/pages/202"},
				},
			},
			"_links": map[string]any{},
		})
	})

	mux.HandleFunc("/api/v2/pages/201/labels", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, map[string]any{
			"results": []map[string]any{
				{"id": "1", "name": "architecture"},
			},
		})
	})

	mux.HandleFunc("/api/v2/pages/202/labels", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, map[string]any{
			"results": []map[string]any{},
		})
	})

	return httptest.NewServer(mux)
}

func newMockServerWithURNs(t *testing.T) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()

	mux.HandleFunc("/api/v2/spaces", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, map[string]any{
			"results": []map[string]any{
				{"id": "100", "key": "DATA", "name": "Data", "type": "global", "status": "current", "_links": map[string]any{}},
			},
			"_links": map[string]any{},
		})
	})

	mux.HandleFunc("/api/v2/pages", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, map[string]any{
			"results": []map[string]any{
				{
					"id":       "301",
					"title":    "Orders Pipeline",
					"status":   "current",
					"spaceId":  "100",
					"parentId": "",
					"authorId": "user-1",
					"version":  map[string]any{"number": 1, "authorId": "user-1", "createdAt": "2024-01-01T00:00:00Z"},
					"body": map[string]any{"storage": map[string]any{
						"value": "<p>This pipeline reads from urn:bigquery:prod:table:project.dataset.orders and writes results.</p>",
					}},
					"_links": map[string]any{},
				},
			},
			"_links": map[string]any{},
		})
	})

	mux.HandleFunc("/api/v2/pages/301/labels", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, map[string]any{"results": []map[string]any{}})
	})

	return httptest.NewServer(mux)
}

func newMockServerMultiSpace(t *testing.T) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()

	mux.HandleFunc("/api/v2/spaces", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, map[string]any{
			"results": []map[string]any{
				{"id": "100", "key": "ENG", "name": "Engineering", "type": "global", "status": "current", "_links": map[string]any{}},
				{"id": "200", "key": "ARCHIVE", "name": "Archive", "type": "global", "status": "current", "_links": map[string]any{}},
			},
			"_links": map[string]any{},
		})
	})

	mux.HandleFunc("/api/v2/pages", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, map[string]any{
			"results": []map[string]any{},
			"_links":  map[string]any{},
		})
	})

	return httptest.NewServer(mux)
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v) //nolint:errcheck
}
