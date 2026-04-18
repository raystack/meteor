//go:build integration
// +build integration

package confluence_file_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/raystack/meteor/cmd"
	_ "github.com/raystack/meteor/plugins/extractors"
	_ "github.com/raystack/meteor/plugins/processors"
	_ "github.com/raystack/meteor/plugins/sinks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfluenceToFile(t *testing.T) {
	// Start a mock Confluence v2 API server.
	mock := newMockConfluenceServer()
	server := httptest.NewServer(mock)
	defer server.Close()

	// Create a temp file for the file sink output.
	outFile, err := os.CreateTemp("", "confluence-e2e-*.ndjson")
	require.NoError(t, err)
	outFile.Close()
	defer os.Remove(outFile.Name())

	// Write a temporary recipe.
	recipeContent := fmt.Sprintf(`
name: confluence-to-file-e2e
version: v1beta1
source:
  name: confluence
  scope: e2e-test
  config:
    base_url: %s
    username: test@example.com
    token: test-token
    spaces:
      - ENG
sinks:
  - name: file
    config:
      path: %s
      format: ndjson
      overwrite: true
`, server.URL, outFile.Name())

	recipeFile, err := os.CreateTemp("", "recipe-*.yml")
	require.NoError(t, err)
	defer os.Remove(recipeFile.Name())

	_, err = recipeFile.WriteString(recipeContent)
	require.NoError(t, err)
	require.NoError(t, recipeFile.Close())

	// Run meteor with the recipe.
	command := cmd.RunCmd()
	command.SetArgs([]string{recipeFile.Name()})
	err = command.Execute()
	require.NoError(t, err)

	// Read and parse the output file.
	data, err := os.ReadFile(outFile.Name())
	require.NoError(t, err)

	records := parseNDJSON(t, data)

	// Expect: 1 space + 3 pages = 4 records.
	require.Len(t, records, 4, "expected 1 space + 3 pages")

	// Verify space record.
	spaceRec := findByEntityType(records, "space")
	require.Len(t, spaceRec, 1)
	spaceEntity := spaceRec[0]["entity"].(map[string]any)
	assert.Equal(t, "Engineering", spaceEntity["name"])
	assert.Equal(t, "confluence", spaceEntity["source"])
	assert.Contains(t, spaceEntity["urn"], "space:ENG")

	spaceProps := spaceEntity["properties"].(map[string]any)
	assert.Equal(t, "ENG", spaceProps["space_key"])
	assert.Equal(t, "global", spaceProps["space_type"])

	// Verify document records.
	docRecs := findByEntityType(records, "document")
	require.Len(t, docRecs, 3)

	// Find specific pages by name.
	archPage := findByName(docRecs, "Architecture Overview")
	require.NotNil(t, archPage)
	archEntity := archPage["entity"].(map[string]any)
	assert.Contains(t, archEntity["urn"], "document:101")

	// Verify edges on Architecture Overview: belongs_to + owned_by (no parent).
	archEdges := toEdges(archPage)
	assert.Len(t, archEdges, 2)
	assertHasEdgeType(t, archEdges, "belongs_to")
	assertHasEdgeType(t, archEdges, "owned_by")

	// Verify child page has child_of edge.
	childPage := findByName(docRecs, "Database Design")
	require.NotNil(t, childPage)
	childEdges := toEdges(childPage)
	assertHasEdgeType(t, childEdges, "child_of")
	assertHasEdgeType(t, childEdges, "belongs_to")
	assertHasEdgeType(t, childEdges, "owned_by")

	// Verify URN-reference page has documented_by edge.
	urnPage := findByName(docRecs, "Pipeline Docs")
	require.NotNil(t, urnPage)
	urnEdges := toEdges(urnPage)
	assertHasEdgeType(t, urnEdges, "documented_by")

	docByEdge := findEdgeByType(urnEdges, "documented_by")
	require.NotNil(t, docByEdge)
	assert.Equal(t, "urn:bigquery:prod:table:project.dataset.orders", docByEdge["target_urn"])

	// Verify labels on Architecture Overview.
	archProps := archEntity["properties"].(map[string]any)
	labels, ok := archProps["labels"].([]any)
	require.True(t, ok)
	assert.Contains(t, labels, "architecture")
	assert.Contains(t, labels, "design")

	// Print summary.
	fmt.Printf("\n=== Confluence E2E Test Summary ===\n")
	fmt.Printf("Total records: %d\n", len(records))
	fmt.Printf("  Spaces:    %d\n", len(spaceRec))
	fmt.Printf("  Documents: %d\n", len(docRecs))
	for _, r := range records {
		e := r["entity"].(map[string]any)
		fmt.Printf("  - %s (type=%s, urn=%s)\n", e["name"], e["type"], e["urn"])
	}
}

// --- Mock Confluence Server ---

func newMockConfluenceServer() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/api/v2/spaces", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, map[string]any{
			"results": []map[string]any{
				{
					"id":     "10",
					"key":    "ENG",
					"name":   "Engineering",
					"type":   "global",
					"status": "current",
					"description": map[string]any{
						"plain": map[string]any{"value": "Engineering team docs"},
					},
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
					"id":       "101",
					"title":    "Architecture Overview",
					"status":   "current",
					"spaceId":  "10",
					"parentId": "",
					"authorId": "user-a",
					"createdAt": "2024-01-10T09:00:00Z",
					"version":  map[string]any{"number": 5, "authorId": "user-a", "createdAt": "2024-03-15T14:30:00Z"},
					"body":     map[string]any{"storage": map[string]any{"value": "<p>High-level architecture overview.</p>"}},
					"_links":   map[string]any{"webui": "/spaces/ENG/pages/101"},
				},
				{
					"id":       "102",
					"title":    "Database Design",
					"status":   "current",
					"spaceId":  "10",
					"parentId": "101",
					"authorId": "user-b",
					"createdAt": "2024-02-01T10:00:00Z",
					"version":  map[string]any{"number": 2, "authorId": "user-b", "createdAt": "2024-03-20T11:00:00Z"},
					"body":     map[string]any{"storage": map[string]any{"value": "<p>Database schema design decisions.</p>"}},
					"_links":   map[string]any{"webui": "/spaces/ENG/pages/102"},
				},
				{
					"id":       "103",
					"title":    "Pipeline Docs",
					"status":   "current",
					"spaceId":  "10",
					"parentId": "101",
					"authorId": "user-a",
					"createdAt": "2024-03-01T08:00:00Z",
					"version":  map[string]any{"number": 1, "authorId": "user-a", "createdAt": "2024-03-01T08:00:00Z"},
					"body": map[string]any{"storage": map[string]any{
						"value": "<p>This pipeline reads from urn:bigquery:prod:table:project.dataset.orders daily.</p>",
					}},
					"_links": map[string]any{"webui": "/spaces/ENG/pages/103"},
				},
			},
			"_links": map[string]any{},
		})
	})

	mux.HandleFunc("/api/v2/pages/101/labels", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, map[string]any{
			"results": []map[string]any{
				{"id": "1", "name": "architecture"},
				{"id": "2", "name": "design"},
			},
		})
	})

	mux.HandleFunc("/api/v2/pages/102/labels", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, map[string]any{
			"results": []map[string]any{
				{"id": "3", "name": "database"},
			},
		})
	})

	mux.HandleFunc("/api/v2/pages/103/labels", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, map[string]any{
			"results": []map[string]any{},
		})
	})

	return mux
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v) //nolint:errcheck
}

// --- Assertion helpers ---

func parseNDJSON(t *testing.T, data []byte) []map[string]any {
	t.Helper()
	var records []map[string]any
	for _, line := range splitLines(data) {
		if len(line) == 0 {
			continue
		}
		var rec map[string]any
		require.NoError(t, json.Unmarshal(line, &rec))
		records = append(records, rec)
	}
	return records
}

func splitLines(data []byte) [][]byte {
	var lines [][]byte
	start := 0
	for i, b := range data {
		if b == '\n' {
			lines = append(lines, data[start:i])
			start = i + 1
		}
	}
	if start < len(data) {
		lines = append(lines, data[start:])
	}
	return lines
}

func findByEntityType(records []map[string]any, typ string) []map[string]any {
	var out []map[string]any
	for _, r := range records {
		if e, ok := r["entity"].(map[string]any); ok {
			if e["type"] == typ {
				out = append(out, r)
			}
		}
	}
	return out
}

func findByName(records []map[string]any, name string) map[string]any {
	for _, r := range records {
		if e, ok := r["entity"].(map[string]any); ok {
			if e["name"] == name {
				return r
			}
		}
	}
	return nil
}

func toEdges(record map[string]any) []map[string]any {
	edgesRaw, ok := record["edges"].([]any)
	if !ok {
		return nil
	}
	var edges []map[string]any
	for _, e := range edgesRaw {
		if m, ok := e.(map[string]any); ok {
			edges = append(edges, m)
		}
	}
	return edges
}

func findEdgeByType(edges []map[string]any, typ string) map[string]any {
	for _, e := range edges {
		if e["type"] == typ {
			return e
		}
	}
	return nil
}

func assertHasEdgeType(t *testing.T, edges []map[string]any, typ string) {
	t.Helper()
	for _, e := range edges {
		if e["type"] == typ {
			return
		}
	}
	t.Errorf("expected edge of type %q, found none in %d edges", typ, len(edges))
}
