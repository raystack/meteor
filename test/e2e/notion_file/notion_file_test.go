//go:build integration
// +build integration

package notion_file_test

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

func TestNotionToFile(t *testing.T) {
	mock := newMockNotionServer()
	server := httptest.NewServer(mock)
	defer server.Close()

	outFile, err := os.CreateTemp("", "notion-e2e-*.ndjson")
	require.NoError(t, err)
	outFile.Close()
	defer os.Remove(outFile.Name())

	recipeContent := fmt.Sprintf(`
name: notion-to-file-e2e
version: v1beta1
source:
  name: notion
  scope: e2e-test
  config:
    token: test-token
    base_url: %s
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

	command := cmd.RunCmd()
	command.SetArgs([]string{recipeFile.Name()})
	err = command.Execute()
	require.NoError(t, err)

	data, err := os.ReadFile(outFile.Name())
	require.NoError(t, err)

	records := parseNDJSON(t, data)

	// Expect: 2 pages + 1 database = 3 records.
	require.Len(t, records, 3, "expected 2 pages + 1 database")

	// Verify page records.
	docRecords := findByEntityType(records, "document")
	require.Len(t, docRecords, 3)

	// Find the parent page.
	archPage := findByName(docRecords, "Architecture Docs")
	require.NotNil(t, archPage)
	archEntity := archPage["entity"].(map[string]any)
	assert.Contains(t, archEntity["urn"], "document:page-1")

	// Find child page and verify child_of edge.
	childPage := findByName(docRecords, "Database Design")
	require.NotNil(t, childPage)
	childEdges := toEdges(childPage)
	assertHasEdgeType(t, childEdges, "child_of")
	assertHasEdgeType(t, childEdges, "owned_by")

	// Find database and verify columns.
	dbPage := findByName(docRecords, "Project Tracker")
	require.NotNil(t, dbPage)
	dbEntity := dbPage["entity"].(map[string]any)
	assert.Equal(t, "notion", dbEntity["source"])

	dbProps := dbEntity["properties"].(map[string]any)
	assert.NotNil(t, dbProps["columns"])

	// Print summary.
	fmt.Printf("\n=== Notion E2E Test Summary ===\n")
	fmt.Printf("Total records: %d\n", len(records))
	for _, r := range records {
		e := r["entity"].(map[string]any)
		fmt.Printf("  - %s (type=%s, urn=%s)\n", e["name"], e["type"], e["urn"])
	}
}

// --- Mock Notion Server ---

func newMockNotionServer() http.Handler {
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
					makePage("page-1", "Architecture Docs", "", "workspace", "user-a", "Alice"),
					makePage("page-2", "Database Design", "page-1", "page_id", "user-b", "Bob"),
				},
				"has_more": false,
			})
		case "database":
			writeJSON(w, map[string]any{
				"results": []map[string]any{
					makeDatabase("db-1", "Project Tracker", "Engineering projects", "user-a", "Alice",
						map[string]any{
							"Name":   map[string]any{"type": "title"},
							"Status": map[string]any{"type": "select"},
						}),
				},
				"has_more": false,
			})
		}
	})

	mux.HandleFunc("/v1/blocks/page-1/children", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, map[string]any{
			"results":  []map[string]any{makeBlock("paragraph", "Architecture overview content.")},
			"has_more": false,
		})
	})

	mux.HandleFunc("/v1/blocks/page-2/children", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, map[string]any{
			"results":  []map[string]any{makeBlock("paragraph", "Database schema docs.")},
			"has_more": false,
		})
	})

	return mux
}

func makePage(id, title, parentID, parentType, userID, userName string) map[string]any {
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
		"archived":         false,
		"url":              "https://www.notion.so/" + id,
		"parent":           parent,
		"properties": map[string]any{
			"Name": map[string]any{
				"type":  "title",
				"title": []map[string]any{{"plain_text": title}},
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
			"rich_text": []map[string]any{{"plain_text": text}},
		},
	}
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v) //nolint:errcheck
}

// --- helpers ---

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

func assertHasEdgeType(t *testing.T, edges []map[string]any, typ string) {
	t.Helper()
	for _, e := range edges {
		if e["type"] == typ {
			return
		}
	}
	t.Errorf("expected edge of type %q, found none in %d edges", typ, len(edges))
}
