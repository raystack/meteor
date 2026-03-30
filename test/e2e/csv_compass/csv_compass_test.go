//go:build integration
// +build integration

package csv_compass_test

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"

	"github.com/raystack/meteor/cmd"
	_ "github.com/raystack/meteor/plugins/extractors"
	_ "github.com/raystack/meteor/plugins/processors"
	_ "github.com/raystack/meteor/plugins/sinks"
	"github.com/raystack/meteor/plugins/sinks/compass"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCSVToCompass(t *testing.T) {
	// Start a mock Compass v2 server that records all requests.
	mock := newMockCompassServer()
	server := httptest.NewServer(mock)
	defer server.Close()

	// Write a temporary recipe that uses the CSV extractor and compass sink.
	recipeContent := fmt.Sprintf(`
name: csv-to-compass-e2e
version: v1beta1
source:
  name: csv
  scope: e2e-test
  config:
    path: ./testdata
sinks:
  - name: compass
    config:
      host: %s
      headers:
        Compass-User-UUID: meteor-e2e@raystack.io
`, server.URL)

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

	// Verify the mock server received the expected requests.
	entities := mock.Entities()
	edges := mock.Edges()

	// CSV extractor should produce 2 files: employees.csv and orders.csv.
	require.Len(t, entities, 2, "expected 2 UpsertEntity calls")

	// Build a map by name for easier assertion.
	entityByName := make(map[string]compass.UpsertEntityRequest)
	for _, e := range entities {
		entityByName[e.Name] = e
	}

	// Verify employees.csv entity.
	emp, ok := entityByName["employees.csv"]
	require.True(t, ok, "expected employees.csv entity")
	assert.Equal(t, "table", emp.Type)
	assert.Equal(t, "csv", emp.Source)
	assert.Contains(t, emp.URN, "employees.csv")
	assert.NotNil(t, emp.Properties, "properties should contain flattened table data")
	// Columns from CSV header should be in properties.
	if columns, ok := emp.Properties["columns"]; ok {
		cols, ok := columns.([]any)
		require.True(t, ok, "columns should be an array")
		assert.Len(t, cols, 4, "employees.csv has 4 columns")
	}

	// Verify orders.csv entity.
	ord, ok := entityByName["orders.csv"]
	require.True(t, ok, "expected orders.csv entity")
	assert.Equal(t, "table", ord.Type)
	assert.Equal(t, "csv", ord.Source)
	assert.Contains(t, ord.URN, "orders.csv")

	// CSV extractor doesn't produce owners, so no edges expected.
	assert.Empty(t, edges, "CSV extractor produces no owners, so no edges expected")

	// Verify headers were sent.
	headers := mock.Headers()
	require.NotEmpty(t, headers)
	foundUUID := false
	for _, h := range headers {
		if h.Get("Compass-User-UUID") == "meteor-e2e@raystack.io" {
			foundUUID = true
			break
		}
	}
	assert.True(t, foundUUID, "expected Compass-User-UUID header in requests")

	// Print summary for visibility.
	fmt.Printf("\n=== E2E Test Summary ===\n")
	fmt.Printf("Entities upserted: %d\n", len(entities))
	fmt.Printf("Edges upserted:    %d\n", len(edges))
	for _, e := range entities {
		fmt.Printf("  - %s (type=%s, source=%s, urn=%s)\n", e.Name, e.Type, e.Source, e.URN)
	}
}

// mockCompassServer simulates Compass v2 Connect RPC endpoints.
type mockCompassServer struct {
	mu       sync.Mutex
	entities []compass.UpsertEntityRequest
	edges    []compass.UpsertEdgeRequest
	headers  []http.Header
}

func newMockCompassServer() *mockCompassServer {
	return &mockCompassServer{}
}

func (m *mockCompassServer) Entities() []compass.UpsertEntityRequest {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]compass.UpsertEntityRequest, len(m.entities))
	copy(out, m.entities)
	return out
}

func (m *mockCompassServer) Edges() []compass.UpsertEdgeRequest {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]compass.UpsertEdgeRequest, len(m.edges))
	copy(out, m.edges)
	return out
}

func (m *mockCompassServer) Headers() []http.Header {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]http.Header, len(m.headers))
	copy(out, m.headers)
	return out
}

func (m *mockCompassServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	m.mu.Lock()
	m.headers = append(m.headers, r.Header.Clone())
	m.mu.Unlock()

	switch r.URL.Path {
	case "/raystack.compass.v1beta1.CompassService/UpsertEntity":
		var req compass.UpsertEntityRequest
		if err := json.Unmarshal(body, &req); err != nil {
			http.Error(w, "bad json", http.StatusBadRequest)
			return
		}
		m.mu.Lock()
		m.entities = append(m.entities, req)
		m.mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, `{"id":"mock-id-%d"}`, len(m.entities))

	case "/raystack.compass.v1beta1.CompassService/UpsertEdge":
		var req compass.UpsertEdgeRequest
		if err := json.Unmarshal(body, &req); err != nil {
			http.Error(w, "bad json", http.StatusBadRequest)
			return
		}
		m.mu.Lock()
		m.edges = append(m.edges, req)
		m.mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, `{}`)

	default:
		http.Error(w, fmt.Sprintf("unknown route: %s", r.URL.Path), http.StatusNotFound)
	}
}
