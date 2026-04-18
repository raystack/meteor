//go:build plugins
// +build plugins

package github_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	gh "github.com/google/go-github/v68/github"
	"github.com/raystack/meteor/models"
	meteorv1beta1 "github.com/raystack/meteor/models/raystack/meteor/v1beta1"
	"github.com/raystack/meteor/plugins"
	extractor "github.com/raystack/meteor/plugins/extractors/github"
	"github.com/raystack/meteor/test/mocks"
	testutils "github.com/raystack/meteor/test/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const urnScope = "test-github"

func TestInit(t *testing.T) {
	t.Run("should return error when org is missing", func(t *testing.T) {
		err := extractor.New(testutils.Logger).Init(context.TODO(), plugins.Config{
			URNScope: urnScope,
			RawConfig: map[string]any{
				"token": "some-token",
			},
		})
		assert.ErrorAs(t, err, &plugins.InvalidConfigError{})
	})

	t.Run("should return error when token is missing", func(t *testing.T) {
		err := extractor.New(testutils.Logger).Init(context.TODO(), plugins.Config{
			URNScope: urnScope,
			RawConfig: map[string]any{
				"org": "my-org",
			},
		})
		assert.ErrorAs(t, err, &plugins.InvalidConfigError{})
	})

	t.Run("should succeed with valid config", func(t *testing.T) {
		err := extractor.New(testutils.Logger).Init(context.TODO(), plugins.Config{
			URNScope: urnScope,
			RawConfig: map[string]any{
				"org":   "my-org",
				"token": "some-token",
			},
		})
		assert.NoError(t, err)
	})
}

func TestExtract(t *testing.T) {
	t.Run("should extract users with member_of edges", func(t *testing.T) {
		server := setupServer(t, serverConfig{
			members: []*gh.User{
				{Login: strPtr("alice"), NodeID: strPtr("U_alice")},
			},
			userDetails: map[string]*gh.User{
				"alice": {
					NodeID:    strPtr("U_alice"),
					Login:     strPtr("alice"),
					Name:      strPtr("Alice Smith"),
					Email:     strPtr("alice@example.com"),
					Company:   strPtr("Acme"),
					Location:  strPtr("NYC"),
					Bio:       strPtr("Engineer"),
					AvatarURL: strPtr("https://avatar/alice"),
					HTMLURL:   strPtr("https://github.com/alice"),
				},
			},
		})
		defer server.Close()

		extr := initExtractor(t, server.URL, map[string]any{
			"extract": []string{"users"},
		})

		emitter := mocks.NewEmitter()
		err := extr.Extract(context.Background(), emitter.Push)
		require.NoError(t, err)

		records := emitter.Get()
		require.Len(t, records, 1)

		entity := records[0].Entity()
		assert.Equal(t, models.NewURN("github", urnScope, "user", "U_alice"), entity.GetUrn())
		assert.Equal(t, "user", entity.GetType())
		assert.Equal(t, "Alice Smith", entity.GetName())
		assert.Equal(t, "github", entity.GetSource())

		props := entity.GetProperties().AsMap()
		assert.Equal(t, "alice@example.com", props["email"])
		assert.Equal(t, "alice", props["username"])
		assert.Equal(t, "Alice Smith", props["full_name"])
		assert.Equal(t, "Acme", props["company"])
		assert.Equal(t, "NYC", props["location"])
		assert.Equal(t, "Engineer", props["bio"])
		assert.Equal(t, "active", props["status"])

		edges := records[0].Edges()
		require.Len(t, edges, 1)
		assert.Equal(t, "member_of", edges[0].GetType())
		assert.Equal(t, models.NewURN("github", urnScope, "user", "U_alice"), edges[0].GetSourceUrn())
		assert.Equal(t, models.NewURN("github", urnScope, "org", "my-org"), edges[0].GetTargetUrn())
	})

	t.Run("should extract repositories with owned_by edges", func(t *testing.T) {
		server := setupServer(t, serverConfig{
			repos: []*gh.Repository{
				{
					NodeID:        strPtr("R_repo1"),
					Name:          strPtr("meteor"),
					FullName:      strPtr("my-org/meteor"),
					Description:   strPtr("Metadata collector"),
					HTMLURL:       strPtr("https://github.com/my-org/meteor"),
					Language:      strPtr("Go"),
					Visibility:    strPtr("public"),
					DefaultBranch: strPtr("main"),
					Archived:      boolPtr(false),
					Fork:          boolPtr(false),
					StargazersCount: intPtr(42),
					ForksCount:      intPtr(5),
					OpenIssuesCount: intPtr(3),
					Topics:          []string{"metadata", "golang"},
					Owner: &gh.User{
						NodeID: strPtr("U_owner1"),
					},
				},
			},
		})
		defer server.Close()

		extr := initExtractor(t, server.URL, map[string]any{
			"extract": []string{"repositories"},
		})

		emitter := mocks.NewEmitter()
		err := extr.Extract(context.Background(), emitter.Push)
		require.NoError(t, err)

		records := emitter.Get()
		require.Len(t, records, 1)

		entity := records[0].Entity()
		assert.Equal(t, models.NewURN("github", urnScope, "repository", "R_repo1"), entity.GetUrn())
		assert.Equal(t, "repository", entity.GetType())
		assert.Equal(t, "meteor", entity.GetName())

		props := entity.GetProperties().AsMap()
		assert.Equal(t, "my-org/meteor", props["full_name"])
		assert.Equal(t, "Go", props["language"])
		assert.Equal(t, "public", props["visibility"])
		assert.Equal(t, float64(42), props["stargazers"])

		edges := records[0].Edges()
		require.Len(t, edges, 1)
		assert.Equal(t, "owned_by", edges[0].GetType())
		assert.Equal(t, models.NewURN("github", urnScope, "repository", "R_repo1"), edges[0].GetSourceUrn())
		assert.Equal(t, models.NewURN("github", urnScope, "user", "U_owner1"), edges[0].GetTargetUrn())
	})

	t.Run("should extract teams with member_of edges", func(t *testing.T) {
		server := setupServer(t, serverConfig{
			teams: []*gh.Team{
				{
					NodeID:      strPtr("T_team1"),
					Name:        strPtr("Backend"),
					Slug:        strPtr("backend"),
					Description: strPtr("Backend team"),
					Privacy:     strPtr("closed"),
					Permission:  strPtr("push"),
				},
			},
			teamMembers: map[string][]*gh.User{
				"backend": {
					{NodeID: strPtr("U_alice"), Login: strPtr("alice")},
					{NodeID: strPtr("U_bob"), Login: strPtr("bob")},
				},
			},
		})
		defer server.Close()

		extr := initExtractor(t, server.URL, map[string]any{
			"extract": []string{"teams"},
		})

		emitter := mocks.NewEmitter()
		err := extr.Extract(context.Background(), emitter.Push)
		require.NoError(t, err)

		records := emitter.Get()
		require.Len(t, records, 1)

		entity := records[0].Entity()
		assert.Equal(t, models.NewURN("github", urnScope, "team", "T_team1"), entity.GetUrn())
		assert.Equal(t, "team", entity.GetType())
		assert.Equal(t, "Backend", entity.GetName())

		props := entity.GetProperties().AsMap()
		assert.Equal(t, "backend", props["slug"])
		assert.Equal(t, "Backend team", props["description"])

		edges := records[0].Edges()
		require.Len(t, edges, 2)
		for _, edge := range edges {
			assert.Equal(t, "member_of", edge.GetType())
			assert.Equal(t, models.NewURN("github", urnScope, "team", "T_team1"), edge.GetTargetUrn())
		}
	})

	t.Run("should extract documents with belongs_to edges", func(t *testing.T) {
		server := setupServer(t, serverConfig{
			repos: []*gh.Repository{
				{
					NodeID:   strPtr("R_repo1"),
					Name:     strPtr("meteor"),
					FullName: strPtr("my-org/meteor"),
				},
			},
			repoContents: map[string]map[string]any{
				"meteor/docs": {
					"type": "dir",
					"entries": []*gh.RepositoryContent{
						{
							Type: strPtr("file"),
							Name: strPtr("getting-started.md"),
							Path: strPtr("docs/getting-started.md"),
							SHA:  strPtr("abc123"),
							Size: intPtr(1024),
						},
						{
							Type: strPtr("file"),
							Name: strPtr("image.png"),
							Path: strPtr("docs/image.png"),
							SHA:  strPtr("img456"),
							Size: intPtr(4096),
						},
					},
				},
				"meteor/docs/getting-started.md": {
					"type": "file",
					"file": &gh.RepositoryContent{
						Type:     strPtr("file"),
						Name:     strPtr("getting-started.md"),
						Path:     strPtr("docs/getting-started.md"),
						SHA:      strPtr("abc123"),
						Size:     intPtr(1024),
						Encoding: strPtr("base64"),
						Content:  strPtr("IyBHZXR0aW5nIFN0YXJ0ZWQKClRoaXMgaXMgYSBndWlkZS4="),
						HTMLURL:  strPtr("https://github.com/my-org/meteor/blob/main/docs/getting-started.md"),
					},
				},
			},
		})
		defer server.Close()

		extr := initExtractor(t, server.URL, map[string]any{
			"extract": []string{"documents"},
			"docs": map[string]any{
				"repos":   []string{"meteor"},
				"paths":   []string{"docs"},
				"pattern": "*.md",
			},
		})

		emitter := mocks.NewEmitter()
		err := extr.Extract(context.Background(), emitter.Push)
		require.NoError(t, err)

		records := emitter.Get()
		require.Len(t, records, 1)

		entity := records[0].Entity()
		assert.Equal(t, models.NewURN("github", urnScope, "document", "abc123"), entity.GetUrn())
		assert.Equal(t, "document", entity.GetType())
		assert.Equal(t, "getting-started", entity.GetName())
		assert.Equal(t, "github", entity.GetSource())

		props := entity.GetProperties().AsMap()
		assert.Equal(t, "docs/getting-started.md", props["path"])
		assert.Equal(t, "getting-started.md", props["file_name"])
		assert.Equal(t, "# Getting Started\n\nThis is a guide.", props["content"])
		assert.Equal(t, "https://github.com/my-org/meteor/blob/main/docs/getting-started.md", props["html_url"])
		assert.Equal(t, "my-org/meteor", props["repo"])
		assert.Equal(t, "abc123", props["sha"])

		edges := records[0].Edges()
		require.Len(t, edges, 1)
		assert.Equal(t, "belongs_to", edges[0].GetType())
		assert.Equal(t, models.NewURN("github", urnScope, "document", "abc123"), edges[0].GetSourceUrn())
		assert.Equal(t, models.NewURN("github", urnScope, "repository", "R_repo1"), edges[0].GetTargetUrn())
	})

	t.Run("should recurse into subdirectories for documents", func(t *testing.T) {
		server := setupServer(t, serverConfig{
			repos: []*gh.Repository{
				{
					NodeID:   strPtr("R_repo1"),
					Name:     strPtr("meteor"),
					FullName: strPtr("my-org/meteor"),
				},
			},
			repoContents: map[string]map[string]any{
				"meteor/docs": {
					"type": "dir",
					"entries": []*gh.RepositoryContent{
						{
							Type: strPtr("dir"),
							Name: strPtr("guides"),
							Path: strPtr("docs/guides"),
						},
					},
				},
				"meteor/docs/guides": {
					"type": "dir",
					"entries": []*gh.RepositoryContent{
						{
							Type: strPtr("file"),
							Name: strPtr("setup.md"),
							Path: strPtr("docs/guides/setup.md"),
							SHA:  strPtr("def456"),
							Size: intPtr(512),
						},
					},
				},
				"meteor/docs/guides/setup.md": {
					"type": "file",
					"file": &gh.RepositoryContent{
						Type:     strPtr("file"),
						Name:     strPtr("setup.md"),
						Path:     strPtr("docs/guides/setup.md"),
						SHA:      strPtr("def456"),
						Size:     intPtr(512),
						Encoding: strPtr("base64"),
						Content:  strPtr("IyBTZXR1cA=="),
						HTMLURL:  strPtr("https://github.com/my-org/meteor/blob/main/docs/guides/setup.md"),
					},
				},
			},
		})
		defer server.Close()

		extr := initExtractor(t, server.URL, map[string]any{
			"extract": []string{"documents"},
			"docs": map[string]any{
				"repos": []string{"meteor"},
				"paths": []string{"docs"},
			},
		})

		emitter := mocks.NewEmitter()
		err := extr.Extract(context.Background(), emitter.Push)
		require.NoError(t, err)

		records := emitter.Get()
		require.Len(t, records, 1)

		entity := records[0].Entity()
		assert.Equal(t, "setup", entity.GetName())
		assert.Equal(t, "# Setup", entity.GetProperties().AsMap()["content"])
	})

	t.Run("should skip docs path that does not exist", func(t *testing.T) {
		server := setupServer(t, serverConfig{
			repos: []*gh.Repository{
				{
					NodeID:   strPtr("R_repo1"),
					Name:     strPtr("meteor"),
					FullName: strPtr("my-org/meteor"),
				},
			},
			repoContents: map[string]map[string]any{},
		})
		defer server.Close()

		extr := initExtractor(t, server.URL, map[string]any{
			"extract": []string{"documents"},
			"docs": map[string]any{
				"repos": []string{"meteor"},
				"paths": []string{"nonexistent"},
			},
		})

		emitter := mocks.NewEmitter()
		err := extr.Extract(context.Background(), emitter.Push)
		require.NoError(t, err)
		assert.Empty(t, emitter.Get())
	})

	t.Run("should extract all entity types by default", func(t *testing.T) {
		server := setupServer(t, serverConfig{
			members: []*gh.User{
				{Login: strPtr("alice"), NodeID: strPtr("U_alice")},
			},
			userDetails: map[string]*gh.User{
				"alice": {NodeID: strPtr("U_alice"), Login: strPtr("alice"), Name: strPtr("Alice")},
			},
			repos: []*gh.Repository{
				{NodeID: strPtr("R_repo1"), Name: strPtr("repo1")},
			},
			teams: []*gh.Team{
				{NodeID: strPtr("T_team1"), Name: strPtr("team1"), Slug: strPtr("team1")},
			},
			teamMembers: map[string][]*gh.User{},
		})
		defer server.Close()

		extr := initExtractor(t, server.URL, nil)

		emitter := mocks.NewEmitter()
		err := extr.Extract(context.Background(), emitter.Push)
		require.NoError(t, err)

		records := emitter.Get()
		require.Len(t, records, 3)

		types := map[string]bool{}
		for _, r := range records {
			types[r.Entity().GetType()] = true
		}
		assert.True(t, types["user"])
		assert.True(t, types["repository"])
		assert.True(t, types["team"])
	})

	t.Run("should handle pagination for users", func(t *testing.T) {
		server := setupServerWithPagination(t)
		defer server.Close()

		extr := initExtractor(t, server.URL, map[string]any{
			"extract": []string{"users"},
		})

		emitter := mocks.NewEmitter()
		err := extr.Extract(context.Background(), emitter.Push)
		require.NoError(t, err)

		entities := emitter.GetAllEntities()
		assert.Len(t, entities, 2)
	})

	t.Run("should skip users that fail to fetch details", func(t *testing.T) {
		server := setupServer(t, serverConfig{
			members: []*gh.User{
				{Login: strPtr("alice"), NodeID: strPtr("U_alice")},
				{Login: strPtr("ghost"), NodeID: strPtr("U_ghost")},
			},
			userDetails: map[string]*gh.User{
				"alice": {NodeID: strPtr("U_alice"), Login: strPtr("alice"), Name: strPtr("Alice")},
				// "ghost" is missing — will 404
			},
		})
		defer server.Close()

		extr := initExtractor(t, server.URL, map[string]any{
			"extract": []string{"users"},
		})

		emitter := mocks.NewEmitter()
		err := extr.Extract(context.Background(), emitter.Push)
		require.NoError(t, err)

		assert.Len(t, emitter.GetAllEntities(), 1)
		assert.Equal(t, "Alice", emitter.GetAllEntities()[0].GetName())
	})

	t.Run("should return error when listing members fails", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		}))
		defer server.Close()

		extr := initExtractor(t, server.URL, map[string]any{
			"extract": []string{"users"},
		})

		emitter := mocks.NewEmitter()
		err := extr.Extract(context.Background(), emitter.Push)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "extract users")
	})

	t.Run("should extract collaborators with has_access_to edges", func(t *testing.T) {
		server := setupServer(t, serverConfig{
			repos: []*gh.Repository{
				{
					NodeID:   strPtr("R_repo1"),
					Name:     strPtr("meteor"),
					FullName: strPtr("my-org/meteor"),
				},
			},
			repoCollaborators: map[string][]*gh.User{
				"meteor": {
					{NodeID: strPtr("U_alice"), Login: strPtr("alice"), Permissions: map[string]bool{"admin": true, "push": true, "pull": true}},
					{NodeID: strPtr("U_bob"), Login: strPtr("bob"), Permissions: map[string]bool{"push": true, "pull": true}},
				},
			},
		})
		defer server.Close()

		extr := initExtractor(t, server.URL, map[string]any{
			"extract": []string{"collaborators"},
		})

		emitter := mocks.NewEmitter()
		err := extr.Extract(context.Background(), emitter.Push)
		require.NoError(t, err)

		records := emitter.Get()
		require.Len(t, records, 1)

		entity := records[0].Entity()
		assert.Equal(t, models.NewURN("github", urnScope, "repository", "R_repo1"), entity.GetUrn())
		assert.Equal(t, "repository", entity.GetType())

		edges := records[0].Edges()
		require.Len(t, edges, 2)

		for _, edge := range edges {
			assert.Equal(t, "has_access_to", edge.GetType())
			assert.Equal(t, models.NewURN("github", urnScope, "repository", "R_repo1"), edge.GetTargetUrn())
		}

		// Check permission levels via edge properties.
		aliceEdge := findEdgeBySource(edges, models.NewURN("github", urnScope, "user", "U_alice"))
		require.NotNil(t, aliceEdge)
		assert.Equal(t, "admin", aliceEdge.GetProperties().AsMap()["permission"])

		bobEdge := findEdgeBySource(edges, models.NewURN("github", urnScope, "user", "U_bob"))
		require.NotNil(t, bobEdge)
		assert.Equal(t, "push", bobEdge.GetProperties().AsMap()["permission"])
	})

	t.Run("should skip collaborators for repos that fail", func(t *testing.T) {
		server := setupServer(t, serverConfig{
			repos: []*gh.Repository{
				{NodeID: strPtr("R_repo1"), Name: strPtr("meteor"), FullName: strPtr("my-org/meteor")},
			},
			// No repoCollaborators entry → will 404
		})
		defer server.Close()

		extr := initExtractor(t, server.URL, map[string]any{
			"extract": []string{"collaborators"},
		})

		emitter := mocks.NewEmitter()
		err := extr.Extract(context.Background(), emitter.Push)
		require.NoError(t, err)
		assert.Empty(t, emitter.Get())
	})

	t.Run("should extract repos without owner edge when owner is nil", func(t *testing.T) {
		server := setupServer(t, serverConfig{
			repos: []*gh.Repository{
				{NodeID: strPtr("R_no_owner"), Name: strPtr("orphan-repo")},
			},
		})
		defer server.Close()

		extr := initExtractor(t, server.URL, map[string]any{
			"extract": []string{"repositories"},
		})

		emitter := mocks.NewEmitter()
		err := extr.Extract(context.Background(), emitter.Push)
		require.NoError(t, err)

		records := emitter.Get()
		require.Len(t, records, 1)
		assert.Empty(t, records[0].Edges())
	})
}

// --- helpers ---

func initExtractor(t *testing.T, serverURL string, extraConfig map[string]any) *extractor.Extractor {
	t.Helper()
	extr := extractor.New(testutils.Logger)

	// Override the client to point to test server.
	cfg := map[string]any{
		"org":   "my-org",
		"token": "test-token",
	}
	for k, v := range extraConfig {
		cfg[k] = v
	}

	err := extr.Init(context.Background(), plugins.Config{
		URNScope:  urnScope,
		RawConfig: cfg,
	})
	require.NoError(t, err)

	// Replace client's base URL with test server.
	extr.SetBaseURL(serverURL)

	return extr
}

type serverConfig struct {
	members           []*gh.User
	userDetails       map[string]*gh.User
	repos             []*gh.Repository
	teams             []*gh.Team
	teamMembers       map[string][]*gh.User
	repoContents      map[string]map[string]any // key: "repo/path" -> {"type":"dir","entries":[]} or {"type":"file","file":*RepositoryContent}
	repoCollaborators map[string][]*gh.User      // key: repo name -> collaborators
}

func setupServer(t *testing.T, cfg serverConfig) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()

	mux.HandleFunc("/api/v3/orgs/my-org/members", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, cfg.members)
	})
	mux.HandleFunc("/api/v3/users/", func(w http.ResponseWriter, r *http.Request) {
		login := r.URL.Path[len("/api/v3/users/"):]
		if usr, ok := cfg.userDetails[login]; ok {
			writeJSON(w, usr)
		} else {
			w.WriteHeader(http.StatusNotFound)
		}
	})
	mux.HandleFunc("/api/v3/orgs/my-org/repos", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, cfg.repos)
	})
	mux.HandleFunc("/api/v3/orgs/my-org/teams", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, cfg.teams)
	})
	mux.HandleFunc("/api/v3/orgs/my-org/teams/", func(w http.ResponseWriter, r *http.Request) {
		// /api/v3/orgs/my-org/teams/{slug}/members
		path := r.URL.Path
		// Extract slug from path.
		const prefix = "/api/v3/orgs/my-org/teams/"
		rest := path[len(prefix):]
		// rest is "{slug}/members"
		slug := rest[:len(rest)-len("/members")]
		if members, ok := cfg.teamMembers[slug]; ok {
			writeJSON(w, members)
		} else {
			writeJSON(w, []*gh.User{})
		}
	})
	// Individual repo endpoint for docs.repos config, contents, and collaborators.
	mux.HandleFunc("/api/v3/repos/my-org/", func(w http.ResponseWriter, r *http.Request) {
		urlPath := r.URL.Path
		const prefix = "/api/v3/repos/my-org/"
		rest := urlPath[len(prefix):]

		// Check if this is a contents request: "{repo}/contents/{path}"
		if idx := indexOf(rest, "/contents/"); idx >= 0 {
			repoName := rest[:idx]
			contentPath := rest[idx+len("/contents/"):]
			key := repoName + "/" + contentPath
			if entry, ok := cfg.repoContents[key]; ok {
				if entry["type"] == "dir" {
					writeJSON(w, entry["entries"])
				} else {
					writeJSON(w, entry["file"])
				}
			} else {
				w.WriteHeader(http.StatusNotFound)
			}
			return
		}

		// Check if this is a collaborators request: "{repo}/collaborators"
		if strings.HasSuffix(rest, "/collaborators") {
			repoName := rest[:len(rest)-len("/collaborators")]
			if collabs, ok := cfg.repoCollaborators[repoName]; ok {
				writeJSON(w, collabs)
			} else {
				w.WriteHeader(http.StatusNotFound)
			}
			return
		}

		// Otherwise it's a repo get: "{repo}"
		repoName := rest
		for _, repo := range cfg.repos {
			if repo.GetName() == repoName {
				writeJSON(w, repo)
				return
			}
		}
		w.WriteHeader(http.StatusNotFound)
	})

	return httptest.NewServer(mux)
}

func setupServerWithPagination(t *testing.T) *httptest.Server {
	t.Helper()
	callCount := 0
	mux := http.NewServeMux()

	mux.HandleFunc("/api/v3/orgs/my-org/members", func(w http.ResponseWriter, r *http.Request) {
		callCount++
		if callCount == 1 {
			// Page 1: return one user and link to page 2.
			w.Header().Set("Link", `<`+r.URL.Query().Get("_server")+`/api/v3/orgs/my-org/members?page=2>; rel="next"`)
			writeJSON(w, []*gh.User{{Login: strPtr("alice"), NodeID: strPtr("U_alice")}})
		} else {
			// Page 2: return second user, no next link.
			writeJSON(w, []*gh.User{{Login: strPtr("bob"), NodeID: strPtr("U_bob")}})
		}
	})
	mux.HandleFunc("/api/v3/users/", func(w http.ResponseWriter, r *http.Request) {
		login := r.URL.Path[len("/api/v3/users/"):]
		writeJSON(w, &gh.User{
			NodeID: strPtr("U_" + login),
			Login:  strPtr(login),
			Name:   strPtr(login),
		})
	})

	server := httptest.NewServer(mux)

	// Wrap to fix Link header URLs.
	wrapped := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Proxy to actual mux, but the Link header needs the real server URL.
		// We'll just use the mux directly with proper Link headers.
		mux.ServeHTTP(w, r)
	}))

	// Close the unused server.
	server.Close()

	return wrapped
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v) //nolint:errcheck
}

func strPtr(s string) *string { return &s }
func boolPtr(b bool) *bool    { return &b }
func intPtr(i int) *int       { return &i }

func indexOf(s, substr string) int { return strings.Index(s, substr) }

func findEdgeBySource(edges []*meteorv1beta1.Edge, sourceURN string) *meteorv1beta1.Edge {
	for _, e := range edges {
		if e.GetSourceUrn() == sourceURN {
			return e
		}
	}
	return nil
}

