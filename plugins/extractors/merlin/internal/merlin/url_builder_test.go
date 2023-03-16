//go:build plugins
// +build plugins

package merlin_test

import (
	"net/url"
	"testing"

	"github.com/goto/meteor/plugins/extractors/merlin/internal/merlin"
	"github.com/stretchr/testify/assert"
)

func TestNewURLBuilderSource(t *testing.T) {
	cases := []struct {
		name        string
		baseURL     string
		expected    *url.URL
		expectedErr string
	}{
		{
			name:    "Simple",
			baseURL: "https://api.example.com",
			expected: &url.URL{
				Scheme: "https",
				Host:   "api.example.com",
			},
		},
		{
			name:    "WithQueryParams",
			baseURL: "http://example.com?limit=10&offset=120",
			expected: &url.URL{
				Scheme:   "http",
				Host:     "example.com",
				RawQuery: "limit=10&offset=120",
			},
		},
		{
			name:    "WithoutScheme",
			baseURL: "api.example.com",
			expected: &url.URL{
				Scheme: "http",
				Host:   "api.example.com",
			},
		},
		{
			name:        "InvalidBaseURL",
			baseURL:     ":foo",
			expectedErr: `invalid input: parse ":foo": missing protocol scheme`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b, err := merlin.NewURLBuilderSource(tc.baseURL)
			if tc.expectedErr != "" {
				assert.ErrorContains(t, err, tc.expectedErr)
				return
			}

			assert.NoError(t, err)
			assert.Equal(t, tc.expected, b.New().URL())
		})
	}
}

func TestURLBuilder(t *testing.T) {
	b, err := merlin.NewURLBuilderSource("https://api.example.com/v1/?limit=10&mode=light")
	if err != nil {
		t.Fatalf("NewURLBuilderSource(): %s", err)
	}

	cases := []struct {
		name     string
		url      *url.URL
		expected *url.URL
	}{
		{
			name: "Simple",
			url:  b.New().Path("users").URL(),
			expected: &url.URL{
				Scheme:   "https",
				Host:     "api.example.com",
				Path:     "/v1/users",
				RawQuery: "limit=10&mode=light",
			},
		},
		{
			name: "PathParams",
			url: b.New().
				Path("/users/{userID}/posts/{postID}/comments").
				PathParam("userID", "foo").
				PathParamInt("postID", 42).
				URL(),
			expected: &url.URL{
				Scheme:   "https",
				Host:     "api.example.com",
				Path:     "/v1/users/foo/posts/42/comments",
				RawQuery: "limit=10&mode=light",
			},
		},
		{
			name: "DoublePathParam",
			url: b.New().
				Path("/path/{p1}/{p2}/{p1}").
				PathParam("p1", "a").
				PathParam("p2", "b").
				URL(),
			expected: &url.URL{
				Scheme:   "https",
				Host:     "api.example.com",
				Path:     "/v1/path/a/b/a",
				RawQuery: "limit=10&mode=light",
			},
		},
		{
			name: "EscapePathParam",
			url: b.New().
				Path("/posts/{title}").
				PathParam("title", `Letters & "Special" Characters`).
				URL(),
			expected: &url.URL{
				Scheme:   "https",
				Host:     "api.example.com",
				Path:     "/v1/posts/Letters%20&%20%22Special%22%20Characters",
				RawQuery: "limit=10&mode=light",
			},
		},
		{
			name: "PathParamsMap",
			url: b.New().
				Path("{app}/users/{userID}/posts/{postID}/comments").
				PathParams(map[string]string{
					"app":    "myapp",
					"userID": "1",
					"postID": "42",
				}).
				URL(),
			expected: &url.URL{
				Scheme:   "https",
				Host:     "api.example.com",
				Path:     "/v1/myapp/users/1/posts/42/comments",
				RawQuery: "limit=10&mode=light",
			},
		},
		{
			name: "QueryParams",
			url: b.New().
				Path("/search").
				QueryParam("author_id", "foo", "bar").
				QueryParamInt("limit", 20).
				QueryParamInt("ints", 1, 3, 5, 7).
				QueryParamBool("recent", true).
				QueryParamFloat("min_rating", 4.5).
				QueryParamFloat("floats", 0, -2, 4.6735593624473).
				URL(),
			expected: &url.URL{
				Scheme:   "https",
				Host:     "api.example.com",
				Path:     "/v1/search",
				RawQuery: "author_id=foo&author_id=bar&floats=0&floats=-2&floats=4.6735593624473&ints=1&ints=3&ints=5&ints=7&limit=20&min_rating=4.5&mode=light&recent=true",
			},
		},
		{
			name: "EscapeQueryParam",
			url: b.New().
				Path("/search").
				QueryParam("text", "foo bar/®").
				URL(),
			expected: &url.URL{
				Scheme:   "https",
				Host:     "api.example.com",
				Path:     "/v1/search",
				RawQuery: "limit=10&mode=light&text=foo+bar%2F%C2%AE",
			},
		},
		{
			name: "QueryParamValues",
			url: b.New().
				QueryParams(url.Values{
					"mode":   {"dark"},
					"offset": {"20"},
				}).
				URL(),
			expected: &url.URL{
				Scheme:   "https",
				Host:     "api.example.com",
				Path:     "/v1",
				RawQuery: "limit=10&mode=dark&offset=20",
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, tc.url)
		})
	}
}
