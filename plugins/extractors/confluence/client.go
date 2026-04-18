package confluence

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Page represents a Confluence page from the v2 API.
type Page struct {
	ID        string    `json:"id"`
	Title     string    `json:"title"`
	Status    string    `json:"status"`
	SpaceID   string    `json:"spaceId"`
	ParentID  string    `json:"parentId"`
	AuthorID  string    `json:"authorId"`
	CreatedAt time.Time `json:"createdAt"`
	Version   struct {
		Number    int       `json:"number"`
		AuthorID  string    `json:"authorId"`
		CreatedAt time.Time `json:"createdAt"`
	} `json:"version"`
	Body struct {
		Storage struct {
			Value string `json:"value"`
		} `json:"storage"`
	} `json:"body"`
	Labels struct {
		Results []Label `json:"results"`
	} `json:"labels"`
	Links struct {
		WebUI string `json:"webui"`
	} `json:"_links"`
}

// Space represents a Confluence space.
type Space struct {
	ID          string `json:"id"`
	Key         string `json:"key"`
	Name        string `json:"name"`
	Type        string `json:"type"`
	Status      string `json:"status"`
	Description struct {
		Plain struct {
			Value string `json:"value"`
		} `json:"plain"`
	} `json:"description"`
	Links struct {
		WebUI string `json:"webui"`
	} `json:"_links"`
}

// Label represents a Confluence label.
type Label struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type pageResponse struct {
	Results []Page `json:"results"`
	Links   struct {
		Next string `json:"next"`
	} `json:"_links"`
}

type spaceResponse struct {
	Results []Space `json:"results"`
	Links   struct {
		Next string `json:"next"`
	} `json:"_links"`
}

// Client wraps the Confluence REST API v2.
type Client struct {
	baseURL    string
	httpClient *http.Client
	username   string
	token      string
}

// NewClient creates a new Confluence API client.
func NewClient(baseURL, username, token string) *Client {
	return &Client{
		baseURL:    strings.TrimRight(baseURL, "/"),
		httpClient: &http.Client{Timeout: 30 * time.Second},
		username:   username,
		token:      token,
	}
}

// GetSpaces returns all spaces, optionally filtered by keys.
func (c *Client) GetSpaces(ctx context.Context, keys []string) ([]Space, error) {
	var all []Space
	cursor := ""
	for {
		params := url.Values{}
		params.Set("limit", "25")
		if len(keys) > 0 {
			params.Set("keys", strings.Join(keys, ","))
		}
		if cursor != "" {
			params.Set("cursor", cursor)
		}

		var resp spaceResponse
		if err := c.get(ctx, "/api/v2/spaces", params, &resp); err != nil {
			return nil, fmt.Errorf("get spaces: %w", err)
		}
		all = append(all, resp.Results...)

		cursor = parseCursor(resp.Links.Next)
		if cursor == "" {
			break
		}
	}
	return all, nil
}

// GetPages returns all pages in a space.
func (c *Client) GetPages(ctx context.Context, spaceID string) ([]Page, error) {
	var all []Page
	cursor := ""
	for {
		params := url.Values{}
		params.Set("space-id", spaceID)
		params.Set("limit", "25")
		params.Set("body-format", "storage")
		if cursor != "" {
			params.Set("cursor", cursor)
		}

		var resp pageResponse
		if err := c.get(ctx, "/api/v2/pages", params, &resp); err != nil {
			return nil, fmt.Errorf("get pages for space %s: %w", spaceID, err)
		}
		all = append(all, resp.Results...)

		cursor = parseCursor(resp.Links.Next)
		if cursor == "" {
			break
		}
	}
	return all, nil
}

// GetPageLabels returns all labels for a page, handling pagination.
func (c *Client) GetPageLabels(ctx context.Context, pageID string) ([]Label, error) {
	var all []Label
	cursor := ""
	for {
		params := url.Values{}
		params.Set("limit", "25")
		if cursor != "" {
			params.Set("cursor", cursor)
		}

		var resp struct {
			Results []Label `json:"results"`
			Links   struct {
				Next string `json:"next"`
			} `json:"_links"`
		}
		if err := c.get(ctx, "/api/v2/pages/"+pageID+"/labels", params, &resp); err != nil {
			return nil, fmt.Errorf("get labels for page %s: %w", pageID, err)
		}
		all = append(all, resp.Results...)

		cursor = parseCursor(resp.Links.Next)
		if cursor == "" {
			break
		}
	}
	return all, nil
}

func (c *Client) get(ctx context.Context, path string, params url.Values, out any) error {
	u := c.baseURL + path
	if len(params) > 0 {
		u += "?" + params.Encode()
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.SetBasicAuth(c.username, c.token)
	req.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("unexpected status %d: %s", resp.StatusCode, truncate(string(body), 200))
	}

	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	return nil
}

// parseCursor extracts the cursor parameter from a next-link URL.
func parseCursor(nextLink string) string {
	if nextLink == "" {
		return ""
	}
	u, err := url.Parse(nextLink)
	if err != nil {
		return ""
	}
	return u.Query().Get("cursor")
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
