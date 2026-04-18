package notion

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

const (
	defaultBaseURL    = "https://api.notion.com"
	notionAPIVersion  = "2022-06-28"
	defaultPageSize   = 100
)

// Page represents a Notion page.
type Page struct {
	ID             string         `json:"id"`
	CreatedTime    time.Time      `json:"created_time"`
	LastEditedTime time.Time      `json:"last_edited_time"`
	CreatedBy      User           `json:"created_by"`
	LastEditedBy   User           `json:"last_edited_by"`
	Archived       bool           `json:"archived"`
	URL            string         `json:"url"`
	Parent         Parent         `json:"parent"`
	Properties     map[string]any `json:"properties"`
}

// Database represents a Notion database.
type Database struct {
	ID             string         `json:"id"`
	CreatedTime    time.Time      `json:"created_time"`
	LastEditedTime time.Time      `json:"last_edited_time"`
	CreatedBy      User           `json:"created_by"`
	LastEditedBy   User           `json:"last_edited_by"`
	Title          []RichText     `json:"title"`
	Description    []RichText     `json:"description"`
	Archived       bool           `json:"archived"`
	URL            string         `json:"url"`
	Parent         Parent         `json:"parent"`
	Properties     map[string]any `json:"properties"`
}

// User represents a Notion user.
type User struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

// Parent represents the parent of a page or database.
type Parent struct {
	Type       string `json:"type"`
	PageID     string `json:"page_id,omitempty"`
	DatabaseID string `json:"database_id,omitempty"`
	WorkspaceID string `json:"workspace,omitempty"`
}

// RichText represents a Notion rich text object.
type RichText struct {
	PlainText string `json:"plain_text"`
}

// searchResponse is the response from the Notion search API.
type searchResponse struct {
	Results    []json.RawMessage `json:"results"`
	HasMore    bool              `json:"has_more"`
	NextCursor string            `json:"next_cursor"`
}

// blockChildrenResponse is the response from the block children API.
type blockChildrenResponse struct {
	Results    []Block `json:"results"`
	HasMore    bool    `json:"has_more"`
	NextCursor string  `json:"next_cursor"`
}

// Block represents a Notion block (used for reading page content).
type Block struct {
	ID   string `json:"id"`
	Type string `json:"type"`
	// We flatten all block types into a generic map for URN scanning.
	Paragraph      *blockContent `json:"paragraph,omitempty"`
	Heading1       *blockContent `json:"heading_1,omitempty"`
	Heading2       *blockContent `json:"heading_2,omitempty"`
	Heading3       *blockContent `json:"heading_3,omitempty"`
	BulletedList   *blockContent `json:"bulleted_list_item,omitempty"`
	NumberedList   *blockContent `json:"numbered_list_item,omitempty"`
	Quote          *blockContent `json:"quote,omitempty"`
	Callout        *blockContent `json:"callout,omitempty"`
	Code           *blockContent `json:"code,omitempty"`
}

type blockContent struct {
	RichText []RichText `json:"rich_text"`
}

// PlainText extracts all plain text from a block.
func (b *Block) PlainText() string {
	for _, content := range []*blockContent{
		b.Paragraph, b.Heading1, b.Heading2, b.Heading3,
		b.BulletedList, b.NumberedList, b.Quote, b.Callout, b.Code,
	} {
		if content == nil {
			continue
		}
		var text string
		for _, rt := range content.RichText {
			text += rt.PlainText
		}
		if text != "" {
			return text
		}
	}
	return ""
}

// Client wraps the Notion API.
type Client struct {
	baseURL    string
	httpClient *http.Client
	token      string
}

// NewClient creates a new Notion API client.
func NewClient(token string) *Client {
	return &Client{
		baseURL:    defaultBaseURL,
		httpClient: &http.Client{Timeout: 30 * time.Second},
		token:      token,
	}
}

// SetBaseURL overrides the API base URL (used for testing).
func (c *Client) SetBaseURL(url string) {
	c.baseURL = url
}

// SearchPages returns all pages, optionally filtered by query.
func (c *Client) SearchPages(ctx context.Context) ([]Page, error) {
	var all []Page
	cursor := ""
	for {
		body := map[string]any{
			"filter":    map[string]any{"value": "page", "property": "object"},
			"page_size": defaultPageSize,
		}
		if cursor != "" {
			body["start_cursor"] = cursor
		}

		var resp searchResponse
		if err := c.post(ctx, "/v1/search", body, &resp); err != nil {
			return nil, fmt.Errorf("search pages: %w", err)
		}

		for _, raw := range resp.Results {
			var page Page
			if err := json.Unmarshal(raw, &page); err != nil {
				return nil, fmt.Errorf("unmarshal page: %w", err)
			}
			all = append(all, page)
		}

		if !resp.HasMore || resp.NextCursor == "" {
			break
		}
		cursor = resp.NextCursor
	}
	return all, nil
}

// SearchDatabases returns all databases.
func (c *Client) SearchDatabases(ctx context.Context) ([]Database, error) {
	var all []Database
	cursor := ""
	for {
		body := map[string]any{
			"filter":    map[string]any{"value": "database", "property": "object"},
			"page_size": defaultPageSize,
		}
		if cursor != "" {
			body["start_cursor"] = cursor
		}

		var resp searchResponse
		if err := c.post(ctx, "/v1/search", body, &resp); err != nil {
			return nil, fmt.Errorf("search databases: %w", err)
		}

		for _, raw := range resp.Results {
			var db Database
			if err := json.Unmarshal(raw, &db); err != nil {
				return nil, fmt.Errorf("unmarshal database: %w", err)
			}
			all = append(all, db)
		}

		if !resp.HasMore || resp.NextCursor == "" {
			break
		}
		cursor = resp.NextCursor
	}
	return all, nil
}

// GetBlockChildren returns the top-level blocks of a page or block.
func (c *Client) GetBlockChildren(ctx context.Context, blockID string) ([]Block, error) {
	var all []Block
	cursor := ""
	for {
		path := fmt.Sprintf("/v1/blocks/%s/children?page_size=%d", blockID, defaultPageSize)
		if cursor != "" {
			path += "&start_cursor=" + cursor
		}

		var resp blockChildrenResponse
		if err := c.get(ctx, path, &resp); err != nil {
			return nil, fmt.Errorf("get block children for %s: %w", blockID, err)
		}
		all = append(all, resp.Results...)

		if !resp.HasMore || resp.NextCursor == "" {
			break
		}
		cursor = resp.NextCursor
	}
	return all, nil
}

func (c *Client) post(ctx context.Context, path string, body any, out any) error {
	jsonBody, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, bytes.NewReader(jsonBody))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	c.setHeaders(req)
	req.Header.Set("Content-Type", "application/json")

	return c.do(req, out)
}

func (c *Client) get(ctx context.Context, path string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, nil)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	c.setHeaders(req)

	return c.do(req, out)
}

func (c *Client) setHeaders(req *http.Request) {
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Notion-Version", notionAPIVersion)
	req.Header.Set("Accept", "application/json")
}

func (c *Client) do(req *http.Request, out any) error {
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

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
