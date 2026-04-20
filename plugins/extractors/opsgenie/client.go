package opsgenie

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

// Service represents an OpsGenie service.
type Service struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description"`
	TeamID      string `json:"teamId"`
}

// Incident represents an OpsGenie incident.
type Incident struct {
	ID               string   `json:"id"`
	Message          string   `json:"message"`
	Status           string   `json:"status"`
	Priority         string   `json:"priority"`
	CreatedAt        string   `json:"createdAt"`
	ResolvedAt       string   `json:"resolvedAt"`
	Tags             []string `json:"tags"`
	ImpactedServices []string `json:"impactedServices"`
	OwnerTeam        string   `json:"ownerTeam"`
}

type serviceResponse struct {
	Data       []Service `json:"data"`
	TotalCount int       `json:"totalCount"`
}

type incidentResponse struct {
	Data       []Incident `json:"data"`
	TotalCount int        `json:"totalCount"`
}

// Client wraps the OpsGenie REST API v1.
type Client struct {
	baseURL    string
	apiKey     string
	httpClient *http.Client
}

// NewClient creates a new OpsGenie API client.
func NewClient(baseURL, apiKey string) *Client {
	return &Client{
		baseURL:    strings.TrimRight(baseURL, "/"),
		apiKey:     apiKey,
		httpClient: &http.Client{Timeout: 30 * time.Second},
	}
}

const pageLimit = 100

// ListServices returns all OpsGenie services using offset pagination.
func (c *Client) ListServices(ctx context.Context) ([]Service, error) {
	var all []Service
	offset := 0
	for {
		params := url.Values{}
		params.Set("limit", fmt.Sprintf("%d", pageLimit))
		params.Set("offset", fmt.Sprintf("%d", offset))

		var resp serviceResponse
		if err := c.get(ctx, "/v1/services", params, &resp); err != nil {
			return nil, fmt.Errorf("list services: %w", err)
		}
		all = append(all, resp.Data...)

		if len(all) >= resp.TotalCount {
			break
		}
		offset += pageLimit
	}
	return all, nil
}

// ListIncidents returns all OpsGenie incidents using offset pagination.
func (c *Client) ListIncidents(ctx context.Context) ([]Incident, error) {
	var all []Incident
	offset := 0
	for {
		params := url.Values{}
		params.Set("limit", fmt.Sprintf("%d", pageLimit))
		params.Set("offset", fmt.Sprintf("%d", offset))
		params.Set("order", "desc")
		params.Set("sort", "createdAt")

		var resp incidentResponse
		if err := c.get(ctx, "/v1/incidents", params, &resp); err != nil {
			return nil, fmt.Errorf("list incidents: %w", err)
		}
		all = append(all, resp.Data...)

		if len(all) >= resp.TotalCount {
			break
		}
		offset += pageLimit
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
	req.Header.Set("Authorization", "GenieKey "+c.apiKey)
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

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
