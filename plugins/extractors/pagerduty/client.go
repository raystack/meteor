package pagerduty

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

const baseURL = "https://api.pagerduty.com"

// Service represents a PagerDuty service.
type Service struct {
	ID                  string `json:"id"`
	Name                string `json:"name"`
	Description         string `json:"description"`
	Status              string `json:"status"`
	CreatedAt           string `json:"created_at"`
	UpdatedAt           string `json:"updated_at"`
	HTMLURL             string `json:"html_url"`
	AlertCreation       string `json:"alert_creation"`
	EscalationPolicy    struct {
		ID string `json:"id"`
	} `json:"escalation_policy"`
	Teams []struct {
		ID      string `json:"id"`
		Summary string `json:"summary"`
	} `json:"teams"`
	IncidentUrgencyRule struct {
		Type string `json:"type"`
	} `json:"incident_urgency_rule"`
}

// Incident represents a PagerDuty incident.
type Incident struct {
	ID             string `json:"id"`
	IncidentNumber int    `json:"incident_number"`
	Title          string `json:"title"`
	Status         string `json:"status"`
	Urgency        string `json:"urgency"`
	CreatedAt      string `json:"created_at"`
	ResolvedAt     string `json:"resolved_at"`
	HTMLURL        string `json:"html_url"`
	Service        struct {
		ID string `json:"id"`
	} `json:"service"`
	Priority *struct {
		Summary string `json:"summary"`
	} `json:"priority"`
}

type listServicesResponse struct {
	Services []Service `json:"services"`
	Limit    int       `json:"limit"`
	Offset   int       `json:"offset"`
	More     bool      `json:"more"`
}

type listIncidentsResponse struct {
	Incidents []Incident `json:"incidents"`
	Limit     int        `json:"limit"`
	Offset    int        `json:"offset"`
	More      bool       `json:"more"`
}

// Client wraps the PagerDuty REST API v2.
type Client struct {
	httpClient *http.Client
	apiKey     string
}

// NewClient creates a new PagerDuty API client.
func NewClient(apiKey string) *Client {
	return &Client{
		httpClient: &http.Client{Timeout: 30 * time.Second},
		apiKey:     apiKey,
	}
}

// ListServices returns all PagerDuty services using offset-based pagination.
func (c *Client) ListServices(ctx context.Context) ([]Service, error) {
	var all []Service
	offset := 0
	for {
		params := url.Values{}
		params.Set("limit", "25")
		params.Set("offset", fmt.Sprintf("%d", offset))

		var resp listServicesResponse
		if err := c.get(ctx, "/services", params, &resp); err != nil {
			return nil, fmt.Errorf("list services: %w", err)
		}
		all = append(all, resp.Services...)

		if !resp.More {
			break
		}
		offset += resp.Limit
	}
	return all, nil
}

// ListIncidents returns PagerDuty incidents within the given time range
// using offset-based pagination.
func (c *Client) ListIncidents(ctx context.Context, since, until time.Time) ([]Incident, error) {
	var all []Incident
	offset := 0
	for {
		params := url.Values{}
		params.Set("limit", "25")
		params.Set("offset", fmt.Sprintf("%d", offset))
		params.Set("since", since.UTC().Format(time.RFC3339))
		params.Set("until", until.UTC().Format(time.RFC3339))

		var resp listIncidentsResponse
		if err := c.get(ctx, "/incidents", params, &resp); err != nil {
			return nil, fmt.Errorf("list incidents: %w", err)
		}
		all = append(all, resp.Incidents...)

		if !resp.More {
			break
		}
		offset += resp.Limit
	}
	return all, nil
}

func (c *Client) get(ctx context.Context, path string, params url.Values, out any) error {
	u := baseURL + path
	if len(params) > 0 {
		u += "?" + params.Encode()
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Authorization", "Token token="+c.apiKey)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")

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
