package tableau

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/goto/meteor/plugins"
)

const projectPageSize = 100

var graphQLQueryTemplate = `
		{ 
			workbooks(filter: { projectName: "%s" }) {
                id
		    	name
				uri
		    	projectName
				description
		    	createdAt
		    	updatedAt
				owner {
					id
					name
					email
				}
		    	sheets {
			 		id
			 		name
			 		createdAt
			 		updatedAt
		    	}
		    	upstreamTables {
			    	name
			    	schema
					fullName
			    	database {
					    id
					    name
					    connectionType
				    	description
				    	... on DatabaseServer {
					    	hostName
					    	port
					    	service
				     	}
				     	... on CloudFile {
					 		provider
					 	    fileExtension
					 	    mimeType
					 	    requestUrl
				     	}
				     	... on File {
					 	    filePath
				     	}
				     	... on WebDataConnector {
					 	    connectorUrl
				     	}
			    	}
		    	}
	    	}
    	}
`

type projects struct {
	Projects []*Project `json:"project"`
}
type responseProject struct {
	Pagination Pagination `json:"pagination"`
	Projects   projects   `json:"projects"`
}

type siteInfo struct {
	ID         string `json:"id"`
	ContentURL string `json:"contentUrl"`
}

type userInfo struct {
	ID string `json:"id"`
}

type credentials struct {
	Site  siteInfo `json:"site"`
	User  userInfo `json:"user"`
	Token string   `json:"token"`
}

type responseSignIn struct {
	Credentials credentials `json:"credentials"`
}

type dataGraphQL struct {
	Workbooks []*Workbook `json:"workbooks"`
}

type responseGraphQL struct {
	Data dataGraphQL `json:"data"`
}

type Client interface {
	Init(ctx context.Context, cfg Config) (err error)
	GetAllProjects(ctx context.Context) (ps []*Project, err error)
	GetDetailedWorkbooksByProjectName(ctx context.Context, projectName string) (wbs []*Workbook, err error)
	makeRequest(ctx context.Context, method, url string, payload, result interface{}) (err error)
}

type client struct {
	config     Config
	authToken  string
	siteID     string
	httpClient *http.Client
}

func NewClient(httpClient *http.Client) Client {
	c := &client{
		httpClient: httpClient,
	}

	if c.httpClient == nil {
		c.httpClient = &http.Client{}
	}

	c.httpClient.Timeout = 30 * time.Second

	return c
}

func (c *client) Init(ctx context.Context, cfg Config) (err error) {
	c.config = cfg
	if c.httpClient == nil {
		c.httpClient = &http.Client{}
	}
	if c.config.AuthToken != "" && c.config.SiteID != "" {
		c.authToken = c.config.AuthToken
		c.siteID = c.config.SiteID
		return nil
	}
	c.authToken, c.siteID, err = c.getAuthToken(ctx)
	if err != nil {
		return fmt.Errorf("fetch auth token: %w", err)
	}
	return nil
}

func (c *client) getProjectsWithPagination(ctx context.Context, pageNum, pageSize int) (ps []*Project, totalItem int, err error) {
	var response responseProject
	projectPath := fmt.Sprintf("sites/%s/projects?pageSize=%d&pageNumber=%d", c.siteID, pageSize, pageNum)
	projectURL := c.buildURL(projectPath)
	if err := c.makeRequest(ctx, http.MethodGet, projectURL, nil, &response); err != nil {
		return nil, 0, err
	}

	totalItem, err = strconv.Atoi(response.Pagination.TotalAvailable)
	if err != nil {
		return nil, 0, fmt.Errorf("convert total available projects to int: %w", err)
	}

	return response.Projects.Projects, totalItem, nil
}

func (c *client) GetAllProjects(ctx context.Context) ([]*Project, error) {
	pageNum := 1
	var ps []*Project
	for {
		partialProjects, totalItem, err := c.getProjectsWithPagination(ctx, pageNum, projectPageSize)
		if err != nil {
			return nil, fmt.Errorf("get projects with pagination: %w", err)
		}
		ps = append(ps, partialProjects...)
		pageNum++

		if pageNum*projectPageSize >= totalItem {
			break
		}
	}
	return ps, nil
}

func (c *client) GetDetailedWorkbooksByProjectName(ctx context.Context, projectName string) ([]*Workbook, error) {
	var response responseGraphQL
	url := fmt.Sprintf("%s/api/metadata/graphql", c.config.Host)
	graphQLBody := getGraphQLQueryWorkbooksByProjectName(projectName)
	if err := c.makeRequest(ctx, http.MethodPost, url, graphQLBody, &response); err != nil {
		return nil, err
	}
	return response.Data.Workbooks, nil
}

func (c *client) getAuthToken(ctx context.Context) (authToken, siteID string, err error) {
	payload := map[string]interface{}{
		"credentials": map[string]interface{}{
			"name":     c.config.Username,
			"password": c.config.Password,
			"site": map[string]interface{}{
				"contentUrl": c.config.Sitename,
			},
		},
	}

	var data responseSignIn
	signInURL := c.buildURL("auth/signin")
	if err := c.makeRequest(ctx, http.MethodPost, signInURL, payload, &data); err != nil {
		return "", "", err
	}

	return data.Credentials.Token, data.Credentials.Site.ID, nil
}

func (c *client) buildURL(path string) string {
	return fmt.Sprintf("%s/api/%s/%s", c.config.Host, c.config.Version, path)
}

// helper function to avoid rewriting a request
func (c *client) makeRequest(ctx context.Context, method, url string, payload, result interface{}) error {
	jsonBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("encode the payload JSON: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewBuffer(jsonBytes))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("X-Tableau-Auth", c.authToken)

	res, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("generate response: %w", err)
	}
	defer plugins.DrainBody(res)

	if res.StatusCode >= 300 {
		return fmt.Errorf("response status code %d", res.StatusCode)
	}

	data, err := io.ReadAll(res.Body)
	if err != nil {
		return fmt.Errorf("read response body: %w", err)
	}

	if err := json.Unmarshal(data, &result); err != nil {
		return fmt.Errorf("parse: %s: %w", string(data), err)
	}

	return nil
}

func getGraphQLQueryWorkbooksByProjectName(projectName string) map[string]string {
	gqlQuery := fmt.Sprintf(graphQLQueryTemplate, projectName)
	return map[string]string{
		"query": gqlQuery,
	}
}
