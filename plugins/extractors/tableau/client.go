package tableau

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"

	"github.com/pkg/errors"
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
	makeRequest(method, url string, payload interface{}, data interface{}) (err error)
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
	c.authToken, c.siteID, err = c.getAuthToken()
	if err != nil {
		return errors.Wrap(err, "failed to fetch auth token")
	}
	return nil
}

func (c *client) getProjectsWithPagination(ctx context.Context, pageNum int, pageSize int) (ps []*Project, totalItem int, err error) {
	var response responseProject
	projectPath := fmt.Sprintf("sites/%s/projects?pageSize=%d&pageNumber=%d", c.siteID, pageSize, pageNum)
	projectURL := c.buildURL(projectPath)
	err = c.makeRequest(http.MethodGet, projectURL, nil, &response)
	if err != nil {
		return
	}
	totalItem, err = strconv.Atoi(response.Pagination.TotalAvailable)
	if err != nil {
		err = errors.Wrap(err, "cannot convert total available items in get projects pagination to int")
		return
	}
	ps = response.Projects.Projects
	return
}

func (c *client) GetAllProjects(ctx context.Context) (ps []*Project, err error) {
	var pageNum = 1
	for {
		partialProjects, totalItem, errGet := c.getProjectsWithPagination(ctx, pageNum, projectPageSize)
		if err != nil {
			err = errors.Wrap(errGet, "error when get projects with pagination")
			break
		}
		ps = append(ps, partialProjects...)
		pageNum++

		if pageNum*projectPageSize >= totalItem {
			break
		}
	}
	return
}

func (c *client) GetDetailedWorkbooksByProjectName(ctx context.Context, projectName string) (wbs []*Workbook, err error) {
	var response responseGraphQL
	url := fmt.Sprintf("%s/api/metadata/graphql", c.config.Host)
	graphQLBody := getGraphQLQueryWorkbooksByProjectName(projectName)
	err = c.makeRequest(http.MethodPost, url, graphQLBody, &response)
	if err != nil {
		return
	}
	wbs = response.Data.Workbooks
	return
}

func (c *client) getAuthToken() (authToken string, siteID string, err error) {
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
	err = c.makeRequest(http.MethodPost, signInURL, payload, &data)
	if err != nil {
		return
	}
	return data.Credentials.Token, data.Credentials.Site.ID, nil
}

func (c *client) buildURL(path string) string {
	return fmt.Sprintf("%s/api/%s/%s", c.config.Host, c.config.Version, path)
}

// helper function to avoid rewriting a request
func (c *client) makeRequest(method, url string, payload interface{}, data interface{}) (err error) {
	jsonBytes, err := json.Marshal(payload)
	if err != nil {
		return errors.Wrap(err, "failed to encode the payload JSON")
	}

	req, err := http.NewRequest(method, url, bytes.NewBuffer(jsonBytes))
	if err != nil {
		return errors.Wrap(err, "failed to create request")
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("X-Tableau-Auth", c.authToken)

	res, err := c.httpClient.Do(req)
	if err != nil {
		return errors.Wrap(err, "failed to generate response")
	}
	if res.StatusCode >= 300 {
		return fmt.Errorf("getting %d status code", res.StatusCode)
	}

	bytes, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return errors.Wrap(err, "failed to read response body")
	}

	if err = json.Unmarshal(bytes, &data); err != nil {
		return errors.Wrapf(err, "failed to parse: %s", string(bytes))
	}
	return
}

func getGraphQLQueryWorkbooksByProjectName(projectName string) map[string]string {
	gqlQuery := fmt.Sprintf(graphQLQueryTemplate, projectName)
	return map[string]string{
		"query": gqlQuery,
	}
}
