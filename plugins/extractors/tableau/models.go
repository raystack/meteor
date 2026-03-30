package tableau

import (
	"fmt"
	"github.com/raystack/meteor/plugins"
	"regexp"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/raystack/meteor/models"
)

const (
	bigquery                = "bigquery"
	mssql                   = "mssql"
	maxcompute              = "maxcompute"
	maxcomputeDefaultSchema = "default"
)

// https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_concepts_connectiontype.htm
var connectionTypeMap = map[string]string{
	"sqlserver":       mssql,
	"maxcompute_jdbc": maxcompute,
}

func mapConnectionTypeToSource(ct string) string {
	s, ok := connectionTypeMap[ct]
	if !ok {
		return ct
	}
	return s
}

type Pagination struct {
	PageNumber     string `json:"pageNumber"`
	PageSize       string `json:"pageSize"`
	TotalAvailable string `json:"totalAvailable"`
}

type Project struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Description string    `json:"description"`
	CreatedAt   time.Time `json:"createdAt"`
	UpdatedAt   time.Time `json:"updatedAt"`
}

type Table struct {
	ID       string   `json:"id"`
	Name     string   `json:"name"`
	Schema   string   `json:"schema"`
	FullName string   `json:"fullName"`
	Database Database `json:"database"`
}

type Owner struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Email string `json:"email"`
}

type Workbook struct {
	ID             string    `json:"id"`
	Name           string    `json:"name"`
	ProjectName    string    `json:"projectName"`
	URI            string    `json:"uri"`
	Description    string    `json:"description"`
	Owner          Owner     `json:"owner"`
	Sheets         []*Sheet  `json:"sheets"`
	UpstreamTables []*Table  `json:"upstreamTables"`
	CreatedAt      time.Time `json:"createdAt"`
	UpdatedAt      time.Time `json:"updatedAt"`
}

type Sheet struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
}

// https://help.tableau.com/current/api/metadata_api/en-us/docs/meta_api_model.html
type DatabaseInterface interface {
	CreateResourceURN(tableInfo Table) string
}

type Database map[string]any

type DatabaseServer struct {
	ID             string `json:"id"`
	Name           string `json:"name"`
	ConnectionType string `json:"connectionType"`
	Description    string `json:"description"`
	HostName       string `json:"hostName"`
	Port           int    `json:"port"`
	Service        string `json:"service"`
}

// parseBigQueryTableFullName would parse table full name into splitted strings (project id, dataset, table name)
// Full name found in tableau with biqquery source is like this
// sometimes table name can also be the same as full name (e.g. project-id.schema.table1)
// `project-id.schema.table1`
// [project-id.schema].[table1]`
func parseBigQueryTableFullName(fullName string) (splittedFN []string, err error) {
	omitedChars := "`" + "\\[\\]"
	re := regexp.MustCompile("[" + omitedChars + "]")
	cleanedFN := re.ReplaceAllString(fullName, "")
	splittedFN = strings.Split(cleanedFN, ".")
	if len(splittedFN) == 3 {
		return
	}
	err = errors.New("unexpected length of splitted full name")
	return
}

func (dbs *DatabaseServer) CreateResourceURN(tableInfo Table) string {
	source := mapConnectionTypeToSource(dbs.ConnectionType)

	var urn string
	switch source {
	case bigquery:
		fullNameSplitted, err := parseBigQueryTableFullName(tableInfo.FullName)
		if err != nil {
			urn = plugins.BigQueryURN(fullNameSplitted[0], tableInfo.Schema, tableInfo.Name)
			break
		}
		urn = plugins.BigQueryURN(fullNameSplitted[0], fullNameSplitted[1], fullNameSplitted[2])
	case maxcompute:
		schema := tableInfo.Schema
		if schema == dbs.Name {
			schema = maxcomputeDefaultSchema
		}
		urn = plugins.MaxComputeURN(dbs.Name, schema, tableInfo.Name)
	default:
		host := fmt.Sprintf("%s:%d", dbs.HostName, dbs.Port)
		urn = models.NewURN(source, host, "table", fmt.Sprintf("%s.%s", dbs.Name, tableInfo.Name))
	}
	return urn
}

type CloudFile struct {
	ID             string `json:"id"`
	Name           string `json:"name"`
	ConnectionType string `json:"connectionType"`
	Description    string `json:"description"`
	Provider       string `json:"provider"`
	FileExtension  string `json:"fileExtension"`
	MimeType       string `json:"mimeType"`
	RequestURL     string `json:"requestUrl"`
}

func (cf *CloudFile) CreateResourceURN(tableInfo Table) string {
	source := mapConnectionTypeToSource(cf.ConnectionType)
	return models.NewURN(source, cf.Provider, "bucket", fmt.Sprintf("%s/%s", cf.Name, tableInfo.Name))
}

type File struct {
	ID             string `json:"id"`
	Name           string `json:"name"`
	ConnectionType string `json:"connectionType"`
	Description    string `json:"description"`
	FilePath       string `json:"filePath"`
}

func (f *File) CreateResourceURN(tableInfo Table) string {
	source := mapConnectionTypeToSource(f.ConnectionType)
	return models.NewURN(source, f.FilePath, "bucket", fmt.Sprintf("%s.%s", f.Name, tableInfo.Name))
}

type WebDataConnector struct {
	ID             string `json:"id"`
	Name           string `json:"name"`
	ConnectionType string `json:"connectionType"`
	Description    string `json:"description"`
	ConnectorURL   string `json:"connectorUrl"`
}

func (wdc *WebDataConnector) CreateResourceURN(tableInfo Table) string {
	source := mapConnectionTypeToSource(wdc.ConnectionType)
	return models.NewURN(source, wdc.ConnectorURL, "table", fmt.Sprintf("%s.%s", wdc.Name, tableInfo.Name))
}
