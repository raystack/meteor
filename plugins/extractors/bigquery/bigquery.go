package bigquery

import (
	"context"
	"fmt"
	"html/template"
	"strings"
	"sync"

	"cloud.google.com/go/bigquery"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/proto/odpf/assets"
	"github.com/odpf/meteor/proto/odpf/assets/common"
	"github.com/odpf/meteor/proto/odpf/assets/facets"
	"github.com/odpf/meteor/registry"
	"github.com/odpf/meteor/utils"
	"github.com/odpf/salt/log"
	"github.com/pkg/errors"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Config struct {
	ProjectID            string `mapstructure:"project_id" validate:"required"`
	ServiceAccountJSON   string `mapstructure:"service_account_json"`
	TablePattern         string `mapstructure:"table_pattern"`
	IncludeColumnProfile bool   `mapstructure:"include_column_profile"`
}

var (
	configInfo = ``
	inputInfo  = `
Input:
 _______________________________________________________________________________________________________________________
| Key               | Example                             | Description                                    |            |
|___________________|_____________________________________|________________________________________________|____________|
| "project_id"      | "my-project"                        | BigQuery Project ID                            | *required* |
| "credentials_json"| "{'private_key':., 'private_id':.}" | Service Account in JSON string                 | *optional* |
| "table_pattern"   | "gofood.fact_"                      | Regex pattern, filters bigquery table to scan  | *optional* |
| "profile_column"  | "true"                              | true to have profile the column value          | *optional* |
|___________________|_____________________________________|________________________________________________|____________|
`
	outputInfo = `
Output:
 ___________________________________________________________
|Field               |Sample Value                          |
|____________________|______________________________________|
|"resource.urn"      |"project_id.dataset_name.table_name"  |
|"resource.name"     |"table_name"                          |
|"resource.service"  |"bigquery"                            |
|"description"       |"table description"                   |
|"profile.total_rows"|"2100"                                |
|"schema"            |[]Column             	                |
|____________________|______________________________________|`
)

type Extractor struct {
	logger log.Logger
	client *bigquery.Client
	config Config
}

func (e *Extractor) GetDescription() string {
	return inputInfo + outputInfo
}

func (e *Extractor) GetSampleConfig() string {
	return configInfo
}

func (e *Extractor) Extract(ctx context.Context, config map[string]interface{}, out chan<- interface{}) (err error) {
	err = utils.BuildConfig(config, &e.config)
	if err != nil {
		return plugins.InvalidConfigError{}
	}

	e.client, err = e.createClient(ctx)
	if err != nil {
		return
	}

	// Fetch and iterate over datesets
	it := e.client.Datasets(ctx)
	for {
		ds, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return errors.Wrap(err, "failed to fetch dataset")
		}
		e.extractTable(ctx, ds, out)
	}

	return

}

// Create big query client
func (e *Extractor) createClient(ctx context.Context) (*bigquery.Client, error) {
	if e.config.ServiceAccountJSON == "" {
		e.logger.Info("credentials are not specified, creating bigquery client using default credentials...")
		return bigquery.NewClient(ctx, e.config.ProjectID)
	}

	return bigquery.NewClient(ctx, e.config.ProjectID, option.WithCredentialsJSON([]byte(e.config.ServiceAccountJSON)))
}

// Create big query client
func (e *Extractor) extractTable(ctx context.Context, ds *bigquery.Dataset, out chan<- interface{}) {
	tb := ds.Tables(ctx)
	for {
		table, err := tb.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			e.logger.Error("failed to scan, skipping table", "err", err)
			continue
		}
		tmd, err := table.Metadata(ctx)
		if err != nil {
			e.logger.Error("failed to fetch table's metadata, skipping table", "err", err)
			continue
		}

		out <- e.buildTable(ctx, table, tmd)
	}
}

// Build the bigquery table metadata
func (e *Extractor) buildTable(ctx context.Context, t *bigquery.Table, md *bigquery.TableMetadata) assets.Table {
	return assets.Table{
		Resource: &common.Resource{
			Urn:     fmt.Sprintf("%s:%s.%s", t.ProjectID, t.DatasetID, t.TableID),
			Name:    t.TableID,
			Service: "bigquery",
		},
		Schema: &facets.Columns{
			Columns: e.buildColumns(ctx, md),
		},
		Properties: &facets.Properties{
			Attributes: utils.TryParseMapToProto(map[string]interface{}{
				"dataset": t.DatasetID,
				"project": t.ProjectID,
				"type":    string(md.Type),
			}),
			Labels: md.Labels,
		},
		Timestamps: &common.Timestamp{
			CreateTime: timestamppb.New(md.CreationTime),
			UpdateTime: timestamppb.New(md.LastModifiedTime),
		},
	}
}

// Extract table schema
func (e *Extractor) buildColumns(ctx context.Context, tm *bigquery.TableMetadata) []*facets.Column {
	schema := tm.Schema
	var wg sync.WaitGroup

	wg.Add(len(schema))
	columns := make([]*facets.Column, len(schema))
	for i, b := range schema {
		index := i
		go func(s *bigquery.FieldSchema) {
			defer wg.Done()

			columns[index] = e.buildColumn(ctx, s, tm)
		}(b)
	}
	wg.Wait()

	return columns
}

func (e *Extractor) buildColumn(ctx context.Context, field *bigquery.FieldSchema, tm *bigquery.TableMetadata) (col *facets.Column) {
	col = &facets.Column{
		Name:        field.Name,
		Description: field.Description,
		DataType:    string(field.Type),
		IsNullable:  !(field.Required || field.Repeated),
		Properties: &facets.Properties{
			Attributes: utils.TryParseMapToProto(map[string]interface{}{
				"mode": e.getColumnMode(field),
			}),
		},
	}

	if e.config.IncludeColumnProfile {
		profile, err := e.getColumnProfile(ctx, field, tm)
		if err != nil {
			e.logger.Error("error fetching column's profile", "error", err)
		}
		col.Profile = profile
	}

	return
}

func (e *Extractor) getColumnProfile(ctx context.Context, col *bigquery.FieldSchema, tm *bigquery.TableMetadata) (cp *facets.ColumnProfile, err error) {
	if col.Type == bigquery.BytesFieldType || col.Repeated || col.Type == bigquery.RecordFieldType {
		e.logger.Info("Skip profiling " + col.Name + " column")
		return
	}

	// build and run query
	query, err := e.buildColumnProfileQuery(col, tm)
	if err != nil {
		return nil, err
	}
	it, err := query.Read(ctx)
	if err != nil {
		return nil, err
	}

	// fetch first row for column profile result
	type Row struct {
		Min    string  `bigquery:"min"`
		Max    string  `bigquery:"max"`
		Avg    float64 `bigquery:"avg"`
		Med    float64 `bigquery:"med"`
		Unique int64   `bigquery:"unique"`
		Count  int64   `bigquery:"count"`
		Top    string  `bigquery:"top"`
	}
	var row Row
	err = it.Next(&row)
	if err != nil && err != iterator.Done {
		return
	}

	// map row data to column profile
	cp = &facets.ColumnProfile{
		Min:    row.Min,
		Max:    row.Max,
		Avg:    row.Avg,
		Med:    row.Med,
		Unique: row.Unique,
		Count:  row.Count,
		Top:    row.Top,
	}

	return
}

func (e *Extractor) buildColumnProfileQuery(col *bigquery.FieldSchema, tm *bigquery.TableMetadata) (query *bigquery.Query, err error) {
	queryTemplate := `SELECT
		COALESCE(CAST(MIN({{ .ColumnName }}) AS STRING), "") AS min,
		COALESCE(CAST(MAX({{ .ColumnName }}) AS STRING), "") AS max,
		COALESCE(AVG(SAFE_CAST(SAFE_CAST({{ .ColumnName }} AS STRING) AS FLOAT64)), 0.0) AS avg,
		COALESCE(SAFE_CAST(CAST(APPROX_QUANTILES({{ .ColumnName }}, 2)[OFFSET(1)] AS STRING) AS FLOAT64), 0.0) AS med,
		COALESCE(APPROX_COUNT_DISTINCT({{ .ColumnName }}),0) AS unique,
		COALESCE(COUNT({{ .ColumnName }}), 0) AS count,
		COALESCE(CAST(APPROX_TOP_COUNT({{ .ColumnName }}, 1)[OFFSET(0)].value AS STRING), "") AS top
	FROM
		{{ .TableName }}`
	data := map[string]interface{}{
		"ColumnName": col.Name,
		"TableName":  strings.ReplaceAll(tm.FullID, ":", "."),
	}
	temp := template.Must(template.New("query").Parse(queryTemplate))
	builder := &strings.Builder{}
	err = temp.Execute(builder, data)
	if err != nil {
		return
	}
	finalQuery := builder.String()
	query = e.client.Query(finalQuery)

	return
}

func (e *Extractor) getColumnMode(col *bigquery.FieldSchema) string {
	switch {
	case col.Repeated:
		return "REPEATED"
	case col.Required:
		return "REQUIRED"
	default:
		return "NULLABLE"
	}
}

// Register the extractor to catalog
func init() {
	if err := registry.Extractors.Register("bigquery", func() plugins.Extractor {
		return &Extractor{
			logger: plugins.GetLog(),
		}
	}); err != nil {
		panic(err)
	}
}
