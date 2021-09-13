package bigquery

import (
	"context"
	_ "embed" // used to print the embedded assets
	"encoding/json"
	"fmt"
	"html/template"
	"strings"
	"sync"

	"cloud.google.com/go/bigquery"
	"github.com/odpf/meteor/models"
	"github.com/odpf/meteor/models/odpf/assets"
	"github.com/odpf/meteor/models/odpf/assets/common"
	"github.com/odpf/meteor/models/odpf/assets/facets"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/registry"
	"github.com/odpf/meteor/utils"
	"github.com/odpf/salt/log"
	"github.com/pkg/errors"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/protobuf/types/known/timestamppb"
)

//go:embed README.md
var summary string

// previewTotalRows is the number of rows to preview
const previewTotalRows = 30

// Config hold the set of configuration for the bigquery extractor
type Config struct {
	ProjectID            string `mapstructure:"project_id" validate:"required"`
	ServiceAccountJSON   string `mapstructure:"service_account_json"`
	TablePattern         string `mapstructure:"table_pattern"`
	IncludeColumnProfile bool   `mapstructure:"include_column_profile"`
}

var sampleConfig = `
 project_id: google-project-id
 table_pattern: gofood.fact_
 include_column_profile: true
 service_account_json: |-
   {
     "type": "service_account",
     "private_key_id": "xxxxxxx",
     "private_key": "xxxxxxx",
     "client_email": "xxxxxxx",
     "client_id": "xxxxxxx",
     "auth_uri": "https://accounts.google.com/o/oauth2/auth",
     "token_uri": "https://oauth2.googleapis.com/token",
     "auth_provider_x509_cert_url": "xxxxxxx",
     "client_x509_cert_url": "xxxxxxx"
   }`

// Extractor manages the communication with the bigquery service
type Extractor struct {
	logger log.Logger
	client *bigquery.Client
	config Config
}

func New(logger log.Logger) *Extractor {
	return &Extractor{
		logger: logger,
	}
}

// Info returns the detailed information about the extractor
func (e *Extractor) Info() plugins.Info {
	return plugins.Info{
		Description:  "Big Query table metadata and metrics",
		SampleConfig: sampleConfig,
		Summary:      summary,
		Tags:         []string{"gcp,table,extractor"},
	}
}

// Validate validates the configuration of the extractor
func (e *Extractor) Validate(configMap map[string]interface{}) (err error) {
	return utils.BuildConfig(configMap, &Config{})
}

// Extract checks if the table is valid and extracts the table schema
func (e *Extractor) Init(ctx context.Context, configMap map[string]interface{}) (err error) {
	err = utils.BuildConfig(configMap, &e.config)
	if err != nil {
		return plugins.InvalidConfigError{}
	}

	e.client, err = e.createClient(ctx)
	if err != nil {
		return
	}

	return
}

// Extract checks if the table is valid and extracts the table schema
func (e *Extractor) Extract(ctx context.Context, emitter plugins.Emitter) (err error) {
	// Fetch and iterate over datasets
	it := e.client.Datasets(ctx)
	for {
		ds, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return errors.Wrapf(err, "failed to fetch dataset")
		}
		e.extractTable(ctx, ds, emitter)
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
func (e *Extractor) extractTable(ctx context.Context, ds *bigquery.Dataset, emitter plugins.Emitter) {
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
			e.logger.Error("failed to fetch table's metadata, skipping table", "err", err, "table", table.FullyQualifiedName())
			continue
		}

		emitter.Emit(models.NewRecord(e.buildTable(ctx, table, tmd)))
	}
}

// Build the bigquery table metadata
func (e *Extractor) buildTable(ctx context.Context, t *bigquery.Table, md *bigquery.TableMetadata) *assets.Table {
	var partitionField string
	if md.TimePartitioning != nil {
		partitionField = md.TimePartitioning.Field
	}

	previewFields, previewRows, err := e.buildPreview(ctx, t)
	if err != nil {
		e.logger.Warn("error building preview", "err", err, "table", t.FullyQualifiedName())
	}

	return &assets.Table{
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
				"dataset":         t.DatasetID,
				"project":         t.ProjectID,
				"type":            string(md.Type),
				"partition_field": partitionField,
				"preview":         previewRows,
				"preview_fields":  previewFields,
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

func (e *Extractor) buildPreview(ctx context.Context, t *bigquery.Table) (fields []interface{}, preview []interface{}, err error) {
	fields = []interface{}{}  // list of column names
	preview = []interface{}{} // rows of column values

	rows := []interface{}{}
	totalRows := 0
	ri := t.Read(ctx)
	for totalRows < previewTotalRows {
		var row []bigquery.Value
		err = ri.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return
		}

		// populate row fields once
		if len(fields) < 1 {
			for _, schema := range ri.Schema {
				fields = append(fields, schema.Name)
			}
		}

		rows = append(rows, row)
		totalRows++
	}

	// this preview will be stored on Properties.Attributes which is a proto struct
	// we need to totally change []bigquery.Value to []interface{}
	// to prevent error when mapping this preview to Properties facets
	jsonBytes, err := json.Marshal(rows)
	if err != nil {
		return
	}
	err = json.Unmarshal(jsonBytes, &preview)
	if err != nil {
		return
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
		return New(plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}
