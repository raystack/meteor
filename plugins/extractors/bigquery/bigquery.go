package bigquery

import (
	"cloud.google.com/go/bigquery"
	"context"
	_ "embed" // used to print the embedded assets
	"encoding/base64"
	"encoding/json"
	"fmt"
	"html/template"
	"strings"
	"sync"

	"cloud.google.com/go/datacatalog/apiv1/datacatalogpb"

	datacatalog "cloud.google.com/go/datacatalog/apiv1"
	"github.com/goto/meteor/models"
	v1beta2 "github.com/goto/meteor/models/gotocompany/assets/v1beta2"
	"github.com/goto/meteor/plugins"
	"github.com/goto/meteor/plugins/extractors/bigquery/auditlog"
	"github.com/goto/meteor/registry"
	"github.com/goto/meteor/utils"
	"github.com/goto/salt/log"
	"github.com/pkg/errors"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

//go:embed README.md
var summary string

// Config holds the set of configuration for the bigquery extractor
type Config struct {
	ProjectID string `mapstructure:"project_id" validate:"required"`
	// ServiceAccountBase64 takes precedence over ServiceAccountJSON field
	ServiceAccountBase64 string   `mapstructure:"service_account_base64"`
	ServiceAccountJSON   string   `mapstructure:"service_account_json"`
	MaxPageSize          int      `mapstructure:"max_page_size"`
	TablePattern         string   `mapstructure:"table_pattern"`
	Exclude              Exclude  `mapstructure:"exclude"`
	IncludeColumnProfile bool     `mapstructure:"include_column_profile"`
	MaxPreviewRows       int      `mapstructure:"max_preview_rows" default:"30"`
	IsCollectTableUsage  bool     `mapstructure:"collect_table_usage" default:"false"`
	UsagePeriodInDay     int64    `mapstructure:"usage_period_in_day" default:"7"`
	UsageProjectIDs      []string `mapstructure:"usage_project_ids"`
}

type Exclude struct {
	// list of datasetIDs
	Datasets []string `mapstructure:"datasets"`
	// list of tableNames in format - datasetID.tableID
	Tables []string `mapstructure:"tables"`
}

const (
	maxPageSizeDefault = 100
)

var sampleConfig = `
project_id: google-project-id
table_pattern: gofood.fact_
exclude:
  datasets:
	- dataset_a
	- dataset_b
  tables:
	- dataset_c.table_a
max_page_size: 100
include_column_profile: true
# Only one of service_account_base64 / service_account_json is needed. 
# If both are present, service_account_base64 takes precedence
service_account_base64: ____base64_encoded_service_account____
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
  }
collect_table_usage: false
usage_period_in_day: 7`

var info = plugins.Info{
	Description:  "Big Query table metadata and metrics",
	SampleConfig: sampleConfig,
	Tags:         []string{"gcp", "table"},
	Summary:      summary,
}

// Extractor manages the communication with the bigquery service
type Extractor struct {
	plugins.BaseExtractor
	logger          log.Logger
	client          *bigquery.Client
	config          Config
	galClient       *auditlog.AuditLog
	policyTagClient *datacatalog.PolicyTagManagerClient
}

func New(logger log.Logger) *Extractor {
	galc := auditlog.New(logger)

	e := &Extractor{
		logger:    logger,
		galClient: galc,
	}
	e.BaseExtractor = plugins.NewBaseExtractor(info, &e.config)
	e.ScopeNotRequired = true

	return e
}

// Init initializes the extractor
func (e *Extractor) Init(ctx context.Context, config plugins.Config) (err error) {
	if err = e.BaseExtractor.Init(ctx, config); err != nil {
		return err
	}

	e.client, err = e.createClient(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to create client")
	}

	if e.config.IsCollectTableUsage {
		errL := e.galClient.Init(ctx,
			auditlog.InitWithConfig(auditlog.Config{
				ProjectID:           e.config.ProjectID,
				ServiceAccountJSON:  e.config.ServiceAccountJSON,
				IsCollectTableUsage: e.config.IsCollectTableUsage,
				UsagePeriodInDay:    e.config.UsagePeriodInDay,
				UsageProjectIDs:     e.config.UsageProjectIDs,
			}),
		)
		if errL != nil {
			e.logger.Error("failed to create google audit log client", "err", errL)
		}
	}

	e.policyTagClient, err = e.createPolicyTagClient(ctx)
	if err != nil {
		e.logger.Error("failed to create policy tag manager client", "err", err)
	}

	return
}

// Extract checks if the table is valid and extracts the table schema
func (e *Extractor) Extract(ctx context.Context, emit plugins.Emit) (err error) {

	// Fetch and iterate over datasets
	it := e.client.Datasets(ctx)
	it.PageInfo().MaxSize = e.getMaxPageSize()
	for {
		ds, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return errors.Wrap(err, "failed to fetch dataset")
		}
		if IsExcludedDataset(ds.DatasetID, e.config.Exclude.Datasets) {
			e.logger.Debug("excluding dataset from bigquery extract", "dataset_id", ds.DatasetID)
			continue
		}
		e.extractTable(ctx, ds, emit)
	}

	return
}

// Create big query client
func (e *Extractor) createClient(ctx context.Context) (*bigquery.Client, error) {
	if e.config.ServiceAccountBase64 == "" && e.config.ServiceAccountJSON == "" {
		e.logger.Info("credentials are not specified, creating bigquery client using default credentials...")
		return bigquery.NewClient(ctx, e.config.ProjectID)
	}

	if e.config.ServiceAccountBase64 != "" {
		serviceAccountJSON, err := base64.StdEncoding.DecodeString(e.config.ServiceAccountBase64)
		if err != nil || len(serviceAccountJSON) == 0 {
			return nil, errors.Wrap(err, "failed to decode base64 service account")
		}
		// overwrite ServiceAccountJSON with credentials from ServiceAccountBase64 value
		e.config.ServiceAccountJSON = string(serviceAccountJSON)
	}

	return bigquery.NewClient(ctx, e.config.ProjectID, option.WithCredentialsJSON([]byte(e.config.ServiceAccountJSON)))
}

func (e *Extractor) createPolicyTagClient(ctx context.Context) (*datacatalog.PolicyTagManagerClient, error) {
	policyManager, err := datacatalog.NewPolicyTagManagerClient(ctx, option.WithCredentialsJSON([]byte(e.config.ServiceAccountJSON)))
	if err != nil {
		return nil, err
	}

	return policyManager, nil
}

// Create big query client
func (e *Extractor) extractTable(ctx context.Context, ds *bigquery.Dataset, emit plugins.Emit) {
	tb := ds.Tables(ctx)
	tb.PageInfo().MaxSize = e.getMaxPageSize()
	for {
		table, err := tb.Next()
		if errors.Is(err, iterator.Done) || errors.Is(err, context.Canceled) {
			break
		} else if err != nil {
			e.logger.Error("failed to get table, skipping table", "err", err)
			continue
		}

		if IsExcludedTable(ds.DatasetID, table.TableID, e.config.Exclude.Tables) {
			e.logger.Debug("excluding table from bigquery extract", "dataset_id", ds.DatasetID, "table_id", table.TableID)
			continue
		}

		tableFQN := table.FullyQualifiedName()

		e.logger.Debug("extracting table", "table", tableFQN)
		tmd, err := table.Metadata(ctx)
		if err != nil {
			e.logger.Error("failed to fetch table metadata", "err", err, "table", tableFQN)
			continue
		}

		asset, err := e.buildAsset(ctx, table, tmd)
		if err != nil {
			e.logger.Error("failed to build asset", "err", err, "table", tableFQN)
			continue
		}

		emit(models.NewRecord(asset))
	}
}

// Build the bigquery table metadata
func (e *Extractor) buildAsset(ctx context.Context, t *bigquery.Table, md *bigquery.TableMetadata) (asset *v1beta2.Asset, err error) {
	var tableStats *auditlog.TableStats
	if e.config.IsCollectTableUsage {
		// Fetch and extract logs first to build a map
		var errL error
		tableStats, errL = e.galClient.Collect(ctx, t.TableID)
		if errL != nil {
			e.logger.Warn("error populating table stats usage", "error", errL)
		}
	}

	tableFQN := t.FullyQualifiedName()
	tableURN := plugins.BigQueryURN(t.ProjectID, t.DatasetID, t.TableID)

	tableProfile := e.buildTableProfile(tableURN, tableStats)
	var partitionField string
	partitionData := make(map[string]interface{})
	if md.TimePartitioning != nil {
		partitionField = md.TimePartitioning.Field
		if partitionField == "" {
			partitionField = "_PARTITIONTIME"
		}
		partitionData["partition_field"] = partitionField
		partitionData["time_partition"] = map[string]interface{}{
			"partition_by":             string(md.TimePartitioning.Type),
			"partition_expire_seconds": md.TimePartitioning.Expiration.Seconds(),
		}
	} else if md.RangePartitioning != nil {
		partitionData["partition_field"] = md.RangePartitioning.Field
		partitionData["range_partition"] = map[string]interface{}{
			"start":    md.RangePartitioning.Range.Start,
			"end":      md.RangePartitioning.Range.End,
			"interval": md.RangePartitioning.Range.Interval,
		}
	}
	partitionData["require_partition_filter"] = md.RequirePartitionFilter

	var clusteringFields []interface{}
	if md.Clustering != nil && len(md.Clustering.Fields) > 0 {
		clusteringFields = make([]interface{}, len(md.Clustering.Fields))
		for idx, field := range md.Clustering.Fields {
			clusteringFields[idx] = field
		}
	}

	var previewFields []string
	var previewRows *structpb.ListValue
	if md.Type == bigquery.RegularTable {
		var err error
		previewFields, previewRows, err = e.buildPreview(ctx, t)
		if err != nil {
			e.logger.Warn("error building preview", "err", err, "table", tableFQN)
		}
	}

	table, err := anypb.New(&v1beta2.Table{
		Columns:       e.buildColumns(ctx, md.Schema, md),
		PreviewFields: previewFields,
		PreviewRows:   previewRows,
		Profile:       tableProfile,
		Attributes: utils.TryParseMapToProto(map[string]interface{}{
			"full_qualified_name": tableFQN,
			"dataset":             t.DatasetID,
			"project":             t.ProjectID,
			"type":                string(md.Type),
			"partition_data":      partitionData,
			"clustering_fields":   clusteringFields,
		}),
		CreateTime: timestamppb.New(md.CreationTime),
		UpdateTime: timestamppb.New(md.LastModifiedTime),
	})
	if err != nil {
		e.logger.Warn("error creating Any struct", "error", err)
	}

	return &v1beta2.Asset{
		Urn:         tableURN,
		Name:        t.TableID,
		Type:        "table",
		Description: md.Description,
		Service:     "bigquery",
		Data:        table,
		Labels:      md.Labels,
	}, nil
}

// Extract table schema
func (e *Extractor) buildColumns(ctx context.Context, schema bigquery.Schema, tm *bigquery.TableMetadata) []*v1beta2.Column {
	var wg sync.WaitGroup

	wg.Add(len(schema))
	columns := make([]*v1beta2.Column, len(schema))
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

func (e *Extractor) buildColumn(ctx context.Context, field *bigquery.FieldSchema, tm *bigquery.TableMetadata) (col *v1beta2.Column) {
	attributesMap := map[string]interface{}{
		"mode": e.getColumnMode(field),
	}

	colPolicyTags := e.getPolicyTagList(ctx, field)
	if len(colPolicyTags) > 0 {
		attributesMap["policy_tags"] = colPolicyTags
	}

	col = &v1beta2.Column{
		Name:        field.Name,
		Description: field.Description,
		DataType:    string(field.Type),
		IsNullable:  !(field.Required || field.Repeated),
		Attributes:  utils.TryParseMapToProto(attributesMap),
	}

	if len(field.Schema) > 0 {
		col.Columns = e.buildColumns(ctx, field.Schema, tm)
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

func (e *Extractor) buildPreview(ctx context.Context, t *bigquery.Table) (fields []string, rows *structpb.ListValue, err error) {
	if e.config.MaxPreviewRows == 0 {
		return
	}

	tempRows := []interface{}{}
	totalRows := 0
	ri := t.Read(ctx)
	// fetch only the required amount of rows
	maxPageSize := e.getMaxPageSize()
	if maxPageSize > e.config.MaxPreviewRows {
		ri.PageInfo().MaxSize = e.config.MaxPreviewRows
	} else {
		ri.PageInfo().MaxSize = maxPageSize
	}

	for totalRows < e.config.MaxPreviewRows {
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

		var temp []interface{}
		var jsonBytes []byte
		jsonBytes, err = json.Marshal(row)
		if err != nil {
			err = errors.Wrapf(err, "error marshalling \"%s\" to json", t.FullyQualifiedName())
			return
		}
		// sanitize unicode sequence
		// replace unicode null characters with "null" string to ensure downstream would not have issues dealing with unicode null characters
		jsonString := strings.ReplaceAll(string(jsonBytes), "\\u0000", "null")
		jsonBytes = []byte(jsonString)
		err = json.Unmarshal(jsonBytes, &temp)
		if err != nil {
			err = errors.Wrapf(err, "error marshalling \"%s\" to json", t.FullyQualifiedName())
			return
		}

		tempRows = append(tempRows, temp)

		totalRows++
	}

	rows, err = structpb.NewList(tempRows)
	if err != nil {
		err = errors.Wrap(err, "error creating preview list")
		return
	}

	return
}

func (e *Extractor) getColumnProfile(ctx context.Context, col *bigquery.FieldSchema, tm *bigquery.TableMetadata) (cp *v1beta2.ColumnProfile, err error) {
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
	it.PageInfo().MaxSize = e.getMaxPageSize()
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
	cp = &v1beta2.ColumnProfile{
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

func (e *Extractor) getPolicyTagList(ctx context.Context, col *bigquery.FieldSchema) []interface{} {
	if col.PolicyTags == nil || e.policyTagClient == nil {
		return nil
	}

	pt := make([]interface{}, 0, len(col.PolicyTags.Names))
	for _, name := range col.PolicyTags.Names {
		policyTag, err := e.policyTagClient.GetPolicyTag(ctx, &datacatalogpb.GetPolicyTagRequest{Name: name})
		if err != nil {
			e.logger.Error("error fetching policy_tag", "policy_tag", name, "err", err)
			continue
		}

		pt = append(pt, fmt.Sprintf("policy_tag:%s:%s", policyTag.DisplayName, policyTag.Name))
	}

	return pt
}

func IsExcludedDataset(datasetID string, excludedDatasets []string) bool {
	for _, d := range excludedDatasets {
		if datasetID == d {
			return true
		}
	}

	return false
}

func IsExcludedTable(datasetID, tableID string, excludedTables []string) bool {
	tableName := fmt.Sprintf("%s.%s", datasetID, tableID)
	for _, t := range excludedTables {
		if tableName == t {
			return true
		}
	}

	return false
}

// getMaxPageSize returns max_page_size if configured in recipe, otherwise returns default value
func (e *Extractor) getMaxPageSize() int {
	if e.config.MaxPageSize > 0 {
		return e.config.MaxPageSize
	}

	// default max page size
	return maxPageSizeDefault
}

// Register the extractor to catalog
func init() {
	if err := registry.Extractors.Register("bigquery", func() plugins.Extractor {
		return New(plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}
