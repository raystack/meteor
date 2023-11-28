package bigquery

import (
	"context"
	_ "embed" // used to print the embedded assets
	"encoding/base64"
	"encoding/json"
	"fmt"
	"html/template"
	"math/rand"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/bigquery"
	datacatalog "cloud.google.com/go/datacatalog/apiv1"
	"cloud.google.com/go/datacatalog/apiv1/datacatalogpb"
	"github.com/goto/meteor/models"
	v1beta2 "github.com/goto/meteor/models/gotocompany/assets/v1beta2"
	"github.com/goto/meteor/plugins"
	"github.com/goto/meteor/plugins/extractors/bigquery/auditlog"
	"github.com/goto/meteor/plugins/extractors/bigquery/upstream"
	"github.com/goto/meteor/registry"
	"github.com/goto/meteor/utils"
	"github.com/goto/salt/log"
	"github.com/pkg/errors"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
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
	ServiceAccountBase64 string  `mapstructure:"service_account_base64"`
	ServiceAccountJSON   string  `mapstructure:"service_account_json"`
	MaxPageSize          int     `mapstructure:"max_page_size"`
	DatasetPageSize      int     `mapstructure:"dataset_page_size"`
	TablePageSize        int     `mapstructure:"table_page_size"`
	TablePattern         string  `mapstructure:"table_pattern"`
	Exclude              Exclude `mapstructure:"exclude"`
	IncludeColumnProfile bool    `mapstructure:"include_column_profile"`
	// MaxPreviewRows can also be set to -1 to restrict adding preview_rows key in asset data
	MaxPreviewRows      int      `mapstructure:"max_preview_rows" default:"30"`
	MixValues           bool     `mapstructure:"mix_values" default:"false"`
	IsCollectTableUsage bool     `mapstructure:"collect_table_usage" default:"false"`
	UsagePeriodInDay    int64    `mapstructure:"usage_period_in_day" default:"7"`
	UsageProjectIDs     []string `mapstructure:"usage_project_ids"`
	BuildViewLineage    bool     `mapstructure:"build_view_lineage" default:"false"`
}

type Exclude struct {
	// list of datasetIDs
	Datasets []string `mapstructure:"datasets"`
	// list of tableNames in format - datasetID.tableID
	Tables []string `mapstructure:"tables"`
}

const (
	maxPageSizeDefault = 100

	metricDatasetsDurn       = "meteor.bq.client.datasets.duration"
	metricTablesDurn         = "meteor.bq.client.tables.duration"
	metricTableDurn          = "meteor.bq.client.table.duration"
	metricExcludedDatasetCtr = "meteor.bq.dataset.excluded"
	metricExcludedTableCtr   = "meteor.bq.table.excluded"
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
build_view_lineage: true
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
	newClient       NewClientFunc
	randFn          randFn

	datasetsDurn       metric.Int64Histogram
	tablesDurn         metric.Int64Histogram
	tableDurn          metric.Int64Histogram
	excludedDatasetCtr metric.Int64Counter
	excludedTableCtr   metric.Int64Counter
}

type randFn func(rndSeed int64) func(int64) int64

type NewClientFunc func(ctx context.Context, logger log.Logger, config *Config) (*bigquery.Client, error)

func New(logger log.Logger, newClient NewClientFunc, randFn randFn) *Extractor {
	meter := otel.Meter("github.com/goto/meteor/plugins/extractors/bigquery")

	datasetsDurn, err := meter.Int64Histogram(metricDatasetsDurn, metric.WithUnit("ms"))
	handleOtelErr(err)

	tablesDurn, err := meter.Int64Histogram(metricTablesDurn, metric.WithUnit("ms"))
	handleOtelErr(err)

	tableDurn, err := meter.Int64Histogram(metricTableDurn, metric.WithUnit("ms"))
	handleOtelErr(err)

	excludedDatasetCtr, err := meter.Int64Counter(metricExcludedDatasetCtr)
	handleOtelErr(err)

	excludedTableCtr, err := meter.Int64Counter(metricExcludedTableCtr)
	handleOtelErr(err)

	galc := auditlog.New(logger)

	e := &Extractor{
		logger:    logger,
		galClient: galc,
		newClient: newClient,
		randFn:    randFn,

		datasetsDurn:       datasetsDurn,
		tablesDurn:         tablesDurn,
		tableDurn:          tableDurn,
		excludedDatasetCtr: excludedDatasetCtr,
		excludedTableCtr:   excludedTableCtr,
	}
	e.BaseExtractor = plugins.NewBaseExtractor(info, &e.config)
	e.ScopeNotRequired = true

	return e
}

// Init initializes the extractor
func (e *Extractor) Init(ctx context.Context, config plugins.Config) error {
	if err := e.BaseExtractor.Init(ctx, config); err != nil {
		return err
	}

	var err error
	e.client, err = e.newClient(ctx, e.logger, &e.config)
	if err != nil {
		return fmt.Errorf("create client: %w", err)
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

	return nil
}

// Extract checks if the table is valid and extracts the table schema
func (e *Extractor) Extract(ctx context.Context, emit plugins.Emit) error {
	pageSize := pickFirstNonZero(e.config.DatasetPageSize, e.config.MaxPageSize, 10)

	// Fetch and iterate over datasets
	pager := iterator.NewPager(e.client.Datasets(ctx), pageSize, "")
	for {
		datasets, hasNext, err := e.fetchDatasetsNextPage(ctx, pager)
		if err != nil {
			return err
		}

		for _, ds := range datasets {
			if IsExcludedDataset(ds.DatasetID, e.config.Exclude.Datasets) {
				e.excludedDatasetCtr.Add(
					ctx, 1, metric.WithAttributes(attribute.String("bq.project_id", e.config.ProjectID)),
				)
				e.logger.Debug("excluding dataset from bigquery extract", "dataset_id", ds.DatasetID)
				continue
			}
			e.extractTable(ctx, ds, emit)
		}

		if !hasNext {
			break
		}
	}

	return nil
}

func (e *Extractor) fetchDatasetsNextPage(ctx context.Context, pager *iterator.Pager) (datasets []*bigquery.Dataset, hasNext bool, err error) {
	defer func(start time.Time) {
		attrs := []attribute.KeyValue{attribute.String("bq.project_id", e.config.ProjectID)}
		if err != nil {
			attrs = append(attrs, attribute.String("bq.error_code", plugins.BQErrReason(err)))
		}
		e.datasetsDurn.Record(
			ctx, time.Since(start).Milliseconds(), metric.WithAttributes(attrs...),
		)
	}(time.Now())

	nextToken, err := pager.NextPage(&datasets)
	if err != nil {
		return nil, false, fmt.Errorf("fetch dataset: %w", err)
	}

	return datasets, nextToken != "", nil
}

// CreateClient creates a bigquery client
func CreateClient(ctx context.Context, logger log.Logger, config *Config) (*bigquery.Client, error) {
	if config.ServiceAccountBase64 == "" && config.ServiceAccountJSON == "" {
		logger.Info("credentials are not specified, creating bigquery client using default credentials...")
		return bigquery.NewClient(ctx, config.ProjectID)
	}

	if config.ServiceAccountBase64 != "" {
		serviceAccountJSON, err := base64.StdEncoding.DecodeString(config.ServiceAccountBase64)
		if err != nil || len(serviceAccountJSON) == 0 {
			return nil, fmt.Errorf("decode base64 service account: %w", err)
		}
		// overwrite ServiceAccountJSON with credentials from ServiceAccountBase64 value
		config.ServiceAccountJSON = string(serviceAccountJSON)
	}

	return bigquery.NewClient(ctx, config.ProjectID, option.WithCredentialsJSON([]byte(config.ServiceAccountJSON)))
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
	pageSize := pickFirstNonZero(e.config.TablePageSize, e.config.MaxPageSize, 50)

	pager := iterator.NewPager(ds.Tables(ctx), pageSize, "")
	for {
		tables, hasNext, err := e.fetchTablesNextPage(ctx, ds.DatasetID, pager)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				break
			}

			e.logger.Error("failed to get page of tables, skipping page", "err", err)
			continue
		}

		for _, table := range tables {
			if IsExcludedTable(ds.DatasetID, table.TableID, e.config.Exclude.Tables) {
				e.excludedTableCtr.Add(ctx, 1, metric.WithAttributes(
					attribute.String("bq.project_id", e.config.ProjectID),
					attribute.String("bq.dataset_id", ds.DatasetID),
				))
				e.logger.Debug("excluding table from bigquery extract", "dataset_id", ds.DatasetID, "table_id", table.TableID)
				continue
			}

			tableFQN := table.FullyQualifiedName()

			e.logger.Debug("extracting table", "table", tableFQN)
			tmd, err := e.fetchTableMetadata(ctx, table)
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

		if !hasNext {
			break
		}
	}
}

func (e *Extractor) fetchTablesNextPage(
	ctx context.Context, datasetID string, pager *iterator.Pager,
) (tables []*bigquery.Table, hasNext bool, err error) {
	defer func(start time.Time) {
		attrs := []attribute.KeyValue{
			attribute.String("bq.project_id", e.config.ProjectID),
			attribute.String("bq.dataset_id", datasetID),
		}
		if err != nil {
			attrs = append(attrs, attribute.String("bq.error_code", plugins.BQErrReason(err)))
		}

		e.tablesDurn.Record(
			ctx, time.Since(start).Milliseconds(), metric.WithAttributes(attrs...),
		)
	}(time.Now())

	nextToken, err := pager.NextPage(&tables)
	if err != nil {
		return nil, false, err
	}

	return tables, nextToken != "", nil
}

// Build the bigquery table metadata
func (e *Extractor) buildAsset(ctx context.Context, t *bigquery.Table, md *bigquery.TableMetadata) (*v1beta2.Asset, error) {
	var tableStats *auditlog.TableStats
	if e.config.IsCollectTableUsage {
		// Fetch and extract logs first to build a map
		var errL error
		tableStats, errL = e.galClient.Collect(ctx, t)
		if errL != nil {
			e.logger.Warn("error populating table stats usage", "error", errL)
		}
	}

	tableFQN := t.FullyQualifiedName()
	tableURN := plugins.BigQueryURN(t.ProjectID, t.DatasetID, t.TableID)

	tableProfile := e.buildTableProfile(tableURN, tableStats, md)
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
		previewFields, previewRows, err = e.buildPreview(ctx, t, md)
		if err != nil {
			e.logger.Warn("error building preview", "err", err, "table", tableFQN)
		}
	}

	asset := &v1beta2.Asset{
		Urn:         tableURN,
		Name:        t.TableID,
		Type:        "table",
		Description: md.Description,
		Service:     "bigquery",
		Labels:      md.Labels,
	}

	attributesData := map[string]interface{}{
		"full_qualified_name": tableFQN,
		"dataset":             t.DatasetID,
		"project":             t.ProjectID,
		"type":                string(md.Type),
		"partition_data":      partitionData,
		"clustering_fields":   clusteringFields,
	}

	if md.Type == bigquery.ViewTable || md.Type == bigquery.MaterializedView {
		query := getViewQuery(md)
		attributesData["sql"] = query
		if e.config.BuildViewLineage {
			upstreamResources := getUpstreamResources(query)
			asset.Lineage = &v1beta2.Lineage{
				Upstreams: upstreamResources,
			}
		}
	}

	tableData := &v1beta2.Table{
		Columns:    e.buildColumns(ctx, md.Schema, md),
		Profile:    tableProfile,
		Attributes: utils.TryParseMapToProto(attributesData),
		CreateTime: timestamppb.New(md.CreationTime),
		UpdateTime: timestamppb.New(md.LastModifiedTime),
	}

	maxPreviewRows := e.config.MaxPreviewRows
	if maxPreviewRows != -1 {
		tableData.PreviewFields = previewFields
		tableData.PreviewRows = previewRows
	}

	table, err := anypb.New(tableData)
	if err != nil {
		e.logger.Warn("error creating Any struct", "error", err)
	}
	asset.Data = table

	return asset, nil
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

func (e *Extractor) buildColumn(ctx context.Context, field *bigquery.FieldSchema, tm *bigquery.TableMetadata) *v1beta2.Column {
	attributesMap := map[string]interface{}{
		"mode": e.getColumnMode(field),
	}

	colPolicyTags := e.getPolicyTagList(ctx, field)
	if len(colPolicyTags) > 0 {
		attributesMap["policy_tags"] = colPolicyTags
	}

	col := &v1beta2.Column{
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

	return col
}

func (e *Extractor) buildPreview(ctx context.Context, t *bigquery.Table, md *bigquery.TableMetadata) (fields []string, rows *structpb.ListValue, err error) {
	maxPreviewRows := e.config.MaxPreviewRows
	if maxPreviewRows <= 0 {
		return nil, nil, nil
	}

	var tempRows []interface{}
	totalRows := 0
	ri := t.Read(ctx)
	// fetch only the required amount of rows
	maxPageSize := e.getMaxPageSize()
	if maxPageSize > maxPreviewRows {
		ri.PageInfo().MaxSize = maxPreviewRows
	} else {
		ri.PageInfo().MaxSize = maxPageSize
	}

	for totalRows < maxPreviewRows {
		var row []bigquery.Value
		err := ri.Next(&row)
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, nil, err
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
			return nil, nil, fmt.Errorf("marshal %q to json: %w", t.FullyQualifiedName(), err)
		}
		// sanitize unicode sequence
		// replace unicode null characters with "null" string to ensure downstream would not have issues dealing with unicode null characters
		jsonString := strings.ReplaceAll(string(jsonBytes), "\\u0000", "null")
		jsonBytes = []byte(jsonString)
		if err = json.Unmarshal(jsonBytes, &temp); err != nil {
			return nil, nil, fmt.Errorf("marshal %q to json: %w", t.FullyQualifiedName(), err)
		}

		tempRows = append(tempRows, temp)

		totalRows++
	}

	tempRows, err = e.mixValuesIfNeeded(tempRows, md.LastModifiedTime.Unix())
	if err != nil {
		return nil, nil, fmt.Errorf("mix values: %w", err)
	}

	rows, err = structpb.NewList(tempRows)
	if err != nil {
		return nil, nil, fmt.Errorf("create preview list: %w", err)
	}

	return fields, rows, nil
}

func (e *Extractor) mixValuesIfNeeded(rows []interface{}, rndSeed int64) ([]interface{}, error) {
	if !e.config.MixValues || len(rows) < 2 {
		return rows, nil
	}

	var table [][]any
	for _, row := range rows {
		arr, ok := row.([]any)
		if !ok {
			return nil, fmt.Errorf("row %d is not a slice", row)
		}
		table = append(table, arr)
	}

	numRows := len(table)
	numColumns := len(table[0])

	rndGen := e.randFn(rndSeed)
	for col := 0; col < numColumns; col++ {
		for row := 0; row < numRows; row++ {
			randomRow := rndGen(int64(numRows))

			table[row][col], table[randomRow][col] = table[randomRow][col], table[row][col]
		}
	}

	mixedRows := make([]any, numRows)
	for i, row := range table {
		mixedRows[i] = row
	}
	return mixedRows, nil
}

func (e *Extractor) getColumnProfile(ctx context.Context, col *bigquery.FieldSchema, tm *bigquery.TableMetadata) (*v1beta2.ColumnProfile, error) {
	if col.Type == bigquery.BytesFieldType || col.Repeated || col.Type == bigquery.RecordFieldType {
		e.logger.Info("Skip profiling " + col.Name + " column")
		return nil, nil
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
	it.PageInfo().MaxSize = e.getMaxPageSize()

	// fetch first row for column profile result
	var row struct {
		Min    string  `bigquery:"min"`
		Max    string  `bigquery:"max"`
		Avg    float64 `bigquery:"avg"`
		Med    float64 `bigquery:"med"`
		Unique int64   `bigquery:"unique"`
		Count  int64   `bigquery:"count"`
		Top    string  `bigquery:"top"`
	}
	err = it.Next(&row)
	if err != nil && errors.Is(err, iterator.Done) {
		return nil, nil
	}

	// map row data to column profile
	return &v1beta2.ColumnProfile{
		Min:    row.Min,
		Max:    row.Max,
		Avg:    row.Avg,
		Med:    row.Med,
		Unique: row.Unique,
		Count:  row.Count,
		Top:    row.Top,
	}, nil
}

func (e *Extractor) buildColumnProfileQuery(col *bigquery.FieldSchema, tm *bigquery.TableMetadata) (*bigquery.Query, error) {
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
	var builder strings.Builder
	if err := temp.Execute(&builder, data); err != nil {
		return nil, err
	}

	finalQuery := builder.String()
	return e.client.Query(finalQuery), nil
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

		policyTagSplit := strings.Split(name, "/")
		if len(policyTagSplit) < 2 {
			e.logger.Error("error splitting policy tag ", "policy_tag", name, "err", "incorrect format")
			continue
		}

		taxonomyResourceName := strings.Join(policyTagSplit[:len(policyTagSplit)-2], "/")
		taxonomy, err := e.policyTagClient.GetTaxonomy(ctx, &datacatalogpb.GetTaxonomyRequest{Name: taxonomyResourceName})
		if err != nil {
			e.logger.Error("error fetching taxonomy", "taxonomy", taxonomy, "err", err)
			continue
		}

		pt = append(pt, fmt.Sprintf("%s:%s:%s", taxonomy.DisplayName, policyTag.DisplayName, policyTag.Name))
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

func (e *Extractor) fetchTableMetadata(ctx context.Context, tbl *bigquery.Table) (md *bigquery.TableMetadata, err error) {
	defer func(start time.Time) {
		attrs := []attribute.KeyValue{
			attribute.String("bq.operation", "table.metadata"),
			attribute.String("bq.project_id", tbl.ProjectID),
			attribute.String("bq.dataset_id", tbl.DatasetID),
		}
		if err != nil {
			attrs = append(attrs, attribute.String("bq.error_code", plugins.BQErrReason(err)))
		}

		e.tableDurn.Record(
			ctx, time.Since(start).Milliseconds(), metric.WithAttributes(attrs...),
		)
	}(time.Now())

	return tbl.Metadata(ctx)
}

func getViewQuery(md *bigquery.TableMetadata) string {
	switch md.Type {
	case bigquery.ViewTable:
		return md.ViewQuery
	case bigquery.MaterializedView:
		return md.MaterializedView.Query
	}
	return ""
}

func getUpstreamResources(query string) []*v1beta2.Resource {
	upstreamDependencies := upstream.ParseTopLevelUpstreamsFromQuery(query)
	uniqueUpstreamDependencies := upstream.UniqueFilterResources(upstreamDependencies)
	var upstreams []*v1beta2.Resource
	for _, dependency := range uniqueUpstreamDependencies {
		urn := plugins.BigQueryURN(dependency.Project, dependency.Dataset, dependency.Name)
		upstreams = append(upstreams, &v1beta2.Resource{
			Urn:     urn,
			Name:    dependency.Name,
			Type:    "table",
			Service: "bigquery",
		})
	}
	return upstreams
}

// Register the extractor to catalog
func init() {
	if err := registry.Extractors.Register("bigquery", func() plugins.Extractor {
		return New(plugins.GetLog(), CreateClient, seededRandom)
	}); err != nil {
		panic(err)
	}
}

func seededRandom(seed int64) func(max int64) int64 {
	rnd := rand.New(rand.NewSource(seed)) //nolint:gosec
	return func(max int64) int64 {
		return rnd.Int63n(max)
	}
}

func pickFirstNonZero(ints ...int) int {
	for _, intItem := range ints {
		if intItem != 0 {
			return intItem
		}
	}
	return 0
}

func handleOtelErr(err error) {
	if err != nil {
		otel.Handle(err)
	}
}
