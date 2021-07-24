package bigquery

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"

	"text/template"

	"cloud.google.com/go/bigquery"
	"github.com/mitchellh/mapstructure"
	"github.com/odpf/meteor/core/extractor"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/proto/odpf/meta"
	"github.com/odpf/meteor/proto/odpf/meta/facets"
	"github.com/odpf/meteor/utils"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

type Config struct {
	ProjectID          string `mapstructure:"project_id" validate:"required"`
	ServiceAccountJSON string `mapstructure:"service_account_json"`
	TablePattern       string `mapstructure:"table_pattern"`
	ProfileColumn      bool   `mapstructure:"profile_column"`
}

type Extractor struct {
	logger plugins.Logger
	client *bigquery.Client
	ctx    context.Context
}

func New(logger plugins.Logger, client *bigquery.Client, ctx context.Context) extractor.TableExtractor {
	return &Extractor{
		logger: logger,
		client: client,
		ctx:    ctx,
	}
}

func (e *Extractor) Extract(configMap map[string]interface{}) (result []meta.Table, err error) {
	e.logger.Info("extracting bigquery metadata...")
	var config Config
	err = utils.BuildConfig(configMap, &config)
	if err != nil {
		return result, extractor.InvalidConfigError{}
	}
	err = e.validateConfig(config)
	if err != nil {
		return
	}

	if e.ctx == nil {
		e.ctx = context.Background()
	}

	if e.client == nil {
		e.client, err = e.createClient(config)
		if err != nil {
			return
		}
	}
	result, err = e.getMetadata(config)
	if err != nil {
		return
	}

	return
}

func (e *Extractor) getMetadata(config Config) (results []meta.Table, err error) {
	it := e.client.Datasets(e.ctx)

	dataset, err := it.Next()
	for err == nil {
		results, err = e.appendTablesMetadata(results, dataset, config)
		if err != nil {
			return
		}

		dataset, err = it.Next()
	}
	if err == iterator.Done {
		err = nil
	}

	return
}

func (e *Extractor) appendTablesMetadata(results []meta.Table, dataset *bigquery.Dataset, config Config) ([]meta.Table, error) {
	it := dataset.Tables(e.ctx)

	table, err := it.Next()
	for err == nil {
		if config.TablePattern != "" {
			fullTableID := fmt.Sprintf("%s.%s", table.DatasetID, table.TableID)
			res, _ := regexp.MatchString(config.TablePattern, fullTableID)
			if res {
				tableResult, err := e.mapTable(table, config.ProfileColumn)
				if err == nil {
					results = append(results, tableResult)
				}
			}
		}
		table, err = it.Next()
	}
	if err == iterator.Done {
		err = nil
	}

	return results, err
}

func (e *Extractor) mapTable(t *bigquery.Table, profileColumn bool) (result meta.Table, err error) {
	tableMetadata, err := e.client.Dataset(t.DatasetID).Table(t.TableID).Metadata(e.ctx)
	result = meta.Table{
		Urn:         fmt.Sprintf("%s.%s.%s", t.ProjectID, t.DatasetID, t.TableID),
		Name:        t.TableID,
		Source:      "bigquery",
		Description: t.DatasetID,
		Schema:      e.extractSchema(tableMetadata.Schema, tableMetadata, profileColumn),
	}
	return result, err
}

func (e *Extractor) extractSchema(col []*bigquery.FieldSchema, t *bigquery.TableMetadata, profileColumn bool) (columns *facets.Columns) {
	var columnList []*facets.Column
	var wg sync.WaitGroup
	var mu sync.Mutex
	wg.Add(len(col))
	for _, b := range col {
		go func(s *bigquery.FieldSchema) {
			defer wg.Done()
			column := e.mapColumn(s, t, profileColumn)
			mu.Lock()
			columnList = append(columnList, column)
			mu.Unlock()
		}(b)
	}
	wg.Wait()
	return &facets.Columns{
		Columns: columnList,
	}
}

func (e *Extractor) mapColumn(col *bigquery.FieldSchema, t *bigquery.TableMetadata, profileColumn bool) *facets.Column {
	var columnProfile *facets.ColumnProfile
	if profileColumn {
		columnProfile, _ = e.findColumnProfile(col, t)
	}
	return &facets.Column{
		Name:        col.Name,
		Description: col.Description,
		DataType:    string(col.Type),
		IsNullable:  !(col.Required || col.Repeated),
		Profile:     columnProfile,
	}
}

func (e *Extractor) findColumnProfile(col *bigquery.FieldSchema, t *bigquery.TableMetadata) (*facets.ColumnProfile, error) {
	if col.Type == bigquery.BytesFieldType || col.Repeated || col.Type == bigquery.RecordFieldType {
		e.logger.Info("Skip profiling " + col.Name + " column")
		return nil, nil
	}
	rows, err := e.profileTheColumn(col, t)
	if err != nil {
		e.logger.Error(err)
		return nil, err
	}
	result, err := e.getResult(rows)

	return &facets.ColumnProfile{
		Min:    result.Min,
		Max:    result.Max,
		Avg:    result.Avg,
		Med:    result.Med,
		Unique: result.Unique,
		Count:  result.Count,
		Top:    result.Top,
	}, err
}

func (e *Extractor) profileTheColumn(col *bigquery.FieldSchema, t *bigquery.TableMetadata) (*bigquery.RowIterator, error) {
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
		"TableName":  strings.ReplaceAll(t.FullID, ":", "."),
	}

	temp := template.Must(template.New("query").Parse(queryTemplate))
	builder := &strings.Builder{}
	if err := temp.Execute(builder, data); err != nil {
		panic(err)
	}
	finalQuery := builder.String()
	query := e.client.Query(finalQuery)
	return query.Read(e.ctx)
}

type ResultRow struct {
	Min    string  `bigquery:"min"`
	Max    string  `bigquery:"max"`
	Avg    float32 `bigquery:"avg"`
	Med    float32 `bigquery:"med"`
	Unique int64   `bigquery:"unique"`
	Count  int64   `bigquery:"count"`
	Top    string  `bigquery:"top"`
}

func (e *Extractor) getResult(iter *bigquery.RowIterator) (ResultRow, error) {
	var row ResultRow
	err := iter.Next(&row)
	if err == iterator.Done {
		return row, nil
	}
	if err != nil {
		return row, fmt.Errorf("error iterating through results: %v", err)
	}

	return row, err
}

func (e *Extractor) createClient(config Config) (*bigquery.Client, error) {
	if config.ServiceAccountJSON == "" {
		e.logger.Info("credentials are not specified, creating bigquery client using Default Credentials...")
		return bigquery.NewClient(e.ctx, config.ProjectID)
	}

	return bigquery.NewClient(e.ctx, config.ProjectID, option.WithCredentialsJSON([]byte(config.ServiceAccountJSON)))
}

func (e *Extractor) getConfig(configMap map[string]interface{}) (config Config, err error) {
	err = mapstructure.Decode(configMap, &config)
	if err != nil {
		return
	}

	return
}

func (e *Extractor) validateConfig(config Config) (err error) {
	if config.ProjectID == "" {
		return errors.New("project_id is required")
	}

	return
}
