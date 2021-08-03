package bigquery

import (
	"context"
	"fmt"
	"html/template"
	"strings"
	"sync"

	"cloud.google.com/go/bigquery"
	"github.com/odpf/meteor/core"
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
	cfg    Config
}

func (e *Extractor) Extract(ctx context.Context, config map[string]interface{}, out chan<- interface{}) (err error) {
	e.logger.Info("extracting table metadata from big query table")

	var cfg Config
	err = utils.BuildConfig(config, &cfg)

	if err != nil {
		return extractor.InvalidConfigError{}
	}

	client, err := e.createClient(ctx, cfg)

	e.client = client
	e.ctx = ctx
	e.cfg = cfg

	if err != nil {
		e.logger.Error(err)
		return
	}

	// Fetch and iterate over datesets
	it := client.Datasets(ctx)
	for {
		ds, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			e.logger.Error(err)
			continue
		}

		// Fetch and iterate over tables
		tb := ds.Tables(ctx)
		for {
			table, err := tb.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				e.logger.Error(err)
				continue
			}
			out <- e.fetchMetadata(table)
		}
	}

	return

}

// Create big query client
func (e *Extractor) createClient(ctx context.Context, config Config) (*bigquery.Client, error) {
	if config.ServiceAccountJSON == "" {
		e.logger.Info("credentials are not specified, creating bigquery client using default credentials...")
		return bigquery.NewClient(ctx, config.ProjectID)
	}

	return bigquery.NewClient(ctx, config.ProjectID, option.WithCredentialsJSON([]byte(config.ServiceAccountJSON)))
}

// Build the bigquery table metadata
func (e *Extractor) fetchMetadata(t *bigquery.Table) meta.Table {
	return meta.Table{
		Urn:         fmt.Sprintf("%s.%s.%s", t.ProjectID, t.DatasetID, t.TableID),
		Name:        t.TableID,
		Source:      "bigquery",
		Description: t.DatasetID,
		Schema:      e.extractSchema(t),
	}
}

// Extract table schema
func (e *Extractor) extractSchema(t *bigquery.Table) (columns *facets.Columns) {

	mt, _ := t.Metadata(e.ctx)
	col := mt.Schema
	var columnList []*facets.Column
	var wg sync.WaitGroup
	var mu sync.Mutex

	wg.Add(len(col))
	for _, b := range col {
		go func(s *bigquery.FieldSchema) {
			defer wg.Done()
			column := e.mapColumn(s, mt)
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

func (e *Extractor) mapColumn(col *bigquery.FieldSchema, mt *bigquery.TableMetadata) *facets.Column {
	var columnProfile *facets.ColumnProfile
	if e.cfg.ProfileColumn {
		columnProfile, _ = e.findColumnProfile(col, mt)
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

// Register the extractor to catalog
func init() {
	if err := extractor.Catalog.Register("bigquery", func() core.Extractor {
		return &Extractor{
			logger: plugins.Log,
		}
	}); err != nil {
		panic(err)
	}
}
