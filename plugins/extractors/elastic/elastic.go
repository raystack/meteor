package elastic

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/raystack/meteor/models"
	"github.com/raystack/meteor/plugins"
	"github.com/raystack/meteor/registry"
	log "github.com/raystack/salt/observability/logger"
)

//go:embed README.md
var summary string

type Config struct {
	User     string `json:"user" yaml:"user" mapstructure:"user"`
	Password string `json:"password" yaml:"password" mapstructure:"password"`
	Host     string `json:"host" yaml:"host" mapstructure:"host" validate:"required"`
}

var sampleConfig = `
user: "elastic"
password: "changeme"
host: elastic_server`

var info = plugins.Info{
	Description:  "Index metadata from Elasticsearch cluster.",
	SampleConfig: sampleConfig,
	Summary:      summary,
	Tags:         []string{"oss", "database"},
	Entities: []plugins.EntityInfo{
		{Type: "table", URNPattern: "urn:elasticsearch:{scope}:index:{index_name}"},
	},
}

// Extractor manages the extraction of data from elastic
type Extractor struct {
	plugins.BaseExtractor
	config Config
	logger log.Logger
	client *elasticsearch.Client
}

// New returns a pointer to an initialized Extractor Object
func New(logger log.Logger) *Extractor {
	e := &Extractor{
		logger: logger,
	}
	e.BaseExtractor = plugins.NewBaseExtractor(info, &e.config)

	return e
}

// Init initializes the extractor
func (e *Extractor) Init(ctx context.Context, config plugins.Config) (err error) {
	if err = e.BaseExtractor.Init(ctx, config); err != nil {
		return err
	}

	// build elasticsearch client
	cfg := elasticsearch.Config{
		Addresses: []string{
			e.config.Host,
		},
		Username: e.config.User,
		Password: e.config.Password,
	}
	if e.client, err = elasticsearch.NewClient(cfg); err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}

	return
}

// Extract extracts the data from the elastic server
// and collected through the emitter
func (e *Extractor) Extract(ctx context.Context, emit plugins.Emit) (err error) {
	res, err := e.client.Cluster.Health(
		e.client.Cluster.Health.WithLevel("indices"),
	)
	if err != nil {
		return fmt.Errorf("failed to fetch cluster information: %w", err)
	}
	var r map[string]any
	err = json.NewDecoder(res.Body).Decode(&r)
	if err != nil {
		return
	}
	x := reflect.ValueOf(r["indices"]).MapRange()
	for x.Next() {
		indexName := x.Key().String()
		docProperties, err1 := e.listIndexInfo(x.Key().String())
		if err1 != nil {
			err = err1
			return
		}
		var columns []any
		for i := range docProperties {
			columns = append(columns, map[string]any{
				"name":      i,
				"data_type": docProperties[i].(map[string]any)["type"].(string),
			})
		}
		countRes, err1 := e.client.Search(
			e.client.Search.WithIndex(x.Key().String()),
		)
		if err1 != nil {
			err = err1
			return
		}
		var t map[string]any
		err = json.NewDecoder(countRes.Body).Decode(&t)
		if err != nil {
			res.Body.Close()
			return
		}
		docCount := len(t["hits"].(map[string]any)["hits"].([]any))
		props := map[string]any{
			"columns": columns,
		}
		if docCount > 0 {
			props["profile"] = map[string]any{
				"total_rows": int64(docCount),
			}
		}

		// Enrich with index settings (shards, replicas).
		settings, err := e.getIndexSettings(indexName)
		if err == nil {
			for k, v := range settings {
				props[k] = v
			}
		}

		emit(models.NewRecord(models.NewEntity(
			models.NewURN("elasticsearch", e.UrnScope, "index", indexName),
			"table", indexName, "elasticsearch",
			props,
		)))
	}
	return
}

// getIndexSettings retrieves settings like number of shards and replicas.
func (e *Extractor) getIndexSettings(index string) (map[string]any, error) {
	res, err := e.client.Indices.GetSettings(
		e.client.Indices.GetSettings.WithIndex(index),
	)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	var r map[string]any
	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		return nil, err
	}

	result := make(map[string]any)
	indexData, ok := r[index].(map[string]any)
	if !ok {
		return result, nil
	}
	settings, ok := indexData["settings"].(map[string]any)
	if !ok {
		return result, nil
	}
	indexSettings, ok := settings["index"].(map[string]any)
	if !ok {
		return result, nil
	}

	if v, ok := indexSettings["number_of_shards"]; ok {
		result["number_of_shards"] = v
	}
	if v, ok := indexSettings["number_of_replicas"]; ok {
		result["number_of_replicas"] = v
	}

	return result, nil
}

// listIndexInfo returns the properties of the index
func (e *Extractor) listIndexInfo(index string) (result map[string]any, err error) {
	var r map[string]any
	res, err := e.client.Indices.GetMapping(
		e.client.Indices.GetMapping.WithIndex(index),
	)
	if err != nil {
		err = fmt.Errorf("failed to retrieve index: %w", err)
		return
	}
	err = json.NewDecoder(res.Body).Decode(&r)
	if err != nil {
		res.Body.Close()
		return
	}
	result = r[index].(map[string]any)["mappings"].(map[string]any)["properties"].(map[string]any)
	_ = res.Body.Close()
	return
}

// init registers the extractor to catalog
func init() {
	if err := registry.Extractors.Register("elastic", func() plugins.Extractor {
		return New(plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}
