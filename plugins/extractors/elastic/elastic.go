package elastic

import (
	"context"
	_ "embed"
	"encoding/json"
	"reflect"

	"github.com/pkg/errors"

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
	Description:  "Search engine based on the Lucene library.",
	SampleConfig: sampleConfig,
	Summary:      summary,
	Tags:         []string{"oss", "extractor"},
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
		return errors.Wrap(err, "failed to create client")
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
		return errors.Wrap(err, "failed to fetch cluster information")
	}
	var r map[string]interface{}
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
		var columns []interface{}
		for i := range docProperties {
			columns = append(columns, map[string]interface{}{
				"name":      i,
				"data_type": docProperties[i].(map[string]interface{})["type"].(string),
			})
		}
		countRes, err1 := e.client.Search(
			e.client.Search.WithIndex(x.Key().String()),
		)
		if err1 != nil {
			err = err1
			return
		}
		var t map[string]interface{}
		err = json.NewDecoder(countRes.Body).Decode(&t)
		if err != nil {
			res.Body.Close()
			return
		}
		docCount := len(t["hits"].(map[string]interface{})["hits"].([]interface{}))
		props := map[string]interface{}{
			"columns": columns,
		}
		if docCount > 0 {
			props["profile"] = map[string]interface{}{
				"total_rows": int64(docCount),
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

// listIndexInfo returns the properties of the index
func (e *Extractor) listIndexInfo(index string) (result map[string]interface{}, err error) {
	var r map[string]interface{}
	res, err := e.client.Indices.GetMapping(
		e.client.Indices.GetMapping.WithIndex(index),
	)
	if err != nil {
		err = errors.Wrap(err, "failed to retrieve index")
		return
	}
	err = json.NewDecoder(res.Body).Decode(&r)
	if err != nil {
		res.Body.Close()
		return
	}
	result = r[index].(map[string]interface{})["mappings"].(map[string]interface{})["properties"].(map[string]interface{})
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
