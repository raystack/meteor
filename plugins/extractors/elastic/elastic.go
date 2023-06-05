package elastic

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"io"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/goto/meteor/models"
	v1beta2 "github.com/goto/meteor/models/gotocompany/assets/v1beta2"
	"github.com/goto/meteor/plugins"
	"github.com/goto/meteor/registry"
	"github.com/goto/salt/log"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"
)

// init registers the extractor to catalog
func init() {
	if err := registry.Extractors.Register("elastic", func() plugins.Extractor {
		return New(plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}

//go:embed README.md
var summary string

type Config struct {
	User     string `mapstructure:"user"`
	Password string `mapstructure:"password"`
	Host     string `mapstructure:"host" validate:"required"`
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
func (e *Extractor) Init(ctx context.Context, config plugins.Config) error {
	if err := e.BaseExtractor.Init(ctx, config); err != nil {
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
	var err error
	if e.client, err = elasticsearch.NewClient(cfg); err != nil {
		return fmt.Errorf("create client: %w", err)
	}

	return nil
}

// Extract extracts the data from the elastic server
// and collected through the emitter
func (e *Extractor) Extract(ctx context.Context, emit plugins.Emit) error {
	res, err := e.client.Cluster.Health(
		e.client.Cluster.Health.WithLevel("indices"),
		e.client.Cluster.Health.WithContext(ctx),
	)
	if err != nil {
		return fmt.Errorf("fetch cluster info: %w", err)
	}
	defer drainBody(res)
	var r struct {
		Indices map[string]interface{} `json:"indices"`
	}
	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		return fmt.Errorf("decode cluster info: %w", err)
	}
	for indexName := range r.Indices {
		if err := e.buildAsset(ctx, indexName, emit); err != nil {
			return err
		}
	}
	return nil
}

func (e *Extractor) buildAsset(ctx context.Context, indexName string, emit plugins.Emit) error {
	docProperties, err := e.listIndexInfo(indexName)
	if err != nil {
		return err
	}
	var columns []*v1beta2.Column
	for field, fieldMapping := range docProperties {
		var typ string
		if s, ok := fieldMapping["type"].(string); ok {
			typ = s
		}
		columns = append(columns, &v1beta2.Column{
			Name:     field,
			DataType: typ,
		})
	}

	countRes, err := e.client.Search(
		e.client.Search.WithIndex(indexName),
		e.client.Search.WithContext(ctx),
	)
	if err != nil {
		return err
	}
	defer drainBody(countRes)

	var t struct {
		Hits struct {
			Hits []interface{} `json:"hits"`
		} `json:"hits"`
	}
	if err := json.NewDecoder(countRes.Body).Decode(&t); err != nil {
		return err
	}

	docCount := len(t.Hits.Hits)
	table, err := anypb.New(&v1beta2.Table{
		Columns:    columns,
		Attributes: &structpb.Struct{}, // ensure attributes don't get overwritten if present
		Profile: &v1beta2.TableProfile{
			TotalRows: int64(docCount),
		},
	})
	if err != nil {
		return fmt.Errorf("create Any struct for table: %w", err)
	}

	emit(models.NewRecord(&v1beta2.Asset{
		Urn:     models.NewURN("elasticsearch", e.UrnScope, "index", indexName),
		Name:    indexName,
		Type:    "table",
		Service: "elasticsearch",
		Data:    table,
	}))
	return nil
}

// listIndexInfo returns the properties of the index
func (e *Extractor) listIndexInfo(index string) (map[string]map[string]interface{}, error) {
	res, err := e.client.Indices.GetMapping(
		e.client.Indices.GetMapping.WithIndex(index),
	)
	if err != nil {
		return nil, fmt.Errorf("retrieve index: %w", err)
	}
	defer drainBody(res)

	var r map[string]struct {
		Mappings struct {
			Properties map[string]map[string]interface{} `json:"properties"`
		} `json:"mappings"`
	}
	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		return nil, err
	}

	return r[index].Mappings.Properties, nil
}

// drainBody drains and closes the response body to avoid the following
// gotcha:
// http://devs.cloudimmunity.com/gotchas-and-common-mistakes-in-go-golang/index.html#close_http_resp_body
func drainBody(resp *esapi.Response) {
	if resp == nil {
		return
	}

	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
}
