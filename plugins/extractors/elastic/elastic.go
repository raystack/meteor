package elastic

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/proto/odpf/entities/facets"
	"github.com/odpf/meteor/proto/odpf/entities/resources"
	"github.com/odpf/meteor/registry"
	"github.com/odpf/meteor/utils"
	"github.com/odpf/salt/log"
)

type Config struct {
	User     string `mapstructure:"user"`
	Password string `mapstructure:"password"`
	Host     string `mapstructure:"host" validate:"required"`
}

type Extractor struct {
	out    chan<- interface{}
	logger log.Logger
}

func New(logger log.Logger) *Extractor {
	return &Extractor{
		logger: logger,
	}
}

func (e *Extractor) Extract(ctx context.Context, configMap map[string]interface{}, out chan<- interface{}) (err error) {
	e.out = out

	//build config
	var config Config
	err = utils.BuildConfig(configMap, &config)
	if err != nil {
		// fmt.Println("1.25")
		return plugins.InvalidConfigError{}
	}

	//build elasticsearch client
	cfg := elasticsearch.Config{
		Addresses: []string{
			config.Host,
		},
		Username: config.User,
		Password: config.Password,
	}
	client, err := elasticsearch.NewClient(cfg)
	if err != nil {
		return
	}

	err = e.extractIndexes(client)
	if err != nil {
		return
	}
	return
}

func (e *Extractor) extractIndexes(client *elasticsearch.Client) (err error) {
	res, err := client.Cluster.Health(
		client.Cluster.Health.WithLevel("indices"),
	)
	if err != nil {
		return
	}
	var r map[string]interface{}
	err = json.NewDecoder(res.Body).Decode(&r)
	if err != nil {
		return
	}
	x := reflect.ValueOf(r["indices"]).MapRange()
	for x.Next() {
		indexName := x.Key().String()
		docProperties, err1 := e.listIndexInfo(client, x.Key().String())
		if err1 != nil {
			err = err1
			return
		}
		var columns []*facets.Column
		for i := range docProperties {
			columns = append(columns, &facets.Column{
				Name:     i,
				DataType: docProperties[i].(map[string]interface{})["type"].(string),
			})
		}
		countRes, err1 := client.Search(
			client.Search.WithIndex(x.Key().String()),
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

		e.out <- resources.Table{
			Urn:  fmt.Sprintf("%s.%s", "elasticsearch", indexName),
			Name: indexName,
			Schema: &facets.Columns{
				Columns: columns,
			},
			Profile: &resources.TableProfile{
				TotalRows: int64(docCount),
			},
		}
	}
	return
}

func (e *Extractor) listIndexInfo(client *elasticsearch.Client, index string) (result map[string]interface{}, err error) {
	var r map[string]interface{}
	res, err := client.Indices.GetMapping(
		client.Indices.GetMapping.WithIndex(index),
	)
	if err != nil {
		return
	}
	err = json.NewDecoder(res.Body).Decode(&r)
	if err != nil {
		res.Body.Close()
		return
	}
	result = r[index].(map[string]interface{})["mappings"].(map[string]interface{})["properties"].(map[string]interface{})
	res.Body.Close()
	return
}

// Register the extractor to catalog
func init() {
	if err := registry.Extractors.Register("elastic", func() plugins.Extractor {
		return New(plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}
