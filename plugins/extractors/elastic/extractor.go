package elastic

import (
	"context"
	"encoding/json"
	"reflect"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/odpf/meteor/plugins"
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

func (e *Extractor) Extract(ctx context.Context, configMap map[string]interface{}, out chan<- interface{}) (err error) {
	e.out = out

	//build config
	var config Config
	err = utils.BuildConfig(configMap, &config)
	if err != nil {
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

	result, err := e.listIndexes(client)
	if err != nil {
		return
	}
	out <- result
	return
}

func (e *Extractor) listIndexes(client *elasticsearch.Client) (result []map[string]interface{}, err error) {
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
		row := make(map[string]interface{})
		row["index_name"] = x.Key().String()
		doc_properties, err1 := e.listIndexInfo(client, x.Key().String())
		if err1 != nil {
			err = err1
			return
		}
		row["document_properties"] = doc_properties
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
		row["document_count"] = len(t["hits"].(map[string]interface{})["hits"].([]interface{}))
		result = append(result, row)
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
		return &Extractor{
			logger: plugins.GetLog(),
		}
	}); err != nil {
		panic(err)
	}
}
