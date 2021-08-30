package elastic

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/proto/odpf/assets"
	"github.com/odpf/meteor/proto/odpf/assets/common"
	"github.com/odpf/meteor/proto/odpf/assets/facets"
	"github.com/odpf/meteor/registry"
	"github.com/odpf/meteor/utils"
	"github.com/odpf/salt/log"
)

type Config struct {
	User     string `mapstructure:"user_id"`
	Password string `mapstructure:"password"`
	Host     string `mapstructure:"host" validate:"required"`
}

var (
	configInfo = ``
	inputInfo  = `
Input:
 _____________________________________________________________________________________________
| Key             | Example          | Description                               |            |
|_________________|__________________|___________________________________________|____________|
| "host"          | "localhost:9200" | The Host at which server is running       | *required* |
| "user_id"       | "admin"          | User ID to access the elastic server      | *optional* |
| "password"      | "1234"           | Password for the elastic Server           | *optional* |
|_________________|__________________|___________________________________________|____________|
`
	outputInfo = `
Output:
 _____________________________________________
|Field               |Sample Value            |
|____________________|________________________|
|"resource.urn"      |"elasticsearch.index1"  |
|"resource.name"     |"index1"                |
|"resource.service"  |"elastic"               |
|"profile.total_rows"|"1"                     |
|"schema"            |[]Column                |
|____________________|________________________|`
)

type Extractor struct {
	out    chan<- interface{}
	logger log.Logger
}

func New(logger log.Logger) *Extractor {
	return &Extractor{
		logger: logger,
	}
}

func (e *Extractor) GetDescription() string {
	return inputInfo + outputInfo
}

func (e *Extractor) GetSampleConfig() string {
	return configInfo
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

		e.out <- assets.Table{
			Resource: &common.Resource{
				Urn:  fmt.Sprintf("%s.%s", "elasticsearch", indexName),
				Name: indexName,
			},
			Schema: &facets.Columns{
				Columns: columns,
			},
			Profile: &assets.TableProfile{
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
