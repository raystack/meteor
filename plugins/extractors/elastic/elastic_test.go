//+build integration

package elastic_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"testing"

	"github.com/elastic/go-elasticsearch/esapi"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/odpf/meteor/models"
	"github.com/odpf/meteor/models/odpf/assets"
	"github.com/odpf/meteor/models/odpf/assets/common"
	"github.com/odpf/meteor/models/odpf/assets/facets"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/plugins/extractors/elastic"
	"github.com/odpf/meteor/test"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/assert"
)

const (
	host = "http://localhost:9200"
	pass = "secret_pass"
	user = "elastic_meteor"
)

var (
	client *elasticsearch.Client
	ctx    = context.Background()
)

func TestMain(m *testing.M) {
	cfg := elasticsearch.Config{
		Addresses: []string{
			host,
		},
		Username: user,
		Password: pass,
	}
	var err error
	client, err = elasticsearch.NewClient(cfg)
	if err != nil {
		log.Fatal(err)
	}

	// setup test
	opts := dockertest.RunOptions{
		Repository: "elasticsearch",
		Tag:        "7.13.2",
		Env: []string{
			"discovery.type=single-node",
			"ES_JAVA_OPTS=-Xms512m -Xmx512m",
			"ELASTIC_USER=" + user,
			"ELASTIC_PASSWORD=" + pass,
			"xpack.security.enabled=false",
		},
		ExposedPorts: []string{"9200"},
		PortBindings: map[docker.Port][]docker.PortBinding{
			"9200": {
				{HostIP: "0.0.0.0", HostPort: "9200"},
			},
		},
	}
	// exponential backoff-retry, because the application in the container might not be ready to accept connections yet
	retryFn := func(resource *dockertest.Resource) (err error) {
		res, err := client.Cluster.Health(
			client.Cluster.Health.WithLevel("indices"),
		)
		if err != nil {
			return
		}
		if res.StatusCode != http.StatusOK {
			return fmt.Errorf("received %d status code", res.StatusCode)
		}
		return
	}
	purgeFn, err := test.CreateContainer(opts, retryFn)
	if err != nil {
		log.Fatal(err)
	}
	if err := setup(); err != nil {
		log.Fatal(err)
	}

	// run tests
	code := m.Run()

	// clean tests
	if err := purgeFn(); err != nil {
		log.Fatal(err)
	}
	os.Exit(code)
}

func TestExtract(t *testing.T) {

	t.Run("should return error if no host in config", func(t *testing.T) {
		err := newExtractor().Extract(context.TODO(), map[string]interface{}{
			"password": "pass",
		}, make(chan<- models.Record))
		assert.Equal(t, plugins.InvalidConfigError{}, err)
	})

	t.Run("should return mockdata we generated with service running on localhost", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		extractOut := make(chan models.Record)
		go func() {
			err := newExtractor().Extract(ctx, map[string]interface{}{
				"host":     host,
				"user":     user,
				"password": pass,
			}, extractOut)
			close(extractOut)

			assert.Nil(t, err)
		}()
		var results []*assets.Table
		for d := range extractOut {
			table, ok := d.Data().(*assets.Table)
			if !ok {
				t.Fatal(errors.New("invalid table format"))
			}

			results = append(results, table)
		}

		assert.Equal(t, getExpectedVal(), results)
	})
}

type MeteorMockElasticDocs struct {
	SomeStr string
	SomeInt int
}

func setup() (err error) {
	doc1 := MeteorMockElasticDocs{}
	doc1.SomeStr = "Value1"
	doc1.SomeInt = 1

	doc2 := MeteorMockElasticDocs{}
	doc2.SomeStr = "Value2"
	doc2.SomeInt = 2
	docStr1 := jsonStruct(doc1)
	docStr2 := jsonStruct(doc2)
	err = populateElasticSearch("index1", "1", docStr1)
	if err != nil {
		return
	}
	err = populateElasticSearch("index2", "1", docStr2)
	if err != nil {
		return
	}
	return
}

func populateElasticSearch(index string, id string, data string) error {
	req := esapi.IndexRequest{
		Index:      index,
		DocumentID: id,
		Body:       strings.NewReader(data),
		Refresh:    "true",
	}
	res, err := req.Do(ctx, client)
	if err != nil {
		return err
	}
	res.Body.Close()
	return err
}

func jsonStruct(doc MeteorMockElasticDocs) string {
	docStruct := &MeteorMockElasticDocs{
		SomeStr: doc.SomeStr,
		SomeInt: doc.SomeInt,
	}
	b, err := json.Marshal(docStruct)
	if err != nil {
		fmt.Println("json.Marshal ERROR:", err)
		return string(err.Error())
	}
	return string(b)
}

func newExtractor() *elastic.Extractor {
	return elastic.New(test.Logger)
}

func getExpectedVal() []*assets.Table {
	return []*assets.Table{
		{
			Resource: &common.Resource{
				Urn:  "elasticsearch.index1",
				Name: "index1",
			},
			Schema: &facets.Columns{
				Columns: []*facets.Column{
					{
						Name:     "SomeInt",
						DataType: "long",
					},
					{
						Name:     "SomeStr",
						DataType: "text",
					},
				},
			},
			Profile: &assets.TableProfile{
				TotalRows: 1,
			},
		},
		{
			Resource: &common.Resource{
				Urn:  "elastic.index2",
				Name: "index2",
			},
			Schema: &facets.Columns{
				Columns: []*facets.Column{
					{
						Name:     "SomeInt",
						DataType: "long",
					},
					{
						Name:     "SomeStr",
						DataType: "text",
					},
				},
			},
			Profile: &assets.TableProfile{
				TotalRows: 1,
			},
		},
	}
}
