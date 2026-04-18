//go:build plugins
// +build plugins

package elastic_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"testing"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/raystack/meteor/models"
	meteorv1beta1 "github.com/raystack/meteor/models/raystack/meteor/v1beta1"
	"github.com/raystack/meteor/plugins"
	"github.com/raystack/meteor/plugins/extractors/elastic"
	"github.com/raystack/meteor/test/mocks"
	"github.com/raystack/meteor/test/utils"
	"github.com/stretchr/testify/assert"
)

const (
	pass     = "secret_pass"
	user     = "elastic_meteor"
	urnScope = "test-elasticsearch"
)

var (
	host            string
	client          *elasticsearch.Client
	ctx             = context.TODO()
	dockerAvailable bool
)

func TestMain(m *testing.M) {
	dockerAvailable = utils.CheckDockerAvailability()
	if !dockerAvailable {
		os.Exit(m.Run())
	}

	// setup test
	opts := dockertest.RunOptions{
		Repository: "elasticsearch",
		Tag:        "8.17.0",
		Env: []string{
			"discovery.type=single-node",
			"ES_JAVA_OPTS=-Xms512m -Xmx512m",
			"ELASTIC_USER=" + user,
			"ELASTIC_PASSWORD=" + pass,
			"xpack.security.enabled=false",
		},
		ExposedPorts: []string{"9200"},
		PortBindings: map[docker.Port][]docker.PortBinding{"9200": {{HostPort: "0"}}},
	}
	// exponential backoff-retry, because the application in the container might not be ready to accept connections yet
	retryFn := func(resource *dockertest.Resource) (err error) {
		host = "http://" + resource.GetHostPort("9200/tcp")
		cfg := elasticsearch.Config{
			Addresses: []string{host},
			Username:  user,
			Password:  pass,
		}
		client, err = elasticsearch.NewClient(cfg)
		if err != nil {
			return
		}
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
	purgeFn, err := utils.CreateContainer(opts, retryFn)
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

func TestInit(t *testing.T) {
	utils.SkipIfNoDocker(t, dockerAvailable)
	t.Run("should return error if no host in config", func(t *testing.T) {
		err := newExtractor().Init(ctx, plugins.Config{
			URNScope: urnScope,
			RawConfig: map[string]any{
				"password": "pass",
			},
		})
		assert.ErrorAs(t, err, &plugins.InvalidConfigError{})
	})
}

func TestExtract(t *testing.T) {
	utils.SkipIfNoDocker(t, dockerAvailable)
	t.Run("should return mockdata we generated with service running on localhost", func(t *testing.T) {
		extr := newExtractor()
		err := extr.Init(ctx, plugins.Config{
			URNScope: urnScope,
			RawConfig: map[string]any{
				"host":     host,
				"user":     user,
				"password": pass,
			},
		})
		if err != nil {
			t.Fatal(err)
		}

		emitter := mocks.NewEmitter()
		err = extr.Extract(ctx, emitter.Push)
		assert.NoError(t, err)
		utils.AssertEqualProtos(t, getExpectedVal(t), utils.SortedEntities(emitter.GetAllEntities()))
	})
}

type MeteorMockElasticDocs struct {
	SomeStr string
}

func setup() (err error) {
	doc1 := MeteorMockElasticDocs{}
	doc1.SomeStr = "Value1"

	doc2 := MeteorMockElasticDocs{}
	doc2.SomeStr = "Value2"
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
	}
	b, err := json.Marshal(docStruct)
	if err != nil {
		fmt.Println("json.Marshal ERROR:", err)
		return string(err.Error())
	}
	return string(b)
}

func newExtractor() *elastic.Extractor {
	return elastic.New(utils.Logger)
}

func getExpectedVal(t *testing.T) []*meteorv1beta1.Entity {
	return []*meteorv1beta1.Entity{
		models.NewEntity("urn:elasticsearch:test-elasticsearch:index:index1", "table", "index1", "elasticsearch", map[string]any{
			"columns": []any{
				map[string]any{"name": "SomeStr", "data_type": "text"},
			},
			"profile": map[string]any{
				"total_rows": float64(1),
			},
		}),
		models.NewEntity("urn:elasticsearch:test-elasticsearch:index:index2", "table", "index2", "elasticsearch", map[string]any{
			"columns": []any{
				map[string]any{"name": "SomeStr", "data_type": "text"},
			},
			"profile": map[string]any{
				"total_rows": float64(1),
			},
		}),
	}
}
