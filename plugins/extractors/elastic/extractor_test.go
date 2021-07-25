// +build integration

package elastic_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/elastic/go-elasticsearch/esapi"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/odpf/meteor/plugins/extractors/elastic"
	"github.com/stretchr/testify/assert"
)

const host = "http://localhost:9200"

type MeteorMockElasticDocs struct {
	SomeStr string
	SomeInt int
}

func TestExtract(t *testing.T) {

	t.Run("should return error if no host in config", func(t *testing.T) {
		extractor := new(elastic.Extractor)
		_, err := extractor.Extract(map[string]interface{}{})

		assert.NotNil(t, err)
	})

	t.Run("should return mockdata we generated with service running on localhost", func(t *testing.T) {

		extr, _ := extractor.Catalog.Get("elastic")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		extractOut := make(chan interface{})

		cfg := elasticsearch.Config{
			Addresses: []string{
				"http://localhost:9200",
			},
		}
		client, err := elasticsearch.NewClient(cfg)
		if err != nil {
			t.Fatal(err)
		}
		_, err = client.Info()
		if err != nil {
			t.Fatal(err)
		}
		err = mockDataGenerator(client)
		if err != nil {
			t.Fatal(err)
		}
		defer cleanUp(client)
		go func() {
			err := extr.Extract(ctx, map[string]interface{}{
				"host": host,
			}, extractOut)
			if err != nil {
				t.Fatal(err)
			}
			expected := getExpectedVal()
			assert.Equal(t, expected, result)
		}()
	})
}

func getExpectedVal() (expected []map[string]interface{}) {
	expected = []map[string]interface{}{
		{
			"document_count": 1,
			"document_properties": map[string]interface{}{
				"SomeInt": map[string]interface{}{
					"type": "long",
				},
				"SomeStr": map[string]interface{}{
					"fields": map[string]interface{}{
						"keyword": map[string]interface{}{
							"ignore_above": float64(256), "type": "keyword"},
					},
					"type": "text",
				},
			},
			"index_name": "index1",
		},
		{
			"document_count": 1,
			"document_properties": map[string]interface{}{
				"SomeInt": map[string]interface{}{
					"type": "long",
				},
				"SomeStr": map[string]interface{}{
					"fields": map[string]interface{}{
						"keyword": map[string]interface{}{
							"ignore_above": float64(256), "type": "keyword"},
					},
					"type": "text",
				},
			},
			"index_name": "index2",
		},
	}
	return
}

func mockDataGenerator(client *elasticsearch.Client) (err error) {
	ctx := context.Background()
	doc1 := MeteorMockElasticDocs{}
	doc1.SomeStr = "Value1"
	doc1.SomeInt = 1

	doc2 := MeteorMockElasticDocs{}
	doc2.SomeStr = "Value2"
	doc2.SomeInt = 2
	docStr1 := jsonStruct(doc1)
	docStr2 := jsonStruct(doc2)
	err = populateElasticSearch(client, ctx, "index1", "1", docStr1)
	if err != nil {
		return
	}
	err = populateElasticSearch(client, ctx, "index2", "1", docStr2)
	if err != nil {
		return
	}
	return
}

func populateElasticSearch(client *elasticsearch.Client, ctx context.Context, index string, id string, data string) error {
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

func cleanUp(client *elasticsearch.Client) {
	client.Indices.Delete([]string{"index1", "index2"})
}
