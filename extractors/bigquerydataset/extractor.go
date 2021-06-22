package bigquerydataset

import (
	"context"
	"errors"

	"cloud.google.com/go/bigquery"
	"github.com/mitchellh/mapstructure"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

type Config struct {
	ProjectID          string `mapstructure:"project_id"`
	ServiceAccountJSON string `mapstructure:"service_account_json"`
}

type Extractor struct{}

func (e *Extractor) Extract(configMap map[string]interface{}) (result []map[string]interface{}, err error) {
	config, err := e.getConfig(configMap)
	if err != nil {
		return
	}
	err = e.validateConfig(config)
	if err != nil {
		return
	}

	ctx := context.Background()
	client, err := e.createClient(ctx, config)
	if err != nil {
		return
	}
	datasets, err := e.getDatasets(ctx, client)
	if err != nil {
		return
	}
	result = e.mapDatasets(datasets)

	return result, err
}

func (e *Extractor) getDatasets(ctx context.Context, client *bigquery.Client) (datasets []*bigquery.Dataset, err error) {
	it := client.Datasets(ctx)

	dataset, err := it.Next()
	for err == nil {
		datasets = append(datasets, dataset)
		dataset, err = it.Next()
	}

	if err == iterator.Done {
		err = nil
	}

	return
}

func (e *Extractor) mapDatasets(datasets []*bigquery.Dataset) []map[string]interface{} {
	results := []map[string]interface{}{}

	for _, d := range datasets {
		results = append(results, map[string]interface{}{
			"name":       d.DatasetID,
			"project_id": d.ProjectID,
		})
	}

	return results
}

func (e *Extractor) createClient(ctx context.Context, config Config) (*bigquery.Client, error) {
	if config.ServiceAccountJSON == "" {
		return bigquery.NewClient(ctx, config.ProjectID)
	}

	return bigquery.NewClient(ctx, config.ProjectID, option.WithCredentialsJSON([]byte(config.ServiceAccountJSON)))
}

func (e *Extractor) getConfig(configMap map[string]interface{}) (config Config, err error) {
	err = mapstructure.Decode(configMap, &config)
	if err != nil {
		return
	}

	return
}

func (e *Extractor) validateConfig(config Config) (err error) {
	if config.ProjectID == "" {
		return errors.New("project_id is required")
	}

	return
}
