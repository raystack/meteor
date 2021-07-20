package bigquerytable

import (
	"context"
	"errors"

	"cloud.google.com/go/bigquery"
	"github.com/mitchellh/mapstructure"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

type Config struct {
	ProjectID       string `mapstructure:"project_id"`
	CredentialsJSON string `mapstructure:"credentials_json"`
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
	result, err = e.getMetadata(ctx, client)
	if err != nil {
		return
	}

	return
}

func (e *Extractor) getMetadata(ctx context.Context, client *bigquery.Client) (results []map[string]interface{}, err error) {
	it := client.Datasets(ctx)

	dataset, err := it.Next()
	for err == nil {
		results, err = e.appendTablesMetadata(ctx, results, dataset)
		if err != nil {
			return
		}

		dataset, err = it.Next()
	}
	if err == iterator.Done {
		err = nil
	}

	return
}

func (e *Extractor) appendTablesMetadata(ctx context.Context, results []map[string]interface{}, dataset *bigquery.Dataset) ([]map[string]interface{}, error) {
	it := dataset.Tables(ctx)

	table, err := it.Next()
	for err == nil {
		results = append(results, e.mapTable(table))
		table, err = it.Next()
	}
	if err == iterator.Done {
		err = nil
	}

	return results, err
}

func (e *Extractor) mapTable(t *bigquery.Table) map[string]interface{} {
	return map[string]interface{}{
		"name":       t.TableID,
		"dataset_id": t.DatasetID,
		"project_id": t.ProjectID,
	}
}

func (e *Extractor) createClient(ctx context.Context, config Config) (*bigquery.Client, error) {
	if config.CredentialsJSON == "" {
		return bigquery.NewClient(ctx, config.ProjectID)
	}

	return bigquery.NewClient(ctx, config.ProjectID, option.WithCredentialsJSON([]byte(config.CredentialsJSON)))
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
