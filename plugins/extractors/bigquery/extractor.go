package bigquery

import (
	"context"
	"errors"
	"fmt"

	"cloud.google.com/go/bigquery"
	"github.com/mitchellh/mapstructure"
	"github.com/odpf/meteor/core/extractor"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/proto/odpf/meta"
	"github.com/odpf/meteor/utils"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

type Config struct {
	ProjectID          string `mapstructure:"project_id" validate:"required"`
	ServiceAccountJSON string `mapstructure:"service_account_json"`
}

type Extractor struct {
	logger plugins.Logger
}

func New(logger plugins.Logger) extractor.TableExtractor {
	return &Extractor{
		logger: logger,
	}
}

func (e *Extractor) Extract(configMap map[string]interface{}) (result []meta.Table, err error) {
	e.logger.Info("extracting kafka metadata...")
	var config Config
	err = utils.BuildConfig(configMap, &config)
	if err != nil {
		return result, extractor.InvalidConfigError{}
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

func (e *Extractor) getMetadata(ctx context.Context, client *bigquery.Client) (results []meta.Table, err error) {
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

func (e *Extractor) appendTablesMetadata(ctx context.Context, results []meta.Table, dataset *bigquery.Dataset) ([]meta.Table, error) {
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

func (e *Extractor) mapTable(t *bigquery.Table) meta.Table {
	return meta.Table{
		Urn:         fmt.Sprintf("%s.%s.%s", t.ProjectID, t.DatasetID, t.TableID),
		Name:        t.TableID,
		Source:      "bigquery",
		Description: t.DatasetID,
	}
}

func (e *Extractor) createClient(ctx context.Context, config Config) (*bigquery.Client, error) {
	if config.ServiceAccountJSON == "" {
		e.logger.Info("credentials are not specified, creating bigquery client using Default Credentials...")
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
