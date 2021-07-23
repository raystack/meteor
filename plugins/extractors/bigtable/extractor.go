package bigtable

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/odpf/meteor/proto/odpf/meta/facets"
	"sync"

	"cloud.google.com/go/bigtable"
	"github.com/odpf/meteor/core/extractor"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/proto/odpf/meta"
	"github.com/odpf/meteor/utils"
)

type Config struct {
	ProjectID string `mapstructure:"project_id" validate:"required"`
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
	e.logger.Info("extracting bigtable metadata...")
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
	instanceAdminClient, err := e.createInstanceAdminClient(ctx, config)
	if err != nil {
		return
	}
	instanceNames, err := e.getInstancesInfo(ctx, instanceAdminClient)
	if err != nil {
		return
	}
	result, err = e.getTablesInfo(ctx, instanceNames, config)
	if err != nil {
		return
	}
	return
}

func (e *Extractor) getInstancesInfo(ctx context.Context, client *bigtable.InstanceAdminClient) (results []string, err error) {
	instanceInfos, err := client.Instances(ctx)
	if err != nil {
		return nil, err
	}
	var instanceNames []string
	for i := 0; i < len(instanceInfos); i++ {
		instanceNames = append(instanceNames, instanceInfos[i].Name)
	}
	return instanceNames, nil
}

func (e *Extractor) getTablesInfo(ctx context.Context, instances []string, config Config) (results []meta.Table, err error) {
	for _, instance := range instances {
		adminClient, err := e.createAdminClient(ctx, instance, config)
		if err != nil {
			return nil, err
		}
		tables, err := adminClient.Tables(ctx)
		wg := sync.WaitGroup{}
		for _, table := range tables {
			wg.Add(1)
			go func(table string) {
				tableInfo, err := adminClient.TableInfo(ctx, table)
				if err != nil {
					return
				}
				customProps := make(map[string]string)
				familyInfoBytes, _ := json.Marshal(tableInfo.FamilyInfos)
				customProps["columnfamily"] = string(familyInfoBytes)
				results = append(results, meta.Table{
					Urn:    fmt.Sprintf("%s.%s.%s", config.ProjectID, instance, table),
					Name:   table,
					Source: "bigtable",
					Custom: &facets.Custom{
						CustomProperties: customProps,
					},
				})
				wg.Done()
			}(table)
		}
		wg.Wait()
	}
	return
}

func (e *Extractor) createInstanceAdminClient(ctx context.Context, config Config) (*bigtable.InstanceAdminClient, error) {
	return bigtable.NewInstanceAdminClient(ctx, config.ProjectID)
}

func (e *Extractor) createAdminClient(ctx context.Context, instance string, config Config) (*bigtable.AdminClient, error) {
	return bigtable.NewAdminClient(ctx, config.ProjectID, instance)
}

func (e *Extractor) validateConfig(config Config) (err error) {
	if config.ProjectID == "" {
		return errors.New("project_id is required")
	}

	return
}
