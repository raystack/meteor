package bigtable

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/odpf/meteor/proto/odpf/assets"
	"github.com/odpf/meteor/proto/odpf/assets/common"
	"github.com/odpf/meteor/proto/odpf/assets/facets"
	"github.com/odpf/meteor/registry"

	"cloud.google.com/go/bigtable"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/utils"
	"github.com/odpf/salt/log"
)

//go:embed README.md
var summary string

type Config struct {
	ProjectID string `mapstructure:"project_id" validate:"required"`
}

var sampleConfig = `
 project_id: google-project-id`

type Extractor struct {
	logger log.Logger
}

type InstancesFetcher interface {
	Instances(context.Context) ([]*bigtable.InstanceInfo, error)
}

var (
	instanceAdminClientCreator = createInstanceAdminClient
	instanceInfoGetter         = getInstancesInfo
)

func (e *Extractor) Info() plugins.Info {
	return plugins.Info{
		Description:  "Compressed, high-performance, proprietary data storage system.",
		SampleConfig: sampleConfig,
		Summary:      summary,
		Tags:         []string{"gcp,extractor"},
	}
}

func (e *Extractor) Validate(configMap map[string]interface{}) (err error) {
	return utils.BuildConfig(configMap, &Config{})
}

func (e *Extractor) Extract(ctx context.Context, configMap map[string]interface{}, out chan<- interface{}) (err error) {
	e.logger.Info("extracting bigtable metadata...")

	var config Config
	err = utils.BuildConfig(configMap, &config)
	if err != nil {
		return plugins.InvalidConfigError{}
	}

	instanceAdminClient, err := instanceAdminClientCreator(ctx, config)
	if err != nil {
		return
	}
	instanceNames, err := instanceInfoGetter(ctx, instanceAdminClient)
	if err != nil {
		return
	}
	result, err := e.getTablesInfo(ctx, instanceNames, config.ProjectID)
	if err != nil {
		return
	}
	out <- result
	return
}

func getInstancesInfo(ctx context.Context, client InstancesFetcher) (instanceNames []string, err error) {
	instanceInfos, err := client.Instances(ctx)
	if err != nil {
		return
	}
	for i := 0; i < len(instanceInfos); i++ {
		instanceNames = append(instanceNames, instanceInfos[i].Name)
	}
	return instanceNames, nil
}

func (e *Extractor) getTablesInfo(ctx context.Context, instances []string, projectID string) (results []assets.Table, err error) {
	for _, instance := range instances {
		adminClient, err := e.createAdminClient(ctx, instance, projectID)
		if err != nil {
			return nil, err
		}
		tables, _ := adminClient.Tables(ctx)
		wg := sync.WaitGroup{}
		for _, table := range tables {
			wg.Add(1)
			go func(table string) {
				tableInfo, err := adminClient.TableInfo(ctx, table)
				if err != nil {
					return
				}
				familyInfoBytes, _ := json.Marshal(tableInfo.FamilyInfos)
				results = append(results, assets.Table{
					Resource: &common.Resource{
						Urn:     fmt.Sprintf("%s.%s.%s", projectID, instance, table),
						Name:    table,
						Service: "bigtable",
					},
					Properties: &facets.Properties{
						Attributes: utils.TryParseMapToProto(map[string]interface{}{
							"column_family": string(familyInfoBytes),
						}),
					},
				})
				wg.Done()
			}(table)
		}
		wg.Wait()
	}
	return
}

func createInstanceAdminClient(ctx context.Context, config Config) (*bigtable.InstanceAdminClient, error) {
	return bigtable.NewInstanceAdminClient(ctx, config.ProjectID)
}

func (e *Extractor) createAdminClient(ctx context.Context, instance string, projectID string) (*bigtable.AdminClient, error) {
	return bigtable.NewAdminClient(ctx, projectID, instance)
}

// Register the extractor to catalog
func init() {
	if err := registry.Extractors.Register("bigtable", func() plugins.Extractor {
		return &Extractor{
			logger: plugins.GetLog(),
		}
	}); err != nil {
		panic(err)
	}
}
