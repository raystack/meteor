package bigtable

import (
	"context"
	_ "embed" // used to print the embedded assets
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/MakeNowJust/heredoc"
	"github.com/raystack/meteor/models"
	v1beta2 "github.com/raystack/meteor/models/raystack/assets/v1beta2"
	"github.com/raystack/meteor/registry"
	"google.golang.org/api/option"
	"google.golang.org/protobuf/types/known/anypb"

	"cloud.google.com/go/bigtable"
	"github.com/raystack/meteor/plugins"
	"github.com/raystack/meteor/utils"
	"github.com/raystack/salt/log"
)

//go:embed README.md
var summary string

const (
	service = "bigtable"
)

// Config holds the configurations for the bigtable extractor
type Config struct {
	ProjectID            string `json:"project_id" yaml:"project_id" mapstructure:"project_id" validate:"required"`
	ServiceAccountBase64 string `mapstructure:"service_account_base64"`
	serviceAccountJSON   []byte
}

var info = plugins.Info{
	Description: "Compressed, high-performance, data storage system.",
	Summary:     summary,
	Tags:        []string{"gcp", "extractor"},
	SampleConfig: heredoc.Doc(`
		project_id: google-project-id
		service_account_base64: ____base64_encoded_service_account____
	`),
}

// InstancesFetcher is an interface for fetching instances
type InstancesFetcher interface {
	Instances(context.Context) ([]*bigtable.InstanceInfo, error)
}

var (
	instanceAdminClientCreator = createInstanceAdminClient
	instanceInfoGetter         = getInstancesInfo
)

// Extractor used to extract bigtable metadata
type Extractor struct {
	plugins.BaseExtractor
	config        Config
	logger        log.Logger
	instanceNames []string
}

func New(logger log.Logger) *Extractor {
	e := &Extractor{
		logger: logger,
	}
	e.BaseExtractor = plugins.NewBaseExtractor(info, &e.config)
	e.ScopeNotRequired = true

	return e
}

func (e *Extractor) Init(ctx context.Context, config plugins.Config) error {
	if err := e.BaseExtractor.Init(ctx, config); err != nil {
		return err
	}

	err := e.decodeServiceAccount()
	if err != nil {
		return err
	}

	client, err := instanceAdminClientCreator(ctx, e.config)
	if err != nil {
		return err
	}
	e.instanceNames, err = instanceInfoGetter(ctx, client)
	if err != nil {
		return err
	}

	return nil
}

// Extract checks if the extractor is configured and
// if so, then extracts the metadata and
// returns the assets.
func (e *Extractor) Extract(ctx context.Context, emit plugins.Emit) (err error) {
	err = e.getTablesInfo(ctx, emit)
	if err != nil {
		return
	}

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

func (e *Extractor) getTablesInfo(ctx context.Context, emit plugins.Emit) error {
	for _, instance := range e.instanceNames {
		adminClient, err := e.createAdminClient(ctx, instance, e.config.ProjectID)
		if err != nil {
			return err
		}
		tables, _ := adminClient.Tables(ctx)
		var wg sync.WaitGroup
		for _, table := range tables {
			wg.Add(1)
			go func(table string) {
				defer wg.Done()
				tableInfo, err := adminClient.TableInfo(ctx, table)
				if err != nil {
					return
				}
				familyInfoBytes, _ := json.Marshal(tableInfo.FamilyInfos)
				tableMeta, err := anypb.New(&v1beta2.Table{
					Attributes: utils.TryParseMapToProto(map[string]interface{}{
						"column_family": string(familyInfoBytes),
					}),
				})
				if err != nil {
					e.logger.Warn("error creating Any struct", "error", err)
				}
				asset := v1beta2.Asset{
					Urn:     models.NewURN(service, e.config.ProjectID, "table", fmt.Sprintf("%s.%s", instance, table)),
					Name:    table,
					Service: service,
					Type:    "table",
					Data:    tableMeta,
				}
				emit(models.NewRecord(&asset))
			}(table)
		}
		wg.Wait()
	}
	return nil
}

func (c Config) clientOptions() []option.ClientOption {
	if c.serviceAccountJSON == nil {
		return nil
	}
	return []option.ClientOption{option.WithCredentialsJSON(c.serviceAccountJSON)}
}

func createInstanceAdminClient(ctx context.Context, config Config) (*bigtable.InstanceAdminClient, error) {
	return bigtable.NewInstanceAdminClient(ctx, config.ProjectID)
}

func (e *Extractor) createAdminClient(ctx context.Context, instance string, projectID string) (*bigtable.AdminClient, error) {
	return bigtable.NewAdminClient(ctx, projectID, instance, e.config.clientOptions()...)
}

func (e *Extractor) decodeServiceAccount() error {
	if e.config.ServiceAccountBase64 == "" {
		return nil
	}

	serviceAccountJSON, err := base64.StdEncoding.DecodeString(e.config.ServiceAccountBase64)
	if err != nil || len(serviceAccountJSON) == 0 {
		return fmt.Errorf("decode Base64 encoded service account: %w", err)
	}

	e.config.serviceAccountJSON = serviceAccountJSON
	return nil
}

// Register the extractor to catalog
func init() {
	if err := registry.Extractors.Register("bigtable", func() plugins.Extractor {
		return New(plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}
