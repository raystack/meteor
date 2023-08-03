package bigtable

import (
	"context"
	_ "embed" // used to print the embedded assets
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sync"

	"cloud.google.com/go/bigtable"
	"github.com/MakeNowJust/heredoc"
	"github.com/goto/meteor/models"
	v1beta2 "github.com/goto/meteor/models/gotocompany/assets/v1beta2"
	"github.com/goto/meteor/plugins"
	"github.com/goto/meteor/registry"
	"github.com/goto/meteor/utils"
	"github.com/goto/salt/log"
	"google.golang.org/api/option"
	"google.golang.org/protobuf/types/known/anypb"
)

//go:embed README.md
var summary string

const (
	service = "bigtable"
)

// Config holds the configurations for the bigtable extractor
type Config struct {
	ProjectID            string `mapstructure:"project_id" validate:"required"`
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
	config         Config
	logger         log.Logger
	instanceNames  []string
	newClient      NewClientFunc
	newAdminClient NewAdminClientFunc
}

// InstanceAdminClient is an interface for *bigtable.InstanceAdminClient
//
//go:generate mockery --name=InstanceAdminClient -r --case underscore --with-expecter --structname InstanceAdminClient --output=./mocks
type InstanceAdminClient interface {
	Instances(ctx context.Context) ([]*bigtable.InstanceInfo, error)
}

// AdminClient is an interface for *bigtable.AdminClient
//
//go:generate mockery --name=AdminClient -r --case underscore --with-expecter --structname AdminClient --output=./mocks
type AdminClient interface {
	Tables(ctx context.Context) ([]string, error)
	TableInfo(ctx context.Context, table string) (*bigtable.TableInfo, error)
}

type (
	NewClientFunc      func(ctx context.Context, cfg Config) (InstanceAdminClient, error)
	NewAdminClientFunc func(ctx context.Context, instance string, config Config) (AdminClient, error)
)

func New(logger log.Logger, newClient NewClientFunc, newAdminClient NewAdminClientFunc) *Extractor {
	e := &Extractor{
		logger:         logger,
		newClient:      newClient,
		newAdminClient: newAdminClient,
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

	client, err := e.newClient(ctx, e.config)
	if err != nil {
		return err
	}

	client = WithInstanceAdminClientMW(e.config.ProjectID)(client)

	e.instanceNames, err = instanceInfoGetter(ctx, client)
	if err != nil {
		return err
	}

	return nil
}

// Extract checks if the extractor is configured and
// if so, then extracts the metadata and
// returns the assets.
func (e *Extractor) Extract(ctx context.Context, emit plugins.Emit) error {
	return e.getTablesInfo(ctx, emit)
}

func getInstancesInfo(ctx context.Context, client InstancesFetcher) ([]string, error) {
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

func (e *Extractor) getTablesInfo(ctx context.Context, emit plugins.Emit) error {
	for _, instance := range e.instanceNames {
		adminClient, err := e.newAdminClient(ctx, instance, e.config)
		if err != nil {
			return err
		}

		adminClient = WithAdminClientMW(e.config.ProjectID, instance)(adminClient)

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

func createInstanceAdminClient(ctx context.Context, config Config) (InstanceAdminClient, error) {
	return bigtable.NewInstanceAdminClient(ctx, config.ProjectID, config.clientOptions()...)
}

func createAdminClient(ctx context.Context, instance string, config Config) (AdminClient, error) {
	return bigtable.NewAdminClient(ctx, config.ProjectID, instance, config.clientOptions()...)
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
		return New(plugins.GetLog(), instanceAdminClientCreator, createAdminClient)
	}); err != nil {
		panic(err)
	}
}
