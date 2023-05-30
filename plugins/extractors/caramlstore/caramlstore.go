package caramlstore

import (
	"context"
	_ "embed" // used to print the embedded assets
	"fmt"
	"time"

	"github.com/goto/meteor/models"
	"github.com/goto/meteor/plugins"
	"github.com/goto/meteor/plugins/extractors/caramlstore/internal/core"
	"github.com/goto/meteor/registry"
	"github.com/goto/meteor/utils"
	"github.com/goto/salt/log"
	"google.golang.org/grpc/codes"
)

// Register the extractor to catalog
func init() {
	if err := registry.Extractors.Register("caramlstore", func() plugins.Extractor {
		return New(plugins.GetLog(), newGRPCClient())
	}); err != nil {
		panic(err)
	}
}

//go:embed README.md
var summary string

// Config holds the set of configuration for the CaraML Store extractor
type Config struct {
	URL            string        `mapstructure:"url" validate:"required"`
	MaxSizeInMB    int           `mapstructure:"max_size_in_mb"`
	RequestTimeout time.Duration `mapstructure:"request_timeout" validate:"min=1ms" default:"10s"`
}

var sampleConfig = `url: caraml-store.com:80`

var info = plugins.Info{
	Description:  "CaraML store ML feature metadata",
	SampleConfig: sampleConfig,
	Summary:      summary,
	Tags:         []string{"caraml", "ml", "feature"},
}

// Extractor manages the communication with the CaraML Store service
type Extractor struct {
	plugins.BaseExtractor

	logger log.Logger
	config Config
	client Client
}

//go:generate mockery --name=Client -r --case underscore --with-expecter --structname CaraMLClient --filename caraml_client_mock.go --output=./internal/mocks

type Client interface {
	Connect(ctx context.Context, host string, maxSizeInMB int, timeout time.Duration) error
	Projects(ctx context.Context) ([]string, error)
	Entities(ctx context.Context, project string) (map[string]*core.Entity, error)
	FeatureTables(ctx context.Context, project string) ([]*core.FeatureTable, error)
	Close() error
}

func New(logger log.Logger, client Client) *Extractor {
	e := &Extractor{
		logger: logger,
		client: client,
	}
	e.BaseExtractor = plugins.NewBaseExtractor(info, &e.config)

	return e
}

// Init initializes the extractor
func (e *Extractor) Init(ctx context.Context, config plugins.Config) error {
	if err := e.BaseExtractor.Init(ctx, config); err != nil {
		return err
	}

	cfg := e.config
	if err := e.client.Connect(ctx, cfg.URL, cfg.MaxSizeInMB, cfg.RequestTimeout); err != nil {
		return fmt.Errorf("connect to host URL: %w", err)
	}

	return nil
}

// Extract checks if the table is valid and extracts the table schema
func (e *Extractor) Extract(ctx context.Context, emit plugins.Emit) error {

	projects, err := e.client.Projects(ctx)
	if err != nil {
		if shouldRetry(err) {
			return plugins.NewRetryError(err)
		}

		return err
	}

	for _, p := range projects {
		entities, err := e.client.Entities(ctx, p)
		if err != nil {
			e.logger.Error("caramlstore extractor", "project", p, "err", err)
			continue
		}

		fts, err := e.client.FeatureTables(ctx, p)
		if err != nil {
			e.logger.Error("caramlstore extractor", "project", p, "err", err)
			continue
		}

		b := featureTableBuilder{
			scope:    e.UrnScope,
			project:  p,
			entities: entities,
		}
		for _, ft := range fts {
			asset, err := b.buildAsset(ft)
			if err != nil {
				e.logger.Error(
					"caramlstore extractor",
					"project", p,
					"feature_table", ft.Spec.Name,
					"err", err,
				)
				continue
			}

			emit(models.NewRecord(asset))
		}
	}

	return nil
}

func (e *Extractor) Close() error {
	return e.client.Close()
}

func shouldRetry(err error) bool {
	switch utils.StatusCode(err) {
	case codes.Canceled,
		codes.DeadlineExceeded,
		codes.ResourceExhausted,
		codes.Internal,
		codes.Unavailable:

		return true
	}

	return false
}
