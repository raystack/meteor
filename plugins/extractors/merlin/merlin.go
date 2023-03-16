package merlin

import (
	"context"
	_ "embed" // used to print the embedded assets
	"encoding/base64"
	"fmt"
	"net/http"
	"runtime/debug"
	"sync"
	"time"

	"github.com/MakeNowJust/heredoc"
	"github.com/goto/meteor/models"
	"github.com/goto/meteor/plugins"
	"github.com/goto/meteor/plugins/extractors/merlin/internal/merlin"
	"github.com/goto/meteor/registry"
	"github.com/goto/salt/log"
	"github.com/pkg/errors"
)

// init register the extractor to the catalog
func init() {
	if err := registry.Extractors.Register("merlin", func() plugins.Extractor {
		return New(plugins.GetLog(), newHTTPClient)
	}); err != nil {
		panic(err)
	}
}

//go:embed README.md
var summary string

// Config holds the set of configuration for the Merlin extractor.
type Config struct {
	URL                  string        `mapstructure:"url" validate:"required"`
	ServiceAccountBase64 string        `mapstructure:"service_account_base64"`
	RequestTimeout       time.Duration `mapstructure:"request_timeout" validate:"min=1ms" default:"10s"`
	WorkerCount          int           `mapstructure:"worker_count" validate:"min=1" default:"5"`
}

var sampleConfig = heredoc.Doc(`
	url: merlin.my-company.com
	service_account_base64: |-
	  ____base64_encoded_service_account_credentials____
`)

var info = plugins.Info{
	Description:  "Merlin ML models metadata",
	SampleConfig: sampleConfig,
	Summary:      summary,
	Tags:         []string{"merlin", "ml", "model"},
}

// Extractor manages the communication with the Merlin service.
type Extractor struct {
	plugins.BaseExtractor

	logger    log.Logger
	newClient NewClientFunc
	client    Client
	config    Config
}

type NewClientFunc func(ctx context.Context, cfg Config) (Client, error)

//go:generate mockery --name=Client -r --case underscore --with-expecter --structname MerlinClient --filename merlin_client_mock.go --output=./internal/mocks

type Client interface {
	Projects(ctx context.Context) ([]merlin.Project, error)
	Models(ctx context.Context, projectID int64) ([]merlin.Model, error)
	ModelVersion(ctx context.Context, modelID, versionID int64) (merlin.ModelVersion, error)
}

// New returns a pointer to an initialized Extractor Object
func New(logger log.Logger, newClient NewClientFunc) *Extractor {
	e := &Extractor{
		logger:    logger,
		newClient: newClient,
	}
	e.BaseExtractor = plugins.NewBaseExtractor(info, &e.config)

	return e
}

// Init initializes the extractor
func (e *Extractor) Init(ctx context.Context, config plugins.Config) error {
	if err := e.BaseExtractor.Init(ctx, config); err != nil {
		return fmt.Errorf("init Merlin extractor: %w", err)
	}

	client, err := e.newClient(ctx, e.config)
	if err != nil {
		return fmt.Errorf("init Merlin extractor: %w", err)
	}

	e.client = client

	return nil
}

func (e *Extractor) Extract(ctx context.Context, emit plugins.Emit) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	projects, err := e.client.Projects(ctx)
	if err != nil {
		if shouldRetry(err) {
			return plugins.NewRetryError(err)
		}

		return err
	}

	errCh := e.startWorkers(ctx, jobQueue(ctx, projects), e.config.WorkerCount, emit)
	select {
	case <-ctx.Done():
		return ctx.Err()

	case err, ok := <-errCh:
		if ok {
			return err
		}
	}

	return nil
}

func (e *Extractor) startWorkers(
	ctx context.Context, jobs <-chan merlin.Project, workerCnt int, emit plugins.Emit,
) <-chan error {
	var wg sync.WaitGroup
	wg.Add(workerCnt)

	errCh := make(chan error)
	for i := 0; i < workerCnt; i++ {
		go func() {
			defer wg.Done()

			sendErr := func(err error) {
				select {
				case <-ctx.Done():
					return

				case errCh <- err:
				}
			}

			for {
				select {
				case <-ctx.Done():
					return

				case p, ok := <-jobs:
					if !ok {
						return
					}

					if err := e.extractProject(ctx, p, emit); err != nil {
						sendErr(err)
						return
					}
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(errCh)
	}()

	return errCh
}

func (e *Extractor) extractProject(ctx context.Context, prj merlin.Project, emit plugins.Emit) (err error) {
	defer func() {
		if r := recover(); r != nil {
			e.logger.Error("panic recovered", "err", r)
			e.logger.Info(string(debug.Stack()))
			if e, ok := r.(error); ok {
				err = fmt.Errorf("extract project '%d': panic: %w", prj.ID, e)
			} else {
				err = fmt.Errorf("extract project '%d': panic: %v", prj.ID, r)
			}
		}
	}()

	mdls, err := e.client.Models(ctx, prj.ID)
	if err != nil {
		e.logger.Error("merlin extractor", "project", prj.ID, "err", err)
		return nil
	}

	for _, mdl := range mdls {
		if len(mdl.Endpoints) == 0 {
			continue
		}

		versions := make(map[int64]merlin.ModelVersion)
		for _, endpoint := range mdl.Endpoints {
			for _, dest := range endpoint.Rule.Destinations {
				if dest.VersionEndpoint == nil {
					continue
				}

				versionID := dest.VersionEndpoint.VersionID
				version, err := e.client.ModelVersion(ctx, mdl.ID, versionID)
				if err != nil {
					e.logger.Error("merlin extractor", "project", prj.ID, "model", mdl.ID, "err", err)
					continue
				}

				versions[versionID] = version
			}
		}

		asset, err := modelBuilder{
			scope:    e.UrnScope,
			project:  prj,
			model:    mdl,
			versions: versions,
		}.buildAsset()
		if err != nil {
			e.logger.Error(
				"merlin extractor",
				"project", prj.ID,
				"model", mdl.ID,
				"err", err,
			)
			continue
		}

		emit(models.NewRecord(asset))
	}

	return nil
}

func newHTTPClient(ctx context.Context, cfg Config) (Client, error) {
	params := merlin.ClientParams{
		BaseURL: cfg.URL,
		Timeout: cfg.RequestTimeout,
	}

	if len(cfg.ServiceAccountBase64) != 0 {
		credsJSON, err := base64.StdEncoding.DecodeString(cfg.ServiceAccountBase64)
		if err != nil {
			return nil, fmt.Errorf("new Merlin client: decode base64: %w", err)
		}

		params.ServiceAccountJSON = credsJSON
	}

	return merlin.NewClient(ctx, params)
}

func jobQueue(ctx context.Context, projects []merlin.Project) <-chan merlin.Project {
	jobs := make(chan merlin.Project)

	go func() {
		defer close(jobs)

		for _, p := range projects {
			select {
			case <-ctx.Done():
				return

			case jobs <- p:
			}
		}
	}()

	return jobs
}

func shouldRetry(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}

	var e *merlin.APIError
	if errors.As(err, &e) && (e.Status >= 500 || e.Status == http.StatusTooManyRequests) {
		return true
	}

	return false
}
