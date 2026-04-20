package azure_blob

import (
	"context"
	_ "embed"
	"fmt"
	"time"

	"github.com/MakeNowJust/heredoc"
	"github.com/raystack/meteor/models"
	"github.com/raystack/meteor/plugins"
	"github.com/raystack/meteor/plugins/sinks/azure_blob/client"
	"github.com/raystack/meteor/registry"
	log "github.com/raystack/salt/observability/logger"
)

//go:embed README.md
var summary string

type Config struct {
	StorageAccountURL string `mapstructure:"storage_account_url" validate:"required"`
	ContainerName     string `mapstructure:"container_name" validate:"required"`
	ObjectPrefix      string `mapstructure:"object_prefix"`
	AccountKey        string `mapstructure:"account_key"`
	ConnectionString  string `mapstructure:"connection_string"`
}

var info = plugins.Info{
	Description: "Save metadata to Azure Blob Storage.",
	Summary:     summary,
	SampleConfig: heredoc.Doc(`
	storage_account_url: https://myaccount.blob.core.windows.net
	container_name: my-container
	object_prefix: github-users
	account_key: ____account_key____
	connection_string: DefaultEndpointsProtocol=https;AccountName=myaccount;AccountKey=...;EndpointSuffix=core.windows.net
	`),
	Tags: []string{"azure", "storage"},
}

type Sink struct {
	plugins.BasePlugin
	logger log.Logger
	writer client.Writer
	config Config
}

func New(logger log.Logger) plugins.Syncer {
	s := &Sink{
		logger: logger,
	}
	s.BasePlugin = plugins.NewBasePlugin(info, &s.config)

	return s
}

func (s *Sink) Init(ctx context.Context, config plugins.Config) (err error) {
	if err = s.BasePlugin.Init(ctx, config); err != nil {
		return err
	}

	blobName := s.resolveBlobName()

	if s.writer, err = client.NewWriter(ctx, s.config.StorageAccountURL, s.config.ContainerName, blobName, s.config.AccountKey, s.config.ConnectionString); err != nil {
		return err
	}

	return nil
}

func (s *Sink) resolveBlobName() string {
	timestamp := time.Now().Format(time.RFC3339)

	objectPrefix := s.config.ObjectPrefix

	if objectPrefix != "" && objectPrefix[len(objectPrefix)-1] != '-' {
		objectPrefix = fmt.Sprintf("%s-", objectPrefix)
	}

	return fmt.Sprintf("%s%s.ndjson", objectPrefix, timestamp)
}

func (s *Sink) Sink(ctx context.Context, batch []models.Record) (err error) {
	if err = s.writeData(batch); err != nil {
		return fmt.Errorf("write data to the object: %w", err)
	}
	return nil
}

func (s *Sink) writeData(batch []models.Record) (err error) {
	for _, record := range batch {
		jsonBytes, err := models.RecordToJSON(record)
		if err != nil {
			return fmt.Errorf("marshal record (%s): %w", record.Entity().GetUrn(), err)
		}

		if err := s.writer.WriteData(jsonBytes); err != nil {
			return err
		}

		if err := s.writer.WriteData([]byte("\n")); err != nil {
			return err
		}
	}
	return nil
}

func (s *Sink) Close() error {
	return s.writer.Close()
}

func init() {
	if err := registry.Sinks.Register("azure_blob", func() plugins.Syncer {
		return New(plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}
