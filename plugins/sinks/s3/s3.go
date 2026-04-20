package s3

import (
	"context"
	_ "embed"
	"fmt"
	"net/url"
	"time"

	"github.com/MakeNowJust/heredoc"
	"github.com/raystack/meteor/models"
	"github.com/raystack/meteor/plugins"
	"github.com/raystack/meteor/plugins/sinks/s3/client"
	"github.com/raystack/meteor/registry"
	log "github.com/raystack/salt/observability/logger"
)

//go:embed README.md
var summary string

type Config struct {
	BucketURL      string `mapstructure:"bucket_url" validate:"required"`
	Region         string `mapstructure:"region" validate:"required"`
	ObjectPrefix   string `mapstructure:"object_prefix"`
	AccessKeyID    string `mapstructure:"access_key_id"`
	SecretAccessKey string `mapstructure:"secret_access_key"`
	Endpoint       string `mapstructure:"endpoint"`
}

var info = plugins.Info{
	Description: "Save metadata to Amazon S3 or S3-compatible storage.",
	Summary:     summary,
	SampleConfig: heredoc.Doc(`
	bucket_url: s3://bucket-name/optional-prefix
	region: us-east-1
	object_prefix: github-users
	access_key_id: ____access_key____
	secret_access_key: ____secret_key____
	endpoint: http://localhost:9000
	`),
	Tags: []string{"aws", "storage"},
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

	bucket, objectName := s.resolveBucketPath()

	if s.writer, err = client.NewWriter(ctx, bucket, objectName, s.config.Region, s.config.AccessKeyID, s.config.SecretAccessKey, s.config.Endpoint); err != nil {
		return err
	}

	return nil
}

func (s *Sink) resolveBucketPath() (string, string) {
	result, _ := url.Parse(s.config.BucketURL)

	bucket := result.Host
	path := ""
	if len(result.Path) > 1 {
		path = result.Path[1:]
	}
	timestamp := time.Now().Format(time.RFC3339)

	objectPrefix := s.config.ObjectPrefix

	if objectPrefix != "" && objectPrefix[len(objectPrefix)-1] != '-' {
		objectPrefix = fmt.Sprintf("%s-", objectPrefix)
	}

	objectName := fmt.Sprintf("%s%s.ndjson", objectPrefix, timestamp)

	if path != "" {
		objectName = fmt.Sprintf("%s/%s%s.ndjson", path, objectPrefix, timestamp)
	}

	return bucket, objectName
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
	if err := registry.Sinks.Register("s3", func() plugins.Syncer {
		return New(plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}
