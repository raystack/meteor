package gcs

import (
	"context"
	_ "embed"
	"encoding/base64"
	"fmt"
	"net/url"
	"time"

	"github.com/MakeNowJust/heredoc"
	"github.com/goto/meteor/models"
	assetsv1beta2 "github.com/goto/meteor/models/gotocompany/assets/v1beta2"
	"github.com/goto/meteor/plugins"
	"github.com/goto/meteor/registry"
	"github.com/goto/salt/log"
	"github.com/pkg/errors"
)

//go:embed README.md
var summary string

type Config struct {
	ProjectID            string `mapstructure:"project_id" validate:"required"`
	URL                  string `mapstructure:"url" validate:"required"`
	ObjectPrefix         string `mapstructure:"object_prefix"`
	ServiceAccountJSON   string `mapstructure:"service_account_json"`
	ServiceAccountBase64 string `mapstructure:"service_account_base64"`
}

var info = plugins.Info{
	Description: "saves data in google cloud storage bucket",
	Summary:     summary,
	SampleConfig: heredoc.Doc(`
	project_id: google-project-id
	url: gcs://bucket_name/target_folder
	object_prefix : github-users
	service_account_base64: ____base64_encoded_service_account____
    service_account_json: |-
      {
       "type": "service_account",
       "private_key_id": "xxxxxxx",
       "private_key": "xxxxxxx",
       "client_email": "xxxxxxx",
       "client_id": "xxxxxxx",
       "auth_uri": "https://accounts.google.com/o/oauth2/auth",
       "token_uri": "https://oauth2.googleapis.com/token",
       "auth_provider_x509_cert_url": "xxxxxxx",
       "client_x509_cert_url": "xxxxxxx"
     }
	`),
	Tags: []string{"gcs", "sink"},
}

type Sink struct {
	plugins.BasePlugin
	logger log.Logger
	writer Writer
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

	if err := s.validateServiceAccountKey(); err != nil {
		return err
	}

	bucketname, objectname := s.resolveBucketPath()

	if s.writer, err = newWriter(ctx, []byte(s.config.ServiceAccountJSON), bucketname, objectname); err != nil {
		return err
	}

	return nil
}

func (s *Sink) validateServiceAccountKey() error {
	if s.config.ServiceAccountBase64 == "" && s.config.ServiceAccountJSON == "" {
		return errors.New("credentials are not specified, failed to create client")
	}

	if s.config.ServiceAccountBase64 != "" {
		serviceAccountJSON, err := base64.StdEncoding.DecodeString(s.config.ServiceAccountBase64)
		if err != nil || len(serviceAccountJSON) == 0 {
			return errors.Wrap(err, "failed to decode base64 service account")
		}
		s.config.ServiceAccountJSON = string(serviceAccountJSON)
	}
	return nil
}

func (s *Sink) resolveBucketPath() (string, string) {
	result, _ := url.Parse(s.config.URL)

	bucketname := result.Host
	path := result.Path[1:]
	timestamp := time.Now().Format(time.RFC3339)

	objectprefix := s.config.ObjectPrefix

	if objectprefix != "" && objectprefix[len(objectprefix)-1] != '-' {
		objectprefix = fmt.Sprintf("%s-", objectprefix)
	}

	objectname := fmt.Sprintf("%s%s.ndjson", objectprefix, timestamp)

	if path != "" {
		objectname = fmt.Sprintf("%s/%s%s.ndjson", path, objectprefix, timestamp)
	}

	return bucketname, objectname
}

func (s *Sink) Sink(ctx context.Context, batch []models.Record) (err error) {
	data := make([]*assetsv1beta2.Asset, 0, len(batch))

	for _, record := range batch {
		data = append(data, record.Data())
	}
	if err = s.writeData(data); err != nil {
		return errors.Wrap(err, "error in writing data to the object")
	}
	return nil
}

func (s *Sink) writeData(data []*assetsv1beta2.Asset) (err error) {
	for _, asset := range data {
		jsonBytes, _ := models.ToJSON(asset)

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
	if err := registry.Sinks.Register("gcs", func() plugins.Syncer {
		return New(plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}
