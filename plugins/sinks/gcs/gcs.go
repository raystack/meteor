package gcs

import (
    "context"
	"strings"
	"time"
	"encoding/base64"
	_ "embed"

	"cloud.google.com/go/storage"
	"github.com/odpf/meteor/models"
	"github.com/odpf/meteor/plugins"
	assetsv1beta2 "github.com/odpf/meteor/models/odpf/assets/v1beta2"
	"google.golang.org/api/option"
	"github.com/odpf/meteor/registry"
	"github.com/MakeNowJust/heredoc"
	"github.com/odpf/salt/log"
	"github.com/pkg/errors"
)

//go:embed README.md
var summary string

type Config struct {
	ProjectID          string `mapstructure:"project_id" validate:"required"`
	Path			string `mapstructure:"path" validate:"required"`
	ObjectPrefix			string `mapstructure:"object_prefix"`
	ServiceAccountJSON string `mapstructure:"service_account_json"`
	ServiceAccountBase64 string   `mapstructure:"service_account_base64"`
}

var info = plugins.Info{
	Description:  "saves data in google cloud storage bucket",
	Summary:     summary,
	SampleConfig: heredoc.Doc(`
	project_id: google-project-id
	path: bucket_name/target_folder
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
	Tags:         []string{"gcs", "sink"},
}

type Sink struct {
	plugins.BasePlugin
	logger log.Logger
	client *storage.Client
	config Config
	writer *storage.Writer
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

	s.client, err = s.createClient(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to create client")
	}

	s.writer = s.createWriter(ctx) 
	return
}

func (s *Sink) createClient(ctx context.Context) (*storage.Client, error) {
	if s.config.ServiceAccountBase64 == "" && s.config.ServiceAccountJSON == "" {
		s.logger.Info("credentials are not specified, creating GCS client using default credentials...")
		return storage.NewClient(ctx)
	}

	if s.config.ServiceAccountBase64 != "" {
		serviceAccountJSON, err := base64.StdEncoding.DecodeString(s.config.ServiceAccountBase64)
		if err != nil || len(serviceAccountJSON) == 0 {
			return nil, errors.Wrap(err, "failed to decode base64 service account")
		}
		s.config.ServiceAccountJSON = string(serviceAccountJSON)
	}

	return storage.NewClient(ctx, option.WithCredentialsJSON([]byte(s.config.ServiceAccountJSON)))
}

func (s *Sink) createWriter(ctx context.Context) (*storage.Writer){
	dirs := strings.Split(s.config.Path, "/")
	bucketname := dirs[0]
	timestamp := time.Now().Format("2006.01.02 15:04:05")

	if s.config.ObjectPrefix!=""{
		s.config.ObjectPrefix = s.config.ObjectPrefix+"-"
	}
	
	filepath := s.config.ObjectPrefix + timestamp +".ndjson"
	if len(dirs) > 1 {
	   filepath = dirs[len(dirs)-1] + "/" + s.config.ObjectPrefix + timestamp + ".ndjson"
    }

	return s.client.Bucket(bucketname).Object(filepath).NewWriter(ctx)
}

func (s *Sink) Sink(ctx context.Context, batch []models.Record) (err error) {
	data := make([]*assetsv1beta2.Asset, 0, len(batch))
	
	for _, record := range batch{
		data = append(data, record.Data())
   	}
	if err= s.writeData(data); err!=nil{
		return errors.Wrap(err, "error in writing data to the object")
	}
	return nil
}

func (s *Sink) writeData(data []*assetsv1beta2.Asset) (err error){
	for _, asset := range data{	
		jsonBytes, _ := models.ToJSON(asset)

		if _, err := s.writer.Write(jsonBytes); err != nil {
			return errors.Wrap(err,"error in writing json data to an object")
		}
		if _,err := s.writer.Write([]byte("\n")); err!=nil{
			return errors.Wrap(err, "error in writing newline")
		}
	}
	return nil
}

func (s *Sink) Close() (err error) { 
	if err := s.writer.Close(); err != nil {
		return errors.Wrap(err, "error closing the writer")
	} 
	return 
}

func init() {
	if err := registry.Sinks.Register("gcs", func() plugins.Syncer {
		return New(plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}