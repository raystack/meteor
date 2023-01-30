package gcs

import (
    "context"
	"strings"
	"time"
	"encoding/base64"
	_ "embed"

	"github.com/odpf/meteor/models"
	"github.com/odpf/meteor/plugins"
	assetsv1beta2 "github.com/odpf/meteor/models/odpf/assets/v1beta2"
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
	client GCSClient
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

	if err:= s.validateServiceAccountKey(); err!=nil{
		return err
	}

	bucketname, objectname := s.resolveBucketandObjectNames()

	if s.client, err = newGCSClient(ctx, []byte(s.config.ServiceAccountJSON), bucketname, objectname); err != nil{
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

func (s *Sink) resolveBucketandObjectNames() (string, string) {
	dirs := strings.Split(s.config.Path, "/")
	bucketname := dirs[0]
	timestamp := time.Now().Format("2006.01.02 15:04:05")

	if s.config.ObjectPrefix!=""{
		s.config.ObjectPrefix = s.config.ObjectPrefix+"-"
	}
	
	objectname := s.config.ObjectPrefix + timestamp +".ndjson"
	if len(dirs) > 1 {
	   objectname = dirs[len(dirs)-1] + "/" + s.config.ObjectPrefix + timestamp + ".ndjson"
    }

	return bucketname, objectname
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

		if err := s.client.WriteData(jsonBytes); err!=nil{
			return err
		}
	
		if err := s.client.WriteData([]byte("\n")); err!=nil{
			return err
		}
	}
	return nil
}

func (s *Sink) Close() (err error) { 
	if err := s.client.Close(); err != nil {
		return err
	} 
	return nil
}

func init() {
	if err := registry.Sinks.Register("gcs", func() plugins.Syncer {
		return New(plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}