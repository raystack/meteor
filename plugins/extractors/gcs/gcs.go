package gcs

import (
	"context"
	_ "embed" // used to print the embedded assets
	"encoding/base64"
	"fmt"

	"cloud.google.com/go/storage"
	"github.com/pkg/errors"
	"github.com/raystack/meteor/models"
	v1beta2 "github.com/raystack/meteor/models/raystack/assets/v1beta2"
	"github.com/raystack/meteor/plugins"
	"github.com/raystack/meteor/plugins/sqlutil"
	"github.com/raystack/meteor/registry"
	"github.com/raystack/salt/log"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

//go:embed README.md
var summary string

// Config holds the set of configuration for the extractor
type Config struct {
	ProjectID string `json:"project_id" yaml:"project_id" mapstructure:"project_id" validate:"required"`
	// ServiceAccountBase64 takes precedence over ServiceAccountJSON field
	ServiceAccountBase64 string   `json:"service_account_base64" yaml:"service_account_base64" mapstructure:"service_account_base64"`
	ServiceAccountJSON   string   `json:"service_account_json" yaml:"service_account_json" mapstructure:"service_account_json"`
	ExtractBlob          bool     `json:"extract_blob" yaml:"extract_blob" mapstructure:"extract_blob"`
	Exclude              []string `json:"exclude" yaml:"exclude" mapstructure:"exclude"`
}

var sampleConfig = `
project_id: google-project-id
extract_blob: true
# Only one of service_account_base64 / service_account_json is needed. 
# If both are present, service_account_base64 takes precedence
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
exclude: [bucket_a,bucket_b]`

var info = plugins.Info{
	Description:  "Online file storage service By Google",
	SampleConfig: sampleConfig,
	Summary:      summary,
	Tags:         []string{"gcp", "extractor"},
}

// Extractor manages the extraction of data
// from the google cloud storage
type Extractor struct {
	plugins.BaseExtractor
	client          *storage.Client
	logger          log.Logger
	config          Config
	excludedBuckets map[string]bool
	newClient       NewClientFunc
}

type NewClientFunc func(ctx context.Context, logger log.Logger, config Config) (*storage.Client, error)

// New returns a pointer to an initialized Extractor Object
func New(logger log.Logger, newClient NewClientFunc) *Extractor {
	e := &Extractor{
		logger:    logger,
		newClient: newClient,
	}
	e.BaseExtractor = plugins.NewBaseExtractor(info, &e.config)
	e.ScopeNotRequired = true

	return e
}

// Init initializes the extractor
func (e *Extractor) Init(ctx context.Context, config plugins.Config) error {
	if err := e.BaseExtractor.Init(ctx, config); err != nil {
		return err
	}

	// build excluded buckets map
	e.excludedBuckets = sqlutil.BuildBoolMap(e.config.Exclude)

	// create client
	var err error
	e.client, err = e.newClient(ctx, e.logger, e.config)
	if err != nil {
		return fmt.Errorf("create client: %w", err)
	}

	return nil
}

func (e *Extractor) Extract(ctx context.Context, emit plugins.Emit) (err error) {
	it := e.client.Buckets(ctx, e.config.ProjectID)
	for {
		bucket, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return fmt.Errorf("iterate over %s: %w", bucket.Name, err)
		}

		// skip excluded buckets
		if e.excludedBuckets[bucket.Name] {
			continue
		}

		var blobs []*v1beta2.Blob
		if e.config.ExtractBlob {
			blobs, err = e.extractBlobs(ctx, bucket.Name, e.config.ProjectID)
			if err != nil {
				return fmt.Errorf("extract blobs from %s: %w", bucket.Name, err)
			}
		}
		asset, err := e.buildBucket(bucket, e.config.ProjectID, blobs)
		if err != nil {
			return fmt.Errorf("build bucket: %w", err)
		}

		emit(models.NewRecord(asset))
	}

	return
}

func (e *Extractor) extractBlobs(ctx context.Context, bucketName, projectID string) ([]*v1beta2.Blob, error) {
	it := e.client.Bucket(bucketName).Objects(ctx, nil)

	var blobs []*v1beta2.Blob
	for {
		object, err := it.Next()
		if errors.Is(err, iterator.Done) {
			return blobs, nil
		}
		if err != nil {
			return nil, err
		}

		blobs = append(blobs, e.buildBlob(object, projectID))
	}
}

func (e *Extractor) buildBucket(b *storage.BucketAttrs, projectID string, blobs []*v1beta2.Blob) (*v1beta2.Asset, error) {
	bkt, err := anypb.New(&v1beta2.Bucket{
		Location:    b.Location,
		StorageType: b.StorageClass,
		Attributes:  &structpb.Struct{},
		CreateTime:  timestamppb.New(b.Created),
		Blobs:       blobs,
	})
	if err != nil {
		return nil, err
	}
	return &v1beta2.Asset{
		Urn:     models.NewURN("gcs", projectID, "bucket", b.Name),
		Name:    b.Name,
		Service: "gcs",
		Type:    "bucket",
		Labels:  b.Labels,
		Data:    bkt,
	}, nil
}

func (e *Extractor) buildBlob(blob *storage.ObjectAttrs, projectID string) *v1beta2.Blob {
	return &v1beta2.Blob{
		Urn:        models.NewURN("gcs", projectID, "object", fmt.Sprintf("%s/%s", blob.Bucket, blob.Name)),
		Name:       blob.Name,
		Size:       blob.Size,
		DeleteTime: timestamppb.New(blob.Deleted),
		ExpireTime: timestamppb.New(blob.RetentionExpirationTime),
		Ownership: []*v1beta2.Owner{
			{Name: blob.Owner},
		},
		CreateTime: timestamppb.New(blob.Created),
		UpdateTime: timestamppb.New(blob.Updated),
	}
}

func createClient(ctx context.Context, logger log.Logger, config Config) (*storage.Client, error) {
	if config.ServiceAccountBase64 == "" && config.ServiceAccountJSON == "" {
		logger.Info("credentials are not specified, creating google cloud storage client using Default Credentials...")
		return storage.NewClient(ctx)
	}

	if config.ServiceAccountBase64 != "" {
		serviceAccountJSON, err := base64.StdEncoding.DecodeString(config.ServiceAccountBase64)
		if err != nil || len(serviceAccountJSON) == 0 {
			return nil, fmt.Errorf("decode Base64 encoded service account: %w", err)
		}
		// overwrite ServiceAccountJSON with credentials from ServiceAccountBase64 value
		config.ServiceAccountJSON = string(serviceAccountJSON)
	}

	return storage.NewClient(ctx, option.WithCredentialsJSON([]byte(config.ServiceAccountJSON)))
}

// Register the extractor to catalog
func init() {
	if err := registry.Extractors.Register("gcs", func() plugins.Extractor {
		return New(plugins.GetLog(), createClient)
	}); err != nil {
		panic(err)
	}
}
