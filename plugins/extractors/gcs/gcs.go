package gcs

import (
	"context"
	_ "embed" // used to print the embedded assets
	"fmt"
	"github.com/pkg/errors"

	"github.com/odpf/meteor/models"
	"github.com/odpf/meteor/models/odpf/assets"
	"github.com/odpf/meteor/models/odpf/assets/common"
	"github.com/odpf/meteor/models/odpf/assets/facets"
	"github.com/odpf/meteor/registry"
	"google.golang.org/protobuf/types/known/timestamppb"

	"cloud.google.com/go/storage"
	"github.com/odpf/meteor/plugins"
	"github.com/odpf/meteor/utils"
	"github.com/odpf/salt/log"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

//go:embed README.md
var summary string

const metadataSource = "googlecloudstorage"

// Config hold the set of configuration for the extractor
type Config struct {
	ProjectID          string `mapstructure:"project_id" validate:"required"`
	ServiceAccountJSON string `mapstructure:"service_account_json"`
	ExtractBlob        bool   `mapstructure:"extract_blob"`
}

var sampleConfig = `
project_id: google-project-id
extract_blob: true
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
  }`

// Extractor manages the extraction of data
// from the google cloud storage
type Extractor struct {
	client *storage.Client
	logger log.Logger
	config Config
}

// New returns a pointer to an initialized Extractor Object
func New(logger log.Logger) *Extractor {
	return &Extractor{
		logger: logger,
	}
}

// Info returns the brief information about the extractor
func (e *Extractor) Info() plugins.Info {
	return plugins.Info{
		Description:  "Online file storage web service for storing and accessing data.",
		SampleConfig: sampleConfig,
		Summary:      summary,
		Tags:         []string{"gcp", "extractor"},
	}
}

// Validate validates the configuration of the extractor
func (e *Extractor) Validate(configMap map[string]interface{}) (err error) {
	return utils.BuildConfig(configMap, &Config{})
}

// Init initializes the extractor
func (e *Extractor) Init(ctx context.Context, configMap map[string]interface{}) (err error) {
	// build config
	err = utils.BuildConfig(configMap, &e.config)
	if err != nil {
		return plugins.InvalidConfigError{}
	}

	// create client
	e.client, err = e.createClient(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to create client")
	}

	return
}

func (e *Extractor) Extract(ctx context.Context, emit plugins.Emit) (err error) {
	it := e.client.Buckets(ctx, e.config.ProjectID)
	for {
		bucket, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return errors.Wrapf(err, "failed to iterate over %s", bucket.Name)
		}

		var blobs []*assets.Blob
		if e.config.ExtractBlob {
			blobs, err = e.extractBlobs(ctx, bucket.Name, e.config.ProjectID)
			if err != nil {
				return errors.Wrapf(err, "failed to extract blobs from %s", bucket.Name)
			}
		}

		emit(models.NewRecord(e.buildBucket(bucket, e.config.ProjectID, blobs)))
	}

	return
}

func (e *Extractor) extractBlobs(ctx context.Context, bucketName string, projectID string) (blobs []*assets.Blob, err error) {
	it := e.client.Bucket(bucketName).Objects(ctx, nil)

	object, err := it.Next()
	for err == nil {
		blobs = append(blobs, e.buildBlob(object, projectID))
		object, err = it.Next()
	}
	if err == iterator.Done {
		err = nil
	}

	return
}

func (e *Extractor) buildBucket(b *storage.BucketAttrs, projectID string, blobs []*assets.Blob) (bucket *assets.Bucket) {
	bucket = &assets.Bucket{
		Resource: &common.Resource{
			Urn:     fmt.Sprintf("%s/%s", projectID, b.Name),
			Name:    b.Name,
			Service: metadataSource,
		},
		Location:    b.Location,
		StorageType: b.StorageClass,
		Timestamps: &common.Timestamp{
			CreateTime: timestamppb.New(b.Created),
		},
		Properties: &facets.Properties{
			Labels: b.Labels,
		},
	}
	if blobs != nil {
		bucket.Blobs = blobs
	}

	return
}

func (e *Extractor) buildBlob(blob *storage.ObjectAttrs, projectID string) *assets.Blob {
	return &assets.Blob{
		Urn:        fmt.Sprintf("%s/%s/%s", projectID, blob.Bucket, blob.Name),
		Name:       blob.Name,
		Size:       blob.Size,
		DeleteTime: timestamppb.New(blob.Deleted),
		ExpireTime: timestamppb.New(blob.RetentionExpirationTime),
		Ownership: &facets.Ownership{
			Owners: []*facets.Owner{
				{Name: blob.Owner},
			},
		},
		Timestamps: &common.Timestamp{
			CreateTime: timestamppb.New(blob.Created),
			UpdateTime: timestamppb.New(blob.Updated),
		},
	}
}

func (e *Extractor) createClient(ctx context.Context) (*storage.Client, error) {
	if e.config.ServiceAccountJSON == "" {
		e.logger.Info("credentials are not specified, creating google cloud storage client using Default Credentials...")
		return storage.NewClient(ctx)
	}

	return storage.NewClient(ctx, option.WithCredentialsJSON([]byte(e.config.ServiceAccountJSON)))
}

// Register the extractor to catalog
func init() {
	if err := registry.Extractors.Register("gcs", func() plugins.Extractor {
		return New(plugins.GetLog())
	}); err != nil {
		panic(err)
	}
}
